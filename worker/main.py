from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel
import httpx
import os
import json
import hashlib
import logging
import time
from typing import Dict, Any, List, Sequence
from collections import defaultdict

import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ComputeEngine.Worker")

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

STORAGE_DIR = "storage"
os.makedirs(STORAGE_DIR, exist_ok=True)

MASTER_URL = os.getenv("MASTER_URL", "http://master:8000")
WORKER_URL = os.getenv("WORKER_URL", "http://localhost:8001")

SAFE_GLOBALS = {"__builtins__": {}}
SAFE_LOCALS = {
    "abs": abs,
    "min": min,
    "max": max,
    "sum": sum,
    "len": len,
}

MR_BATCH = int(os.getenv("MR_SHUFFLE_BATCH", "3000"))

_deployed_mr: Dict[str, Dict[str, str]] = {}


def _is_input_csv(name: str) -> bool:
    return (
        name.endswith(".csv")
        and not name.startswith("map_res_")
        and not name.startswith("mr_")
    )


def partition_idx(key: str, n: int) -> int:
    if n <= 0:
        return 0
    h = hashlib.md5(str(key).encode("utf-8")).hexdigest()
    return int(h[:16], 16) % n


def _load_map_reduce_funcs(job_id: str):
    spec = _deployed_mr.get(job_id)
    if not spec:
        raise HTTPException(status_code=400, detail=f"Немає розгорнутого MR-коду для job_id={job_id}")

    g_map = {"__builtins__": __builtins__}
    exec(spec["mapper_source"], g_map)
    map_row = g_map.get("map_row")
    if not callable(map_row):
        raise HTTPException(status_code=400, detail="mapper повинен визначати функцію map_row(row)")

    g_red = {"__builtins__": __builtins__}
    exec(spec["reducer_source"], g_red)
    reduce_group = g_red.get("reduce_group")
    if not callable(reduce_group):
        raise HTTPException(status_code=400, detail="reducer повинен визначати функцію reduce_group(key, values)")

    return map_row, reduce_group


@app.on_event("startup")
async def startup_event():
    async with httpx.AsyncClient() as client:
        try:
            await client.post(f"{MASTER_URL}/register?worker_url={WORKER_URL}")
            logger.info(f"Воркер успішно зареєстрований: {WORKER_URL}")
        except Exception as e:
            logger.error(f"Не вдалося зареєструватися на Мастері: {e}")


@app.get("/")
def index():
    return {"status": "Worker is up", "worker_url": WORKER_URL}


@app.get("/download")
def download_fragment(filename: str):
    if not filename or "/" in filename or "\\" in filename or ".." in filename:
        raise HTTPException(status_code=400, detail="Некоректне ім'я файлу")
    path = os.path.join(STORAGE_DIR, filename)
    if not os.path.isfile(path):
        raise HTTPException(status_code=404, detail="Файл не знайдено")
    return FileResponse(path, filename=filename)


@app.post("/upload")
async def upload_fragment(file: UploadFile = File(...)):
    try:
        file_path = os.path.join(STORAGE_DIR, file.filename)
        with open(file_path, "wb") as f:
            content = await file.read()
            f.write(content)
        logger.info(f"Файл завантажено: {file.filename}")
        return {"message": f"Saved {file.filename}", "path": file_path}
    except Exception as e:
        logger.error(f"Помилка при завантаженні: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})


class MRDeployBody(BaseModel):
    job_id: str
    mapper_source: str
    reducer_source: str


@app.post("/mr/deploy")
async def mr_deploy(body: MRDeployBody):
    _deployed_mr[body.job_id] = {
        "mapper_source": body.mapper_source,
        "reducer_source": body.reducer_source,
    }
    logger.info(f"MR код розгорнуто для job {body.job_id}")
    return {"status": "ok", "job_id": body.job_id}


@app.post("/mr/map")
async def mr_run_map(job_id: str):
    map_row, _ = _load_map_reduce_funcs(job_id)
    pairs: List[List[Any]] = []

    for filename in os.listdir(STORAGE_DIR):
        if not _is_input_csv(filename):
            continue
        file_path = os.path.join(STORAGE_DIR, filename)
        df = pd.read_csv(file_path)
        for _, row in df.iterrows():
            row_dict = row.to_dict()
            try:
                out = map_row(row_dict)
            except Exception as e:
                logger.exception(f"Помилка map_row у {filename}: {e}")
                continue
            if not out:
                continue
            for item in out:
                if len(item) != 2:
                    continue
                k, v = item[0], item[1]
                pairs.append([str(k), v])

    inter_path = os.path.join(STORAGE_DIR, f"mr_intermediate_{job_id}.json")
    with open(inter_path, "w", encoding="utf-8") as f:
        json.dump(pairs, f, ensure_ascii=False)

    logger.info(f"[MR map {job_id}] пар (key,value): {len(pairs)}")
    return {"status": "success", "job_id": job_id, "pairs": len(pairs)}


class ShuffleForwardBody(BaseModel):
    job_id: str
    worker_urls: List[str]


def _append_shuffled_lines(job_id: str, rows: Sequence[Dict[str, Any]]) -> None:
    path = os.path.join(STORAGE_DIR, f"mr_shuffled_{job_id}.jsonl")
    with open(path, "a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


@app.post("/mr/shuffle-forward")
async def mr_shuffle_forward(body: ShuffleForwardBody):
    job_id = body.job_id
    worker_urls = list(body.worker_urls)
    n = len(worker_urls)
    if n == 0:
        raise HTTPException(status_code=400, detail="worker_urls порожній")

    try:
        my_ix = worker_urls.index(WORKER_URL)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"WORKER_URL {WORKER_URL} відсутній у списку worker_urls кластера",
        )

    inter_path = os.path.join(STORAGE_DIR, f"mr_intermediate_{job_id}.json")
    if not os.path.isfile(inter_path):
        raise HTTPException(status_code=404, detail=f"Немає mr_intermediate для job {job_id}")

    with open(inter_path, encoding="utf-8") as f:
        pairs = json.load(f)

    buckets: Dict[int, List[List[Any]]] = defaultdict(list)
    for pair in pairs:
        if len(pair) != 2:
            continue
        k, v = pair[0], pair[1]
        dest = partition_idx(str(k), n)
        buckets[dest].append([k, v])

    async with httpx.AsyncClient(timeout=120.0) as client:
        for dest, batch in buckets.items():
            for i in range(0, len(batch), MR_BATCH):
                chunk = batch[i : i + MR_BATCH]
                payload = [{"k": p[0], "v": p[1]} for p in chunk]
                if dest == my_ix:
                    _append_shuffled_lines(job_id, payload)
                else:
                    url = f"{worker_urls[dest]}/mr/shuffle-ingest"
                    resp = await client.post(url, json={"job_id": job_id, "rows": payload})
                    if resp.status_code != 200:
                        logger.error(f"shuffle-ingest failed {resp.status_code}: {resp.text}")
                        raise HTTPException(status_code=502, detail=f"shuffle-ingest: {resp.text}")

    logger.info(f"[MR shuffle {job_id}] воркер {my_ix} відправив локальні групи")
    return {"status": "success", "job_id": job_id, "worker_index": my_ix}


class ShuffleIngestBody(BaseModel):
    job_id: str
    rows: List[Dict[str, Any]]


@app.post("/mr/shuffle-ingest")
async def mr_shuffle_ingest(body: ShuffleIngestBody):
    _append_shuffled_lines(body.job_id, body.rows)
    return {"status": "ok", "received": len(body.rows)}


@app.post("/mr/reduce")
async def mr_run_reduce(job_id: str):
    _, reduce_group = _load_map_reduce_funcs(job_id)
    path = os.path.join(STORAGE_DIR, f"mr_shuffled_{job_id}.jsonl")
    if not os.path.isfile(path):
        raise HTTPException(status_code=404, detail=f"Немає shuffled даних для job {job_id}")

    grouped: Dict[str, List[Any]] = defaultdict(list)
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            grouped[str(obj["k"])].append(obj["v"])

    results: Dict[str, Any] = {}
    for k, vals in grouped.items():
        results[k] = reduce_group(k, vals)

    out_path = os.path.join(STORAGE_DIR, f"mr_reduce_{job_id}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False)

    logger.info(f"[MR reduce {job_id}] ключів: {len(results)}")
    return {"status": "success", "job_id": job_id, "keys": len(results)}


@app.get("/mr/output")
def mr_output(job_id: str):
    path = os.path.join(STORAGE_DIR, f"mr_reduce_{job_id}.json")
    if not os.path.isfile(path):
        raise HTTPException(status_code=404, detail="Немає результату reduce")
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return data


@app.post("/mr/cleanup")
def mr_cleanup(job_id: str):
    targets = [
        f"mr_intermediate_{job_id}.json",
        f"mr_shuffled_{job_id}.jsonl",
        f"mr_reduce_{job_id}.json",
    ]
    removed = []
    for name in targets:
        path = os.path.join(STORAGE_DIR, name)
        if os.path.isfile(path):
            try:
                os.remove(path)
                removed.append(name)
            except OSError:
                pass
    _deployed_mr.pop(job_id, None)
    return {"removed": removed}


@app.post("/map")
async def run_map(
    task_type: str,
    job_id: str,
    column: str = "price",
    mapper_expr: str | None = None,
):
    start_time = time.perf_counter()
    results = []
    total_rows = 0

    for filename in os.listdir(STORAGE_DIR):
        if filename.endswith(".csv") and not filename.startswith("map_res_"):
            file_path = os.path.join(STORAGE_DIR, filename)
            df = pd.read_csv(file_path)

            if column not in df.columns:
                logger.error(f"Колонка '{column}' відсутня у файлі {filename}")
                continue

            numeric_col = pd.to_numeric(df[column], errors="coerce").dropna()

            count = len(numeric_col)
            total_rows += count
            mapper_error: str | None = None

            if numeric_col.empty:
                res = 0.0
            elif mapper_expr:
                try:
                    env: Dict[str, Any] = {
                        **SAFE_LOCALS,
                        "series": numeric_col,
                        "pd": pd,
                    }
                    res = float(eval(mapper_expr, SAFE_GLOBALS, env))
                except Exception as e:
                    mapper_error = str(e)
                    res = 0.0
            else:
                if task_type == "max":
                    res = float(numeric_col.max())
                elif task_type == "count":
                    res = float(count)
                else:
                    res = float(numeric_col.sum())

            result_filename = f"map_res_{job_id}_{filename}"
            row = {"key": column, "val": res, "count": count}
            if mapper_error:
                row["mapper_error"] = mapper_error
            pd.DataFrame([row]).to_csv(
                os.path.join(STORAGE_DIR, result_filename), index=False
            )
            results.append(result_filename)

    duration = time.perf_counter() - start_time
    logger.info(f" [Job {job_id}] Оброблено {total_rows} рядків за {duration:.4f} сек.")

    return {
        "status": "success",
        "job_id": job_id,
        "rows": total_rows,
        "execution_time": duration,
    }


@app.get("/get-results")
async def get_results(job_id: str):
    all_results = []
    prefix = f"map_res_{job_id}_"
    for filename in os.listdir(STORAGE_DIR):
        if filename.startswith(prefix):
            try:
                df = pd.read_csv(os.path.join(STORAGE_DIR, filename))
                if len(df) > 0 and "val" in df.columns and "count" in df.columns:
                    key = str(df["key"].iloc[0]) if "key" in df.columns else "default"
                    item = {
                        "key": key,
                        "val": float(df["val"].iloc[0]),
                        "count": int(df["count"].iloc[0]),
                    }
                    if "mapper_error" in df.columns and pd.notna(df["mapper_error"].iloc[0]):
                        item["mapper_error"] = str(df["mapper_error"].iloc[0])
                    all_results.append(item)
                else:
                    logger.warning(f"Невалідні дані в {filename}")
            except Exception as e:
                logger.error(f"Помилка при читанні {filename}: {e}")
    return {"results": all_results}


@app.delete("/cleanup")
def cleanup_storage():
    try:
        removed = 0
        for filename in os.listdir(STORAGE_DIR):
            file_path = os.path.join(STORAGE_DIR, filename)
            if os.path.isfile(file_path):
                os.remove(file_path)
                removed += 1
        logger.info(f"Очищено {removed} файлів")
        return {"message": f"Storage cleared. Removed {removed} files"}
    except Exception as e:
        logger.error(f"Помилка при очищенні: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/cleanup-job")
def cleanup_job_results(job_id: str):
    try:
        removed = 0
        for filename in os.listdir(STORAGE_DIR):
            if filename.startswith(f"map_res_{job_id}_"):
                file_path = os.path.join(STORAGE_DIR, filename)
                if os.path.isfile(file_path):
                    os.remove(file_path)
                    removed += 1
        logger.info(f"Очищено {removed} результатів job {job_id}")
        return {"message": f"Cleaned {removed} files for job {job_id}"}
    except Exception as e:
        logger.error(f"Помилка при очищенні job {job_id}: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})
