from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import httpx
import logging

import os
import asyncio
import time
from typing import List, Dict, Any

DEFAULT_TIMEOUT = float(os.getenv("MASTER_TIMEOUT", 15.0))
MR_TIMEOUT = float(os.getenv("MASTER_MR_TIMEOUT", 300.0))
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ComputeEngine.Master")

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_methods=["*"],
    allow_headers=["*"],
)

lock = asyncio.Lock()
workers: List[str] = []
file_metadata: Dict[str, List[Dict[str, str]]] = {}
shuffled_jobs: Dict[str, Dict[str, List[Dict[str, float]]]] = {}

current_dir = os.path.dirname(os.path.abspath(__file__))
static_dir = os.path.join(current_dir, "static")
if not os.path.exists(static_dir): 
    os.makedirs(static_dir)


app.mount("/static", StaticFiles(directory=static_dir), name="static")

@app.get("/")
async def index():
    async with lock:
        workers_copy = workers.copy()
    return {"status": "Master is running", "active_workers": workers_copy}

@app.post("/register")
async def register_worker(worker_url: str):
    async with lock: 
        if worker_url not in workers:
            workers.append(worker_url)
            logger.info(f"Воркер зареєстровано: {worker_url}")
    return {"message": "Worker registered successfully"}


@app.post("/unregister")
async def unregister_worker(worker_url: str):
    async with lock:
        if worker_url in workers:
            workers.remove(worker_url)
            logger.info(f"Воркер вилучено з реєстру: {worker_url}")
            return {"message": "Worker unregistered"}
    return {"message": "Worker was not in the list"}

@app.post("/record_fragment")
async def record_fragment(filename: str, worker_url: str, fragment_name: str):
    async with lock:  
        if filename not in file_metadata:
            file_metadata[filename] = []
        file_metadata[filename].append({"worker": worker_url, "fragment": fragment_name})
    return {"status": "Metadata updated"}

@app.get("/metadata")
async def get_metadata():
    async with lock:
        metadata_copy = dict(file_metadata)
    return metadata_copy


class MRDeployPayload(BaseModel):
    job_id: str
    mapper_source: str
    reducer_source: str


@app.post("/mr/deploy")
async def mr_deploy_cluster(payload: MRDeployPayload):
    async with lock:
        ws = list(workers)
    if not ws:
        raise HTTPException(status_code=400, detail="Немає зареєстрованих воркерів")

    deploy_body = (
        payload.model_dump() if hasattr(payload, "model_dump") else payload.dict()
    )
    errors = []
    async with httpx.AsyncClient(timeout=MR_TIMEOUT) as client:
        for w in ws:
            try:
                r = await client.post(f"{w}/mr/deploy", json=deploy_body)
                if r.status_code != 200:
                    errors.append({"worker": w, "error": r.text})
            except Exception as e:
                errors.append({"worker": w, "error": str(e)})

    return {"job_id": payload.job_id, "workers": ws, "errors": errors}


@app.post("/mr/run-map")
async def mr_run_map_cluster(job_id: str):
    async with lock:
        ws = list(workers)
    if not ws:
        raise HTTPException(status_code=400, detail="Немає воркерів")

    out = []
    async with httpx.AsyncClient(timeout=MR_TIMEOUT) as client:
        for w in ws:
            try:
                r = await client.post(f"{w}/mr/map", params={"job_id": job_id})
                out.append({"worker": w, "response": r.json()})
            except Exception as e:
                out.append({"worker": w, "error": str(e)})
    return {"job_id": job_id, "details": out}


@app.post("/mr/run-shuffle")
async def mr_run_shuffle_cluster(job_id: str):
    async with lock:
        ws_sorted = sorted(workers.copy())

    if len(ws_sorted) < 1:
        raise HTTPException(status_code=400, detail="Немає воркерів")

    async with httpx.AsyncClient(timeout=MR_TIMEOUT) as client:
        tasks = [
            client.post(
                f"{w}/mr/shuffle-forward",
                json={"job_id": job_id, "worker_urls": ws_sorted},
            )
            for w in ws_sorted
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    errors = []
    for w, res in zip(ws_sorted, results):
        if isinstance(res, Exception):
            errors.append({"worker": w, "error": str(res)})
        elif hasattr(res, "status_code") and res.status_code != 200:
            errors.append({"worker": w, "error": res.text})

    return {"job_id": job_id, "workers_order": ws_sorted, "errors": errors}


@app.post("/mr/run-reduce")
async def mr_run_reduce_cluster(job_id: str):
    async with lock:
        ws = list(workers)

    out = []
    async with httpx.AsyncClient(timeout=MR_TIMEOUT) as client:
        for w in ws:
            try:
                r = await client.post(f"{w}/mr/reduce", params={"job_id": job_id})
                out.append({"worker": w, "response": r.json()})
            except Exception as e:
                out.append({"worker": w, "error": str(e)})
    return {"job_id": job_id, "details": out}


@app.get("/mr/result")
async def mr_collect_result(job_id: str):
    async with lock:
        ws = list(workers)

    merged: Dict[str, Any] = {}
    conflicts: List[Dict[str, Any]] = []

    async with httpx.AsyncClient(timeout=MR_TIMEOUT) as client:
        for w in ws:
            try:
                r = await client.get(f"{w}/mr/output", params={"job_id": job_id})
                if r.status_code == 404:
                    continue
                if r.status_code != 200:
                    conflicts.append({"worker": w, "error": r.text})
                    continue
                partial = r.json()
                if not isinstance(partial, dict):
                    continue
                for k, val in partial.items():
                    if k in merged:
                        if merged[k] != val:
                            conflicts.append(
                                {"key": k, "existing": merged[k], "new": val, "worker": w}
                            )
                    else:
                        merged[k] = val
            except Exception as e:
                conflicts.append({"worker": w, "error": str(e)})

    return {"job_id": job_id, "result": merged, "conflicts": conflicts}


@app.post("/mr/cleanup")
async def mr_cleanup_cluster(job_id: str):
    async with lock:
        ws = list(workers)

    async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as client:
        for w in ws:
            try:
                await client.post(f"{w}/mr/cleanup", params={"job_id": job_id})
            except Exception as e:
                logger.warning(f"mr cleanup {w}: {e}")
    return {"job_id": job_id}

@app.post("/run-map")
async def run_map_across_cluster(
    task_type: str = "sum",
    column: str = "price",
    mapper_expr: str | None = None
):
    if task_type not in ["sum", "max", "mean", "count"]:
        raise HTTPException(status_code=400, detail="Непідтримуваний тип задачі")

    job_id = str(int(time.time()))
    responses = []
    
    async with lock:
        active_workers = workers.copy()
    
    async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as client:
        for worker_url in active_workers:
            try:
                url = f"{worker_url}/map?task_type={task_type}&column={column}&job_id={job_id}"
                if mapper_expr:
                    url += f"&mapper_expr={mapper_expr}"
                resp = await client.post(url)
                responses.append(resp.json())
            except Exception as e:
                logger.error(f"Помилка зв'язку з воркером {worker_url}: {e}")
                responses.append({"worker": worker_url, "error": str(e)})
                
    return {
        "message": "MAP started",
        "job_id": job_id,
        "details": responses,
        "mapper_expr": mapper_expr,
    }

@app.post("/run-shuffle")
async def run_shuffle(job_id: str):
    async with lock:
        active_workers = workers.copy()

    grouped: Dict[str, List[Dict[str, float]]] = {}
    mapper_errors: List[Dict[str, str]] = []

    async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as client:
        for worker_url in active_workers:
            try:
                resp = await client.get(f"{worker_url}/get-results?job_id={job_id}")
                if resp.status_code != 200:
                    continue

                for res in resp.json().get("results", []):
                    key = str(res.get("key", "default"))
                    grouped.setdefault(key, []).append({
                        "val": float(res.get("val", 0.0)),
                        "count": int(res.get("count", 0)),
                    })
                    if "mapper_error" in res:
                        mapper_errors.append({
                            "worker": worker_url,
                            "key": key,
                            "error": str(res["mapper_error"]),
                        })
            except Exception as e:
                logger.error(f"Помилка shuffle зі збором з {worker_url}: {e}")

    async with lock:
        shuffled_jobs[job_id] = grouped

    return {
        "message": "SHUFFLE completed",
        "job_id": job_id,
        "keys": list(grouped.keys()),
        "groups_count": len(grouped),
        "mapper_errors": mapper_errors,
    }

@app.post("/run-reduce")
async def run_reduce_across_cluster(
    job_id: str,
    task_type: str = "sum",
    reducer_expr: str | None = None
):
    async with lock:
        grouped = shuffled_jobs.get(job_id)

    if grouped is None:
        raise HTTPException(
            status_code=400,
            detail="Для цього job немає shuffle-даних. Спочатку виконайте /run-shuffle."
        )

    values: List[float] = []
    counts: List[int] = []
    for bucket in grouped.values():
        for item in bucket:
            values.append(float(item.get("val", 0.0)))
            counts.append(int(item.get("count", 0)))

    total_count = sum(counts)
    if total_count == 0 and task_type != "count":
        return {"final_result": 0, "job_id": job_id, "task": task_type}

    if reducer_expr:
        try:
            final_val = float(eval(
                reducer_expr,
                {"__builtins__": {}},
                {
                    "values": values,
                    "counts": counts,
                    "total_count": total_count,
                    "total_value": sum(values),
                    "max_value": max(values) if values else 0.0,
                    "min_value": min(values) if values else 0.0,
                    "sum": sum,
                    "min": min,
                    "max": max,
                    "len": len,
                }
            ))
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Некоректний reducer_expr: {e}") from e
    else:
        if task_type == "max":
            final_val = max(values) if values else 0.0
        elif task_type == "mean":
            final_val = (sum(values) / total_count) if total_count else 0.0
        elif task_type == "count":
            final_val = float(total_count)
        else:
            final_val = sum(values)

    async with lock:
        active_workers = workers.copy()
        shuffled_jobs.pop(job_id, None)

    asyncio.create_task(cleanup_map_results(job_id, active_workers))

    return {
        "final_result": final_val,
        "job_id": job_id,
        "task": task_type,
        "reducer_expr": reducer_expr,
    }

async def cleanup_map_results(job_id: str, active_workers: list):
    await asyncio.sleep(1) 
    async with httpx.AsyncClient(timeout=10.0) as client:
        for worker_url in active_workers:
            try:
                await client.get(f"{worker_url}/cleanup-job?job_id={job_id}")
                logger.info(f"Очищено результати job {job_id} на {worker_url}")
            except Exception as e:
                logger.warning(f"Не вдалося очистити результати job {job_id} на {worker_url}: {e}")

@app.delete("/reset")
async def reset_cluster():
    async with lock:
        file_metadata.clear()
        shuffled_jobs.clear()
        active_workers = workers.copy()
        
    async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as client:
        for worker_url in active_workers:
            try:
                await client.delete(f"{worker_url}/cleanup")
            except Exception as e:
                logger.error(f"Не вдалося очистити воркер {worker_url}: {e}")
                
    return {"message": "Cluster reset successful"}

@app.get("/dashboard", response_class=HTMLResponse)
async def get_dashboard():
    index_path = os.path.join(static_dir, "index.html")
    try:
        with open(index_path, "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return f"Помилка: Файл index.html не знайдено за шляхом {index_path}!"