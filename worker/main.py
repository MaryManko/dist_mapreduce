from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
import httpx
import os

import pandas as pd
import logging
import time

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

@app.post("/upload")
async def upload_fragment(file: UploadFile = File(...)):
    file_path = os.path.join(STORAGE_DIR, file.filename)
    with open(file_path, "wb") as f:
        content = await file.read()
        f.write(content)
    return {"message": f"Saved {file.filename}", "path": file_path}

@app.post("/map") 
async def run_map(task_type: str, job_id: str, column: str = "price"):
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
            
            numeric_col = pd.to_numeric(df[column], errors='coerce').dropna()
            
            if numeric_col.empty:
                res, count = 0.0, 0
            else:
                count = len(numeric_col)
                if task_type == "max":
                    res = float(numeric_col.max())
                else:
                    res = float(numeric_col.sum()) 
                total_rows += count

            result_filename = f"map_res_{job_id}_{filename}"
            pd.DataFrame([{"val": res, "count": count}]).to_csv(
                os.path.join(STORAGE_DIR, result_filename), index=False
            )
            results.append(result_filename)

    duration = time.perf_counter() - start_time
    logger.info(f" [Job {job_id}] Оброблено {total_rows} рядків за {duration:.4f} сек.")
    
    return {
        "status": "success", 
        "job_id": job_id, 
        "rows": total_rows, 
        "execution_time": duration
    }

@app.get("/get-results")
async def get_results(job_id: str): 
    all_results = []
    prefix = f"map_res_{job_id}_"
    for filename in os.listdir(STORAGE_DIR):
        if filename.startswith(prefix):
            df = pd.read_csv(os.path.join(STORAGE_DIR, filename))

            all_results.append({
                "val": float(df['val'].iloc[0]), 
                "count": int(df['count'].iloc[0])
            })
    return {"results": all_results}

@app.delete("/cleanup")
def cleanup_storage():
    
    for filename in os.listdir(STORAGE_DIR):
        file_path = os.path.join(STORAGE_DIR, filename)
        if os.path.isfile(file_path):
            os.remove(file_path)
    return {"message": "Storage cleared"}