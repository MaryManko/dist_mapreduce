from fastapi import FastAPI, UploadFile, File
import httpx
import os
import asyncio
import pandas as pd
import shutil

app = FastAPI()

STORAGE_DIR = "storage"
os.makedirs(STORAGE_DIR, exist_ok=True)

MASTER_URL = os.getenv("MASTER_URL", "http://master:8000")
WORKER_URL = os.getenv("WORKER_URL", "http://localhost:8001")

@app.on_event("startup")
async def startup_event():
    async with httpx.AsyncClient() as client:
        try:
            await client.post(f"{MASTER_URL}/register?worker_url={WORKER_URL}")
        except Exception as e:
            print(f"Failed to register at Master: {e}")

@app.get("/")
def index():
    return {"status": "Worker is up", "worker_url": WORKER_URL}

@app.post("/upload")
async def upload_fragment(file: UploadFile = File(...)):
    """Зберігає отриманий файл у папку storage"""
    file_path = os.path.join(STORAGE_DIR, file.filename)
    with open(file_path, "wb") as f:
        content = await file.read()
        f.write(content)
    return {"message": f"Saved {file.filename}", "path": file_path}

@app.post("/map")
async def run_map(task_type: str, column: str = "price"):

    results = []

    for filename in os.listdir(STORAGE_DIR):
        if filename.endswith(".csv") and "result" not in filename:
            df = pd.read_csv(os.path.join(STORAGE_DIR, filename))
            
            res_sum = float(df[column].sum())
            res_count = len(df)
            res_max = float(df[column].max())

            if task_type == "sum" or task_type == "mean":
                res = res_sum
            elif task_type == "max":
                res = res_max
            else:
                res = float(res_count)
            
            result_filename = f"map_res_{filename}"
            pd.DataFrame([{"result": res, "count": res_count}]).to_csv(
                os.path.join(STORAGE_DIR, result_filename), index=False
            )
            results.append(result_filename)
            
    return {"status": "Map completed", "intermediate_files": results}

@app.get("/get-results")
async def get_results():
    
    all_results = []
    for filename in os.listdir(STORAGE_DIR):
        if filename.startswith("map_res_"):
            df = pd.read_csv(os.path.join(STORAGE_DIR, filename))
            all_results.append({
                "val": float(df['result'].iloc[0]),
                "count": int(df['count'].iloc[0])
            })
    return {"worker_url": WORKER_URL, "results": all_results}

@app.delete("/cleanup")
def cleanup_storage():
    """Видаляє всі файли в папці storage"""
    for filename in os.listdir(STORAGE_DIR):
        file_path = os.path.join(STORAGE_DIR, filename)
        if os.path.isfile(file_path):
            os.remove(file_path)
    return {"message": "Storage cleared"}