from fastapi import FastAPI, UploadFile, File
import httpx
import os
import asyncio
import pandas as pd

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
async def run_map(task_type: str):
    """Виконує функцію Map над локальними файлами"""
    results = []
    
    for filename in os.listdir(STORAGE_DIR):
        if filename.endswith(".csv") and "result" not in filename:
            file_path = os.path.join(STORAGE_DIR, filename)
            df = pd.read_csv(file_path)
            
            if task_type == "count":
                res = len(df)
            
            result_filename = f"map_res_{filename}"
            result_path = os.path.join(STORAGE_DIR, result_filename)
            
            pd.DataFrame([{"result": res}]).to_csv(result_path, index=False)
            results.append(result_filename)
            
    return {"status": "Map completed", "intermediate_files": results}