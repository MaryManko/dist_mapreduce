from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import httpx
import logging

import os
import asyncio
import time
from typing import List, Dict

DEFAULT_TIMEOUT = float(os.getenv("MASTER_TIMEOUT", 15.0))
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



@app.post("/run-map")
async def run_map_across_cluster(task_type: str = "sum", column: str = "price"):
    if task_type not in ["sum", "max", "mean"]:
        raise HTTPException(status_code=400, detail="Непідтримуваний тип задачі")

    job_id = str(int(time.time()))
    responses = []
    
    async with lock:
        active_workers = workers.copy()
    
    async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as client:
        for worker_url in active_workers:
            try:
                url = f"{worker_url}/map?task_type={task_type}&column={column}&job_id={job_id}"
                resp = await client.post(url)
                responses.append(resp.json())
            except Exception as e:
                logger.error(f"Помилка зв'язку з воркером {worker_url}: {e}")
                responses.append({"worker": worker_url, "error": str(e)})
                
    return {"message": "MAP started", "job_id": job_id, "details": responses}

@app.post("/run-reduce")
async def run_reduce_across_cluster(job_id: str, task_type: str = "sum"):
    final_val = 0.0 if task_type != "max" else -float('inf')
    total_count = 0
    
    async with lock:
        active_workers = workers.copy()

    async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as client:
        for worker_url in active_workers:
            try:
                resp = await client.get(f"{worker_url}/get-results?job_id={job_id}")
                if resp.status_code == 200:
                    for res in resp.json().get("results", []):
                        val, count = res["val"], res["count"]
                        if task_type == "max": 
                            final_val = max(final_val, val)
                        else: 
                            final_val += val
                        total_count += count

            except Exception as e:
                logger.error(f"Помилка збору результатів з {worker_url}: {e}")

    if task_type == "mean" and total_count > 0: 
        final_val = final_val / total_count
    
    asyncio.create_task(cleanup_map_results(job_id, active_workers))
        
    return {"final_result": final_val, "job_id": job_id, "task": task_type}

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