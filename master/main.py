from fastapi import FastAPI
import httpx
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Rodentia.Master")

app = FastAPI()

workers = []
file_metadata = {}

@app.get("/")
def index():
    return {"status": "Master is running", "active_workers": workers}

@app.post("/register")
def register_worker(worker_url: str):
    if worker_url not in workers:
        workers.append(worker_url)

    return {"message": "Worker registered successfully"}

@app.post("/record_fragment")
def record_fragment(filename: str, worker_url: str, fragment_name: str):
    if filename not in file_metadata:
        file_metadata[filename] = []
    file_metadata[filename].append({"worker": worker_url, "fragment": fragment_name})
    return {"status": "Metadata updated"}

@app.get("/metadata")
def get_metadata():
    return file_metadata



@app.post("/run-map")
async def run_map_across_cluster(task_type: str = "sum", column: str = "price"):
    responses = []
    async with httpx.AsyncClient() as client:
        for worker_url in workers:
            try:
                resp = await client.post(
                    f"{worker_url}/map?task_type={task_type}&column={column}", 
                    timeout=10.0
                )
                if resp.status_code == 200:
                    responses.append(resp.json())
                else:
                    logger.error(f"Воркер {worker_url} повернув {resp.status_code}")
                    responses.append({"worker": worker_url, "error": f"Status {resp.status_code}"})
            except Exception as e:
                logger.error(f"Помилка зв'язку з {worker_url}: {e}")
                responses.append({"worker": worker_url, "error": str(e)})
    
    return {"message": "Cluster Map initiated", "details": responses}



@app.post("/run-reduce")
async def run_reduce_across_cluster(task_type: str = "sum"):
    final_val = 0.0 if task_type != "max" else -float('inf')
    total_count = 0
    details = []
    
    async with httpx.AsyncClient() as client:
        for worker_url in workers:
            try:
                resp = await client.get(f"{worker_url}/get-results", timeout=10.0)
                if resp.status_code == 200:
                    worker_data = resp.json().get("results", [])
                    for res in worker_data:
                        val = res["val"]
                        count = res["count"]
                        
                        if task_type == "max":
                            final_val = max(final_val, val)
                        else: 
                            final_val += val
                        total_count += count
                    
                    details.append({"worker": worker_url, "status": "ok"})
                    await client.delete(f"{worker_url}/cleanup") 
            except Exception as e:
                logger.error(f"Error on {worker_url}: {e}")
    
    if task_type == "mean" and total_count > 0:
        final_val = final_val / total_count

    return {"final_result": final_val, "task": task_type, "breakdown": details}

@app.delete("/reset")
async def reset_cluster():
    global file_metadata
    file_metadata = {}
    async with httpx.AsyncClient() as client:
        for worker_url in workers:
            try:
                await client.delete(f"{worker_url}/cleanup")
            except Exception:
                pass
    return {"message": "Cluster reset successful"}