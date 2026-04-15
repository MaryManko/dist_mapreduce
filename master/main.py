from fastapi import FastAPI
import httpx  

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
async def run_map_across_cluster(task_type: str = "count"):
    responses = []
    async with httpx.AsyncClient() as client:
        for worker_url in workers:
            try:
                resp = await client.post(f"{worker_url}/map?task_type={task_type}")
                responses.append(resp.json())
            except Exception as e:
                responses.append({"worker": worker_url, "error": str(e)})
    return {"message": "Cluster Map initiated", "details": responses}

@app.post("/run-map")
async def run_map_across_cluster(task_type: str = "sum", column: str = "price"):
    responses = []
    async with httpx.AsyncClient() as client:
        for worker_url in workers:
            try:
                resp = await client.post(f"{worker_url}/map?task_type={task_type}&column={column}")
                
                if resp.status_code == 200:
                    responses.append(resp.json())
                else:
                    responses.append({"worker": worker_url, "error": f"Worker returned {resp.status_code}"})
            except Exception as e:
                responses.append({"worker": worker_url, "error": str(e)})
    
    return {"message": "Cluster Map initiated", "details": responses}

@app.delete("/reset")
async def reset_cluster():
    global file_metadata
    file_metadata = {}
    async with httpx.AsyncClient() as client:
        for worker_url in workers:
            try:
                await client.delete(f"{worker_url}/cleanup")
            except:
                pass
    return {"message": "Cluster reset successful"}

@app.post("/run-reduce")
async def run_reduce_across_cluster():
    total_sum = 0
    details = []
    
    if not workers:
        return {"final_result": 0, "message": "No active workers", "breakdown": []}

    async with httpx.AsyncClient() as client:
        for worker_url in workers:
            try:
                resp = await client.get(f"{worker_url}/get-results")
                if resp.status_code == 200:
                    data = resp.json()
                    worker_sum = sum(data.get("results", []))
                    total_sum += worker_sum
                    details.append({"worker": worker_url, "count": worker_sum})
                else:
                    details.append({"worker": worker_url, "error": f"Status {resp.status_code}"})
            except Exception as e:
                details.append({"worker": worker_url, "error": str(e)})
                
    return {
        "final_result": total_sum,
        "message": "Aggregation completed",
        "breakdown": details
    }