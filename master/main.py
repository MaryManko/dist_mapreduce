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

@app.post("/run-reduce")
async def run_reduce_across_cluster():
    total_sum = 0
    details = []
    
    async with httpx.AsyncClient() as client:
        for worker_url in workers:
            try:
                resp = await client.get(f"{worker_url}/get-results")
                data = resp.json()
                worker_sum = sum(data["results"])
                total_sum += worker_sum
                details.append({"worker": worker_url, "count": worker_sum})
            except Exception as e:
                details.append({"worker": worker_url, "error": str(e)})
                
    return {
        "final_result": total_sum,
        "message": "Aggregation completed",
        "breakdown": details
    }