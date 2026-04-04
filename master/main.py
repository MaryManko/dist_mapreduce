from fastapi import FastAPI

app = FastAPI()

workers = []

@app.get("/")
def index():
    return {"status": "Master is running", "active_workers": workers}

@app.post("/register")
def register_worker(worker_url: str):
    if worker_url not in workers:
        workers.append(worker_url)
        print(f"Registered new worker: {worker_url}")
    return {"message": "Worker registered successfully"}