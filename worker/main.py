from fastapi import FastAPI
import httpx
import os
import asyncio

app = FastAPI()

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