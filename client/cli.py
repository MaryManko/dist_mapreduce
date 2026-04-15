import argparse
import httpx
import pandas as pd
import io
import os

MASTER_URL = "http://localhost:8000"

def get_status():
    """Перевірка зв'язку з Мастером"""
    try:
        response = httpx.get(f"{MASTER_URL}/")
        print("Статус кластера:", response.json())
    except Exception as e:
        print(f"Помилка: {e}")

def upload_file(file_path: str):
    """Розбиття файлу та завантаження на воркери"""
    workers = httpx.get(f"{MASTER_URL}/").json().get("active_workers", [])
    if not workers:
        print("Немає активних воркерів!")
        return

    df = pd.read_csv(file_path)
    num_workers = len(workers)
    chunk_size = len(df) // num_workers + 1

    print(f"Розділяємо файл на {num_workers} частини...")

    for i, worker_url in enumerate(workers):
        start_row = i * chunk_size
        end_row = (i + 1) * chunk_size
        chunk = df.iloc[start_row:end_row]

        if chunk.empty:
            continue

        csv_buffer = io.StringIO()
        chunk.to_csv(csv_buffer, index=False)
        files = {'file': (f"part_{i}.csv", csv_buffer.getvalue())}

        external_worker_url = worker_url.replace("worker1", "localhost").replace("worker2", "localhost")
        
        try:
            resp = httpx.post(f"{external_worker_url}/upload", files=files)
            print(f"Шматок {i} надіслано на {worker_url}: {resp.json()}")

            httpx.post(f"{MASTER_URL}/record_fragment", params={
                "filename": os.path.basename(file_path),
                "worker_url": worker_url,
                "fragment_name": f"part_{i}.csv"
            })
        except Exception as e:
            print(f"Помилка при завантаженні на {worker_url}: {e}")

def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("status")
    
    up = subparsers.add_parser("upload")
    up.add_argument("file", help="Шлях до CSV файлу")

    run_map = subparsers.add_parser("map", help="Запустити фазу Map на кластері")
    run_map.add_argument("--task", default="count", help="Тип завдання")

    subparsers.add_parser("reduce", help="Зібрати результати з усього кластера")

    args = parser.parse_args()

    if args.command == "status":
        get_status()
    elif args.command == "upload":
        upload_file(args.file)
    elif args.command == "map":
        resp = httpx.post(f"{MASTER_URL}/run-map", params={"task_type": args.task})
        print("Команда надіслана кластеру:", resp.json())
    elif args.command == "reduce":
        resp = httpx.post(f"{MASTER_URL}/run-reduce")
        result = resp.json()
        print(f"ФІНАЛЬНИЙ РЕЗУЛЬТАТ: {result['final_result']}")
        print(f"Деталі по воркерах: {result['breakdown']}")

if __name__ == "__main__":
    main()