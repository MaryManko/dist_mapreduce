import argparse
import httpx
import pandas as pd
import io
import os
import re

MASTER_URL = "http://localhost:8000"

def get_status():
    """Перевірка статусу кластера та списку активних воркерів"""
    try:
        response = httpx.get(f"{MASTER_URL}/", timeout=5.0)
        print("Статус кластера:", response.json())
    except Exception as e:
        print(f"Помилка зв'язку з Мастером: {e}")

def upload_file(file_path: str):
    """Розбиття великого файлу та розподіл чанків по воркерах"""
    try:
        resp = httpx.get(f"{MASTER_URL}/")
        workers = resp.json().get("active_workers", [])
        
        if not workers:
            print("Помилка: Немає активних воркерів у кластері!")
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

            port_match = re.search(r":(\d+)", worker_url)
            port = port_match.group(1) if port_match else "8001"
            external_worker_url = f"http://localhost:{port}"
            
            try:
                worker_resp = httpx.post(f"{external_worker_url}/upload", files=files, timeout=10.0)
                print(f"✅ Шматок {i} успішно надіслано на {worker_url}")

                httpx.post(f"{MASTER_URL}/record_fragment", params={
                    "filename": os.path.basename(file_path),
                    "worker_url": worker_url,
                    "fragment_name": f"part_{i}.csv"
                })
            except Exception as e:
                print(f"❌ Помилка при завантаженні на воркер {worker_url}: {e}")

    except FileNotFoundError:
        print(f"Помилка: Файл '{file_path}' не знайдено.")
    except Exception as e:
        print(f"Критична помилка при завантаженні: {e}")

def main():
    parser = argparse.ArgumentParser(description="Rodentia Distributed MapReduce CLI")
    subparsers = parser.add_subparsers(dest="command", help="Команди")

    subparsers.add_parser("status", help="Перевірити стан кластера")
    
    up = subparsers.add_parser("upload", help="Завантажити та розділити CSV файл")
    up.add_argument("file", help="Шлях до файлу")

    run_map = subparsers.add_parser("map", help="Запустити фазу Map")
    run_map.add_argument("--task", default="sum", choices=["sum", "max", "count", "mean"], help="Тип задачі")
    run_map.add_argument("--col", default="price", help="Назва колонки")

    run_reduce = subparsers.add_parser("reduce", help="Запустити фазу Reduce (агрегація)")
    run_reduce.add_argument("--task", default="sum", choices=["sum", "max", "count", "mean"], help="Тип агрегації")

    subparsers.add_parser("reset", help="Очистити дані на кластері")

    args = parser.parse_args()

    if args.command == "status":
        get_status()
    
    elif args.command == "upload":
        upload_file(args.file)
    
    elif args.command == "map":
        resp = httpx.post(f"{MASTER_URL}/run-map", params={"task_type": args.task, "column": args.col})
        print("Мастер прийняв задачу Map:", resp.json())
    
    elif args.command == "reduce":
        try:
            resp = httpx.post(f"{MASTER_URL}/run-reduce", params={"task_type": args.task})
            result = resp.json()
            print(f"ФІНАЛЬНИЙ РЕЗУЛЬТАТ ({result.get('task')}): {result.get('final_result')}")
        except Exception as e:
            print(f"Помилка при виконанні Reduce: {e}")
            
    elif args.command == "reset":
        resp = httpx.delete(f"{MASTER_URL}/reset")
        print("Кластер очищено:", resp.json())
    
    else:
        parser.print_help()

if __name__ == "__main__":
    main()