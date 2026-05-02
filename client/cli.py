import argparse
import os
import sys

import httpx

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from client.cluster_ops import (
    read_csv_from_cluster,
    run_map_reduce_py,
    upload_csv,
    unregister_worker,
)

MASTER_URL = os.getenv("MASTER_URL", "http://localhost:8000")
last_job_id = None


def get_status():
    try:
        response = httpx.get(f"{MASTER_URL}/", timeout=5.0)
        print("Статус кластера:", response.json())
    except Exception as e:
        print(f"Помилка зв'язку з Мастером: {e}")


def upload_file(file_path: str):
    try:
        upload_csv(MASTER_URL, file_path)
        print(f"Файл успішно розподілено: {file_path}")
    except Exception as e:
        print(f"Помилка завантаження: {e}")


def main():
    parser = argparse.ArgumentParser(description="Rodentia Distributed MapReduce CLI")
    subparsers = parser.add_subparsers(dest="command", help="Команди")

    subparsers.add_parser("status", help="Перевірити стан кластера")

    up = subparsers.add_parser("upload", help="Завантажити та розділити CSV файл")
    up.add_argument("file", help="Шлях до файлу")

    rd = subparsers.add_parser(
        "read",
        help="Зчитати логічний файл з кластера і склеїти локально (метадані master)",
    )
    rd.add_argument("file", help="Логічне ім'я файлу (наприклад test.csv)")
    rd.add_argument("-o", "--output", default=None, help="Шлях для збереження")

    mr_py = subparsers.add_parser(
        "mapreduce-py",
        help="Повний MR з файлів mapper.py / reducer.py (--results CSV)",
    )
    mr_py.add_argument("csv", help="CSV для обробки")
    mr_py.add_argument("--mapper", required=True, help="Шлях до mapper.py")
    mr_py.add_argument("--reducer", required=True, help="Шлях до reducer.py")
    mr_py.add_argument("--results", required=True, help="Вихідний CSV з ключами та значеннями")
    mr_py.add_argument(
        "--skip-upload",
        action="store_true",
        help="Не виконувати upload перед MR",
    )

    unr = subparsers.add_parser(
        "unregister-worker",
        help="Вилучити URL воркера з реєстру master",
    )
    unr.add_argument("worker_url", help="Наприклад http://worker2:8002")

    run_map = subparsers.add_parser("map", help="Спрощений Map (агрегація по колонці)")
    run_map.add_argument(
        "--task",
        default="sum",
        choices=["sum", "max", "count", "mean"],
        help="Тип задачі",
    )
    run_map.add_argument("--col", default="price", help="Назва колонки")
    run_map.add_argument(
        "--mapper-expr",
        dest="mapper_expr",
        default=None,
        help="Кастомний mapper Python-вираз",
    )

    run_shuffle = subparsers.add_parser("shuffle", help="Спрощений Shuffle (master групує)")
    run_shuffle.add_argument(
        "--job-id",
        dest="job_id",
        default=None,
        help="ID job (якщо не задано — з останнього map)",
    )

    run_reduce = subparsers.add_parser("reduce", help="Спрощений Reduce на master")
    run_reduce.add_argument(
        "--task",
        default="sum",
        choices=["sum", "max", "count", "mean"],
        help="Тип агрегації",
    )
    run_reduce.add_argument("--job-id", dest="job_id", default=None)
    run_reduce.add_argument("--reducer-expr", dest="reducer_expr", default=None)

    subparsers.add_parser("reset", help="Очистити дані на кластері")

    args = parser.parse_args()

    global last_job_id

    if args.command == "status":
        get_status()

    elif args.command == "upload":
        upload_file(args.file)

    elif args.command == "read":
        logical = os.path.basename(args.file)
        out = args.output or f"merged_{logical}"
        try:
            read_csv_from_cluster(MASTER_URL, logical, out)
            print(f"Збережено: {out}")
        except Exception as e:
            print(f"Помилка read: {e}")

    elif args.command == "mapreduce-py":
        try:
            info = run_map_reduce_py(
                MASTER_URL,
                args.csv,
                args.mapper,
                args.reducer,
                args.results,
                upload_first=not args.skip_upload,
                cleanup=True,
            )
            print("MR завершено:", info.get("job_id"))
            if info.get("conflicts"):
                print("Конфлікти:", info["conflicts"])
            print("Файл результатів:", args.results)
        except Exception as e:
            print(f"Помилка MR: {e}")

    elif args.command == "unregister-worker":
        try:
            unregister_worker(MASTER_URL, args.worker_url)
            print("Воркер вилучено:", args.worker_url)
        except Exception as e:
            print(f"Помилка: {e}")

    elif args.command == "map":
        try:
            params = {"task_type": args.task, "column": args.col}
            if args.mapper_expr:
                params["mapper_expr"] = args.mapper_expr
            resp = httpx.post(f"{MASTER_URL}/run-map", params=params, timeout=30.0)
            if resp.status_code != 200:
                print(f"Помилка мастера: {resp.status_code} {resp.text}")
                return
            data = resp.json()
            last_job_id = data.get("job_id")
            print("MAP:", data)
            print(f"Job ID: {last_job_id}")
        except httpx.ConnectError:
            print(f"Немає зв'язку з {MASTER_URL}")
        except Exception as e:
            print(f"Помилка MAP: {e}")

    elif args.command == "shuffle":
        try:
            job_id = args.job_id or last_job_id
            if not job_id:
                print("Вкажіть --job-id або виконайте map спочатку")
                return
            resp = httpx.post(
                f"{MASTER_URL}/run-shuffle",
                params={"job_id": job_id},
                timeout=30.0,
            )
            if resp.status_code != 200:
                print(resp.status_code, resp.text)
                return
            print(resp.json())
        except Exception as e:
            print(f"Помилка shuffle: {e}")

    elif args.command == "reduce":
        try:
            job_id = args.job_id or last_job_id
            if job_id is None:
                print("Вкажіть --job-id або виконайте map")
            else:
                params = {"task_type": args.task, "job_id": job_id}
                if args.reducer_expr:
                    params["reducer_expr"] = args.reducer_expr
                resp = httpx.post(
                    f"{MASTER_URL}/run-reduce",
                    params=params,
                    timeout=30.0,
                )
                if resp.status_code != 200:
                    print(resp.status_code, resp.text)
                    return
                result = resp.json()
                print(f"Результат ({result.get('task')}): {result.get('final_result')}")
        except Exception as e:
            print(f"Помилка reduce: {e}")

    elif args.command == "reset":
        try:
            resp = httpx.delete(f"{MASTER_URL}/reset", timeout=20.0)
            if resp.status_code != 200:
                print(resp.status_code, resp.text)
                return
            print("Очищено:", resp.json())
        except Exception as e:
            print(f"Помилка reset: {e}")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
