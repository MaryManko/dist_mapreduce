#!/usr/bin/env python3

from __future__ import annotations

import argparse
import os
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from client.cluster_ops import (
    read_csv_from_cluster,
    run_map_reduce_py,
    upload_csv,
    unregister_worker,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Кластерна консоль (методичка)")
    parser.add_argument(
        "--master-url",
        default=os.getenv("MASTER_URL", "http://localhost:8000"),
        help="URL master API",
    )
    parser.add_argument("--send", metavar="FILE", help="Записати CSV у кластер (розбиття)")
    parser.add_argument("--read", metavar="FILE", help="Прочитати логічний файл з кластера")
    parser.add_argument(
        "--output",
        "-o",
        metavar="FILE",
        help="Куди зберегти при --read (за замовчуванням: merged_<name>)",
    )
    parser.add_argument("--map-reduce", metavar="FILE", dest="map_reduce", help="CSV для MapReduce")
    parser.add_argument("--results", metavar="FILE", help="Файл результатів MR (--map-reduce)")
    parser.add_argument("--mapper", metavar="FILE", help="mapper.py")
    parser.add_argument("--reducer", metavar="FILE", help="reducer.py")
    parser.add_argument(
        "--skip-upload",
        action="store_true",
        help="Не завантажувати CSV перед MR (дані вже на воркерах)",
    )
    parser.add_argument("--unregister-worker", metavar="URL", help="Вилучити воркер з реєстру master")

    args = parser.parse_args()

    master = args.master_url.rstrip("/")

    if args.unregister_worker:
        unregister_worker(master, args.unregister_worker)
        print("OK:", args.unregister_worker)
        return

    if args.send:
        upload_csv(master, args.send)
        print(f"Файл розподілено: {args.send}")
        return

    if args.read:
        logical = os.path.basename(args.read)
        out = args.output or f"merged_{logical}"
        read_csv_from_cluster(master, logical, out)
        print(f"Збережено локально: {out}")
        return

    if args.map_reduce:
        if not args.results or not args.mapper or not args.reducer:
            parser.error("Для --map-reduce потрібні --results, --mapper та --reducer")
        info = run_map_reduce_py(
            master,
            args.map_reduce,
            args.mapper,
            args.reducer,
            args.results,
            upload_first=not args.skip_upload,
            cleanup=True,
        )
        print(f"Job ID: {info.get('job_id')}")
        if info.get("conflicts"):
            print("УВАГА конфлікти ключів:", info["conflicts"])
        print(f"Результат записано: {args.results}")
        return

    parser.print_help()


if __name__ == "__main__":
    main()
