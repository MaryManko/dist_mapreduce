from __future__ import annotations

import io
import os
import re
import uuid
import httpx
import pandas as pd


def worker_url_to_localhost(worker_url: str) -> str:
    m = re.search(r":(\d+)$", worker_url.strip())
    port = m.group(1) if m else "8001"
    return f"http://localhost:{port}"


def _fragment_sort_key(entry: dict) -> tuple:
    name = entry.get("fragment", "")
    for pat in (r"^part_(\d+)\.csv$", r"^p(\d+)\.csv$"):
        m = re.match(pat, name)
        if m:
            return (0, int(m.group(1)))
    return (1, name)


def upload_csv(master_url: str, file_path: str) -> None:
    resp = httpx.get(f"{master_url}/", timeout=10.0)
    resp.raise_for_status()
    workers = resp.json().get("active_workers", [])
    if not workers:
        raise RuntimeError("Немає активних воркерів")

    df = pd.read_csv(file_path)
    num_workers = len(workers)
    n = len(df)
    chunk_size = (n + num_workers - 1) // num_workers if num_workers else max(n, 1)

    logical_name = os.path.basename(file_path)

    for i, worker_url in enumerate(workers):
        start_row = i * chunk_size
        end_row = min((i + 1) * chunk_size, n)
        chunk = df.iloc[start_row:end_row]
        if chunk.empty:
            continue

        buf = io.StringIO()
        chunk.to_csv(buf, index=False)
        fragment_name = f"part_{i}.csv"
        files = {"file": (fragment_name, buf.getvalue())}

        local_w = worker_url_to_localhost(worker_url)
        wr = httpx.post(f"{local_w}/upload", files=files, timeout=120.0)
        if wr.status_code != 200:
            raise RuntimeError(f"Upload на {worker_url}: {wr.status_code} {wr.text}")

        rr = httpx.post(
            f"{master_url}/record_fragment",
            params={
                "filename": logical_name,
                "worker_url": worker_url,
                "fragment_name": fragment_name,
            },
            timeout=15.0,
        )
        rr.raise_for_status()


def read_csv_from_cluster(master_url: str, logical_filename: str, output_path: str) -> None:
    meta = httpx.get(f"{master_url}/metadata", timeout=15.0).json()
    fragments = meta.get(logical_filename)
    if not fragments:
        raise RuntimeError(f"У метаданих немає запису для '{logical_filename}'")

    ordered = sorted(fragments, key=_fragment_sort_key)
    out_lines: list[str] = []
    header: str | None = None

    for idx, frag in enumerate(ordered):
        local_w = worker_url_to_localhost(frag["worker"])
        fn = frag["fragment"]
        r = httpx.get(f"{local_w}/download", params={"filename": fn}, timeout=120.0)
        if r.status_code != 200:
            raise RuntimeError(f"Не вдалося зчитати {fn} з {frag['worker']}: {r.text}")

        lines = [ln for ln in r.text.splitlines() if ln.strip() != ""]
        if not lines:
            continue

        if idx == 0:
            header = lines[0]
            out_lines.extend(lines)
            continue

        if header is not None and lines[0] == header:
            out_lines.extend(lines[1:])
        else:
            out_lines.extend(lines)

    abs_out = os.path.abspath(output_path)
    parent = os.path.dirname(abs_out)
    if parent:
        os.makedirs(parent, exist_ok=True)

    with open(abs_out, "w", encoding="utf-8", newline="") as f:
        f.write("\n".join(out_lines))
        if out_lines:
            f.write("\n")


def run_map_reduce_py(
    master_url: str,
    csv_path: str,
    mapper_path: str,
    reducer_path: str,
    results_path: str,
    *,
    upload_first: bool = True,
    cleanup: bool = True,
) -> dict:
    with open(mapper_path, encoding="utf-8") as f:
        mapper_source = f.read()
    with open(reducer_path, encoding="utf-8") as f:
        reducer_source = f.read()

    if upload_first:
        upload_csv(master_url, csv_path)

    job_id = str(uuid.uuid4())
    timeout = httpx.Timeout(600.0, connect=30.0)

    payload = {
        "job_id": job_id,
        "mapper_source": mapper_source,
        "reducer_source": reducer_source,
    }

    with httpx.Client(timeout=timeout) as client:
        d = client.post(f"{master_url}/mr/deploy", json=payload)
        d.raise_for_status()
        dj = d.json()
        if dj.get("errors"):
            raise RuntimeError(f"mr/deploy помилки на воркерах: {dj['errors']}")

        m = client.post(f"{master_url}/mr/run-map", params={"job_id": job_id})
        m.raise_for_status()

        s = client.post(f"{master_url}/mr/run-shuffle", params={"job_id": job_id})
        s.raise_for_status()
        sj = s.json()
        if sj.get("errors"):
            raise RuntimeError(f"mr/run-shuffle: {sj['errors']}")

        rd = client.post(f"{master_url}/mr/run-reduce", params={"job_id": job_id})
        rd.raise_for_status()

        res = client.get(f"{master_url}/mr/result", params={"job_id": job_id})
        res.raise_for_status()
        pack = res.json()

        if cleanup:
            client.post(f"{master_url}/mr/cleanup", params={"job_id": job_id})

    merged = pack.get("result") or {}
    rows = [(str(k), v) for k, v in merged.items()]
    pd.DataFrame(rows, columns=["key", "value"]).to_csv(results_path, index=False)

    return {"job_id": job_id, **pack}


def unregister_worker(master_url: str, worker_url: str) -> None:
    r = httpx.post(
        f"{master_url}/unregister",
        params={"worker_url": worker_url},
        timeout=15.0,
    )
    r.raise_for_status()
