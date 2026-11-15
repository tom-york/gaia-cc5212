#!/usr/bin/env python3
"""Descarga la partición Norder=4/Dir=0 de UniverseTBD/mmu_gaia_gaia.

Se corre en el nodo borde con el Python del sistema (sin Spark ni PyArrow):
solo necesita `requests`. Los archivos quedan en `raw/` con el nombre
original (`Npix=N.parquet`), que es lo que `src/stage.py` usa para derivar
la columna `source_tile`.

Uso:
    python3 src/download.py --config config.json --check-only
    python3 src/download.py --config config.json
"""

import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import dump_json, load_config, log, resolve, results_path  # noqa: E402

TIMEOUT = 180
RETRIES = 5


def list_files(cfg):
    """Lista (path, size) de los parquet de la partición, vía API de HF."""
    r = requests.get(cfg["dataset"]["url_api"], params={"limit": 1000}, timeout=90)
    r.raise_for_status()
    entries = [
        (f["path"], int(f["size"]))
        for f in r.json()
        if f.get("type") == "file" and f["path"].endswith(".parquet")
    ]
    if not entries:
        raise RuntimeError("la API de HF no devolvió archivos parquet")
    return sorted(entries)


def check(cfg):
    """Verifica conectividad y listado. No descarga nada."""
    try:
        r = requests.head("https://huggingface.co", timeout=20)
        log("huggingface.co alcanzable (HTTP {})".format(r.status_code))
        files = list_files(cfg)
        total = sum(size for _, size in files)
        log("listado OK: {} archivos, {:.2f} GiB".format(len(files), total / 2**30))
        return True
    except Exception as exc:  # noqa: BLE001 - queremos el motivo, no el tipo
        log("sin acceso a Hugging Face: {}: {}".format(type(exc).__name__, exc))
        return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--check-only", action="store_true")
    ap.add_argument("--workers", type=int)
    args = ap.parse_args()

    cfg = load_config(args.config)
    if args.check_only:
        return 0 if check(cfg) else 3
    if not check(cfg):
        return 3

    raw_dir = os.environ.get("GAIA_RAW_DIR") or resolve(cfg["local"]["raw"])
    results_dir = results_path(cfg, "staging")
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(results_dir, exist_ok=True)

    files = list_files(cfg)
    workers = args.workers or cfg["download"]["workers"]
    base_url = cfg["dataset"]["url_resolve"]
    expected_total = sum(size for _, size in files)

    with open(os.path.join(results_dir, "files.tsv"), "w", encoding="utf-8") as fh:
        for path, size in files:
            fh.write("{}\t{}\n".format(path, size))

    def fetch(item):
        path, size = item
        dst = os.path.join(raw_dir, os.path.basename(path))
        if os.path.exists(dst) and os.path.getsize(dst) == size:
            return dst, size, "skip"
        url = base_url + path
        err = None
        for attempt in range(RETRIES):
            try:
                r = requests.get(url, timeout=TIMEOUT)
                r.raise_for_status()
                tmp = dst + ".part"
                with open(tmp, "wb") as fh:
                    fh.write(r.content)
                os.replace(tmp, dst)
                got = os.path.getsize(dst)
                if got != size:
                    raise IOError("tamaño {} != {}".format(got, size))
                return dst, size, "ok"
            except Exception as exc:  # noqa: BLE001
                err = exc
                time.sleep(2 + 2 * attempt)
        return dst, 0, "FAIL {}: {}".format(type(err).__name__, err)

    log("descargando {} archivos con {} hilos".format(len(files), workers))
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(fetch, files))
    elapsed = time.perf_counter() - t0

    failed = [r for r in results if r[2].startswith("FAIL")]
    skipped = [r for r in results if r[2] == "skip"]
    total = sum(r[1] for r in results)
    summary = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "dataset": cfg["dataset"],
        "n_files": len(results),
        "n_downloaded": len(results) - len(failed) - len(skipped),
        "n_skipped": len(skipped),
        "n_failed": len(failed),
        "bytes": total,
        "gib": round(total / 2**30, 3),
        "expected_bytes": expected_total,
        "elapsed_s": round(elapsed, 1),
        "failures": [{"path": r[0], "error": r[2]} for r in failed],
    }
    dump_json(summary, os.path.join(results_dir, "download_summary.json"))

    log(
        "{} archivos, {:.2f} GiB ({:.2f} GiB esperados), {} omitidos, {} fallidos, {:.0f}s".format(
            len(results), total / 2**30, expected_total / 2**30,
            len(skipped), len(failed), elapsed
        )
    )
    if failed:
        for item in failed[:10]:
            log("  FALLO {}".format(item[2]))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
