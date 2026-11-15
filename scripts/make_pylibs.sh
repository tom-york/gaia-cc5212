#!/usr/bin/env bash
# Genera pylibs/pylibs.zip con numpy para los ejecutores de YARN.
#
# Solo hace falta si los contenedores no ven el numpy del nodo borde. El zip
# deja numpy/ en la raíz, de modo que --archives pylibs.zip#pylibs +
# PYTHONPATH=./pylibs resuelve el import.
#
#   bash scripts/make_pylibs.sh          # numpy 2.2.6
#   bash scripts/make_pylibs.sh 2.3.1    # otra versión
set -euo pipefail

cd "$(dirname "$0")/.."

VERSION="${1:-2.2.6}"
BUILD="$(mktemp -d)"
trap 'rm -rf "$BUILD"' EXIT

echo "descargando numpy==${VERSION}"
python3 -m pip install --quiet --no-deps --target "$BUILD" "numpy==${VERSION}"

mkdir -p pylibs
python3 - "$BUILD" "pylibs/pylibs.zip" <<'PY'
import pathlib
import sys
import zipfile

src, dst = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])
with zipfile.ZipFile(dst, "w", zipfile.ZIP_DEFLATED) as zf:
    for path in sorted(src.rglob("*")):
        if path.is_file() and path != dst:
            zf.write(path, path.relative_to(src).as_posix())
PY

python3 - "pylibs/pylibs.zip" <<'PY'
import sys
import zipfile

names = zipfile.ZipFile(sys.argv[1]).namelist()
if not any(n.startswith("numpy/__init__.py") for n in names):
    raise SystemExit("el zip no tiene numpy/ en la raíz")
print(f"pylibs/pylibs.zip OK: {len(names)} archivos")
PY
