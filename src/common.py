"""Utilidades compartidas: config, rutas, logging y saneamiento de tipos.

Sin dependencias de Spark, para poder importarlo desde los scripts de
descarga que corren con el Python plano del nodo borde.
"""

import json
import os
import time


def load_config(path="config.json"):
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def repo_root():
    """Raíz del repositorio (el directorio que contiene src/)."""
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def resolve(path):
    """Convierte rutas relativas del config en absolutas respecto del repo."""
    if os.path.isabs(path):
        return path
    return os.path.join(repo_root(), path)


def results_path(cfg, *parts):
    """Ruta dentro de results/, separada por etapa (staging, clusters, experiments)."""
    return os.path.join(resolve(cfg["local"]["results"]), *parts)


def tagged(path, tag):
    """Sufija un nombre de archivo antes de la extensión, para variantes del barrido.

    Sin tag devuelve la ruta intacta, así la corrida canónica escribe donde siempre.
    """
    if not tag:
        return path
    root, ext = os.path.splitext(path)
    return "{}_{}{}".format(root, tag, ext)


def log(msg):
    print("[{}] {}".format(time.strftime("%H:%M:%S"), msg), flush=True)


def plain(obj):
    """Convierte tipos numpy/pyarrow a tipos nativos para json.dump."""
    if isinstance(obj, dict):
        return {str(k): plain(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [plain(v) for v in obj]
    if hasattr(obj, "item"):
        return obj.item()
    return obj


def dump_json(obj, path):
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(plain(obj), fh, indent=2, ensure_ascii=False)
    return path


def hdfs_uri(cfg, key="merged"):
    """URI completa en HDFS para una ruta del bloque 'hdfs' del config."""
    return cfg["hdfs"]["uri"].rstrip("/") + cfg["hdfs"][key]
