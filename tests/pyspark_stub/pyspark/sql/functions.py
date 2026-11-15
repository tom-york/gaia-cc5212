def __getattr__(name):
    def _no_disponible(*args, **kwargs):
        raise RuntimeError(f"stub: pyspark.sql.functions.{name} no existe aquí")
    return _no_disponible
