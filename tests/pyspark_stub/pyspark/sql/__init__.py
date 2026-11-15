class SparkSession:
    """Builder encadenable que nunca se usa en las pruebas."""

    @staticmethod
    def builder():
        return SparkSession()

    def appName(self, *args, **kwargs):
        return self

    def config(self, *args, **kwargs):
        return self

    def getOrCreate(self):
        raise RuntimeError("stub: no hay Spark real en esta prueba")
