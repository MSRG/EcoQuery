


class Benchmark():
    def createSchema(self, db):
        raise NotImplementedError()

    def loadData(self, db, data_path):
        raise NotImplementedError()

    def run(self, db):
        raise NotImplementedError()