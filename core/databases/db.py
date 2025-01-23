from abc import abstractmethod


class Database:

    def __init__(self):
        pass

    @abstractmethod
    def generate_load_queries(self, data_path, tables, ftype):
        pass
