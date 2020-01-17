from clx.io.factory.abstract_factory import AbstractFactory
from clx.io.reader.blazingsql_reader import BlazingSQLReader


class BlazingSQLFactory(AbstractFactory):
    def __init__(self, config):
        self._config = config

    def get_reader(self):
        return BlazingSQLReader(self.config)

    def get_writer(self):
        raise NotImplementedError
