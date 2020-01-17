from blazingsql import BlazingContext
import logging
from clx.io.reader.file_reader import FileReader

log = logging.getLogger(__name__)

class BlazingSQLReader(FileReader):
    def __init__(self, config):
        self._config = config
        self._has_data = True
        self._bc = BlazingContext()

    def fetch_data(self):
        df = None
        table_name = self.config["table_name"]
        file_path = self.config["input_path"]
        sql = self.config["sql"]
        kwargs = self.config.copy()
        if type in kwargs:
            del kwargs["type"]
        del kwargs["table_name"]
        del kwargs["input_path"]

        self._bc.create_table(table_name, file_path, **kwargs)

        #Query
        result = self._bc.sql(sql)

        self.has_data = False

        return result

    def close(self):
       log.info("Closed fs reader")
