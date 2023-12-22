from os import getcwd
from os.path import join
from lib.utils.logger import Logger


class SqlFileReader:
    def __init__(self) -> None:
        self.sql_dir = join(
            getcwd(), "glue_user", "workspace", "src", "lib", "sql", "query"
        )
        self.logger = Logger()

    def _read_sql_file(self, file_name: str) -> str:
        file_path = join(self.sql_dir, file_name)
        self.logger.info(message=f"SQL FILE PATH: {file_path}")
        with open(file_path, mode="r") as file:
            return file.read()

    def get_sql_query(self, file_name: str, substitutions: [str, str] = None) -> str:
        self.logger.start(message="RETRIEVING QUERY")
        sql_query = self._read_sql_file(file_name=file_name)
        if substitutions is not None:
            sql_query = sql_query.format(**substitutions)
        self.logger.raw_info(message=f"SQL QUERY: {sql_query}")
        self.logger.finish(message="RETRIEVING QUERY")
        return sql_query
