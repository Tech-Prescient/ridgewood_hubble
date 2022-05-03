from abc import abstractmethod
from typing import Dict
from hubble.base.db_executor import MySqlExecutor
from hubble.exceptions import MySqlTableNotCreatedError
from hubble.migrations.config import TableList
import logging
import mysql


logger = logging.getLogger(__name__)


class BaseTableMigration:
    def __init__(self, connection):
        self.connection = connection

    @abstractmethod
    def get_queries(self) -> Dict[TableList, str]:
        pass

    def execute(self):
        queries = self.get_queries()
        with MySqlExecutor(self.connection) as executor:
            for table_name, query in queries.items():
                _ = executor.execute(query)
                try:
                    verify_query = f"DESCRIBE `{table_name}`"
                    cur = executor.execute(verify_query)
                    logger.info(f"Query Execution Status: {cur.fetchall()}")
                except mysql.connector.errors.ProgrammingError as exp:
                    logger.info(f"Table not created properly: {table_name}")
                    raise MySqlTableNotCreatedError(exp)

                logger.info(f"Table created successfully {table_name}")
