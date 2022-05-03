from abc import abstractmethod
from hubble.base.db import DBConnection
import logging

logger = logging.getLogger(__name__)


class BaseDataMigration:
    def __init__(
        self, source_connection: DBConnection, destination_connection: DBConnection
    ):
        self.source_db = source_connection
        self.destination_db = destination_connection
        self.source_connection = source_connection.connection
        self.destination_connection = destination_connection.connection
    
    def get_destination_connection_str(self, dialect, driver):
        return f"{dialect}+{driver}://{self.destination_db.username}:{self.destination_db.password}@{self.destination_db.host}:{self.destination_db.port}/{self.destination_db.database}"

    def get_source_connection_str(self, dialect, driver):
        return f"{dialect}+{driver}://{self.source_db.username}:{self.source_db.password}@{self.source_db.host}:{self.source_db.port}/{self.source_db.database}"

    @abstractmethod
    def execute(self):
        pass

    def __del__(self):
        self.source_connection.close()
        self.destination_connection.close()

