
from abc import abstractmethod
from typing import Dict


class DbExecutor:
    def __init__(self, connection, config: Dict=None):
        self.connection = connection
        self.config = config
        self.cursor = None
    
    def __enter__(self):
        default_config = {'buffered': True}
        if self.config is None:
            self.config = default_config
        else:
            self.config.update(default_config)
        self.cursor =  self.connection.cursor(**self.config)
        return self

    @abstractmethod
    def execute(self, query):
        pass
    
    def __exit__(self, *args, **kwargs):
        try:
            if self.cursor is not None:
                self.cursor.close()
                print('Cursor closed.')
        except Exception as exp:
            print('Exception', exp)
        finally:
            del self.cursor


class MySqlExecutor(DbExecutor):

    def __init__(self, connection, config: Dict = None):
        config = {'dictionary': True}
        super().__init__(connection, config)

    def execute(self, query):
        self.cursor.execute(query)
        return self.cursor
    



