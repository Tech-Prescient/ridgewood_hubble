from abc import abstractmethod
import mysql.connector as mysql


class DBConnection:

    def __init__(self, host, port, username, password, database=None):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.connection = None
    
    @abstractmethod
    def make_connection(self):
        pass

    def close(self):
        if self.connection is not None:
            print('Connection closed.')
            self.connection.close()
    
    def __enter__(self):
        self.connection = self.make_connection()
        return self

    def __exit__(self, *args, **kwargs):
        self.close()


class MySqlConnection(DBConnection):

    def make_connection(self):
        return mysql.connect(
            user=self.username,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database
        )

