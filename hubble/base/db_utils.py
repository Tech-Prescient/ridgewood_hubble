

class DbDdlUtils:
    @staticmethod
    def create_table(table_name, columns_dict):
        cols = ", ".join([f"{key} {value}" for key, value in columns_dict.items()])
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({cols})"
        return query

    @staticmethod
    def drop_table(table_name):
        return f"DROP TABLE {table_name}"


class DbQueryUtils:

    @staticmethod
    def get_total_rows(executor, table, where=None):
        query = f"""
            SELECT count(*) as total_rows
            FROM {table} 
        """
        if where is not None:
            query += f"WHERE {where}"
        cursor = executor.execute(query)
        return cursor.fetchone()

    @staticmethod
    def get_total_rows_by_group(executor, table, columns, filter_data=None):
        query = f"""
            SELECT {columns}, count(*) as total_rows
            FROM {table} 
            GROUP BY {columns}
        """
        if filter_data:
            query += f'HAVING {filter_data}'
        print(query)
        cursor = executor.execute(query)
        return cursor.fetchall()





