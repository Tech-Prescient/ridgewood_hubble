import pandas as pd

from hubble.base.db_executor import MySqlExecutor


class DataIterator:
    @staticmethod
    def batch_process_data(connection, query, chunksize=50000):
        for chunk_df in pd.read_sql_query(query, connection, chunksize=chunksize):
            yield chunk_df


class SourceDataSource:
    @staticmethod
    def preprare_migration_query(
        table,
        seller_col_name,
        seller_ids,
        destination_checkpoint,
        date_column="CreatedAt",
    ):
        if len(destination_checkpoint) == 0:
            # Fresh migration
            seller_ids = ",".join([f"'{seller}'" for seller in seller_ids])
            return [
                f"""
                    SELECT * FROM {table} WHERE {seller_col_name} in ({seller_ids})
                """
            ]
        queries = []

        for seller in seller_ids:
            seller_checkpoint = list(
                filter(lambda x: x[seller_col_name] == seller, destination_checkpoint)
            )
            if len(seller_checkpoint) == 0:
                # if no seller data is not present in the destiantion
                # this will migrate all data for new seller
                queries.append(
                    f"""
                    SELECT * FROM {table} WHERE {seller_col_name} = '{seller}'
                    """
                )
                continue

            last_checkpoint = seller_checkpoint[0]
            query = f"""
                SELECT * 
                FROM {table}
                WHERE {seller_col_name} = '{seller}' AND {date_column} > '{last_checkpoint['last_updated']}'
            """
            queries.append(query)

        return queries

    @staticmethod
    def preprare_migration_query_by_table_join(
        seller_ref_table,
        table,
        seller_col_name,
        seller_ids,
        destination_checkpoint,
        date_column="CreatedAt",
        left_join_on="AmazonOrderId",
        right_join_on="AmazonOrderId"
    ):
        if len(destination_checkpoint) == 0:
            # Fresh migration
            seller_ids_str = ",".join([f"'{seller}'" for seller in seller_ids])
            return [
                f"""
                    SELECT oid.* 
                    FROM
                            {seller_ref_table} od 
                        LEFT JOIN
                            {table} oid 
                        ON od.{left_join_on} = oid.{right_join_on} 
                    WHERE od.{seller_col_name} in ({seller_ids_str})
                """
            ]
        queries = []

        for seller in seller_ids:
            seller_checkpoint = list(
                filter(lambda x: x[seller_col_name] == seller, destination_checkpoint)
            )
            if len(seller_checkpoint) == 0:
                # if no seller data is not present in the destiantion
                # this will migrate all data for new seller
                queries.append(
                    f"""
                    SELECT oid.* 
                    FROM
                            {seller_ref_table} od 
                        LEFT JOIN
                            {table} oid 
                        ON od.{left_join_on} = oid.{right_join_on} 
                    WHERE od.{seller_col_name} = '{seller}'
                """
                )
                continue

            last_checkpoint = seller_checkpoint[0]
            if last_checkpoint["last_updated"] is None:
                # if last updated not given
                queries.append(
                    f"""
                    SELECT oid.* 
                    FROM
                            {seller_ref_table} od 
                        LEFT JOIN
                            {table} oid 
                        ON od.{left_join_on} = oid.{right_join_on} 
                    WHERE od.{seller_col_name} = '{seller}'
                """
                )
                continue

            query = f"""
                SELECT oid.* 
                FROM
                    {seller_ref_table} od 
                    LEFT JOIN
                            {table} oid 
                    ON od.{left_join_on} = oid.{right_join_on} 
                WHERE od.{seller_col_name} = '{seller}' AND oid.{date_column} > '{last_checkpoint['last_updated']}'
            """
            queries.append(query)

        return queries


class DestinationDataSource:
    @staticmethod
    def get_last_updated_by_sellers(
        connection, table, seller_ids, col_name, seller_id_col="sellerid"
    ):
        seller_ids = ",".join([f"'{seller}'" for seller in seller_ids])
        query = f"""
            SELECT {seller_id_col}, MAX({col_name}) as last_updated, COUNT(*) as seller_rows
            FROM {table}
            GROUP BY {seller_id_col} 
            HAVING {seller_id_col} in ({seller_ids})
        """
        with MySqlExecutor(connection) as executor:
            cursor = executor.execute(query)
            return cursor.fetchall()

    @staticmethod
    def get_last_updated_checkpoint_by_table_join(
        connection,
        seller_table_ref,
        table,
        seller_ids,
        checkpoint_col,
        seller_id_col,
        left_join_on="AmazonOrderId",
        right_join_on="AmazonOrderId",
    ):
        seller_ids = ",".join([f"'{seller}'" for seller in seller_ids])
        query = f"""
            SELECT 
                od.{seller_id_col}, 
                MAX(oid.{checkpoint_col}) as last_updated, 
                COUNT(*) as seller_rows 
            FROM
                    {seller_table_ref} od 
                LEFT JOIN
                    {table} oid 
                ON
                    od.{left_join_on} = oid.{right_join_on}
            GROUP BY od.{seller_id_col} 
            HAVING od.{seller_id_col} in ({seller_ids})
        """
        print(175, query)
        with MySqlExecutor(connection) as executor:
            cursor = executor.execute(query)
            return cursor.fetchall()


def cache_engine(func):
    __instance = {}

    def wrapper(*args, **kwargs):
        if "engine" not in __instance:
            __instance["engine"] = func(*args, **kwargs)
        return __instance["engine"]

    return wrapper
