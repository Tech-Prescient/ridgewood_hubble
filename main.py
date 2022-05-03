import sys
import argparse
from hubble.base.db_executor import DbExecutor, MySqlExecutor
from hubble.migrations.data.mysql_data_migration import MySqlDataMigration
from hubble.migrations.tables.mysql_table_migration import MySqlTableMigration

from hubble.exceptions import NoArgsException
from hubble.base.db import MySqlConnection

def get_args(args):
    if len(args) == 0:
        raise NoArgsException('No arguments supplied')
    
    parser = argparse.ArgumentParser()


def main(args):
    print(args)
    dest_conn_str = {
        'host': 'localhost',
        'port': 3306,
        'username': 'root',
        'password': 'password',
        'database': 'migration_test'
    }

    conn_str = {
                "username": "admin",
                "password": "XWq8TxeUxlUW3ipFJAP9",
                "host": "ridgewooddbinstance.ckobuiicknns.us-east-2.rds.amazonaws.com",
                "database": "ridgewood_dawn",
                "port": 3306
            }

    seller_ids = ["BTLUS", "Cube"]
    with MySqlConnection(**conn_str) as source_connection, MySqlConnection(**dest_conn_str) as destination_connection:
        # Migrations for the tables
        table_migration = MySqlTableMigration(destination_connection.connection)
        table_migration.execute()

        # Migrations for the data
        migrations = MySqlDataMigration(source_connection, destination_connection)
        migrations.execute(seller_ids)
        


if __name__ == "__main__":
    main(sys.argv[1:])