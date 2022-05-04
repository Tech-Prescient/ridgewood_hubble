import sys
import argparse
from hubble.migrations.data.mysql_data_migration import MySqlDataMigration
from hubble.migrations.tables.mysql_table_migration import MySqlTableMigration

from hubble.exceptions import NoArgsException
from hubble.base.db import MySqlConnection
import os
import dotenv


dotenv.load_dotenv()


def get_args(args):
    if len(args) == 0:
        raise NoArgsException("No arguments supplied")

    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--seller_ids")
    return parser.parse_args(args)


def main(args):
    args = get_args(args)

    dest_conn_str = {
        "host": "localhost",
        "port": 3306,
        "username": "root",
        "password": "password",
        "database": "migration_test",
    }

    source_connection_data = {
        "username": os.getenv("DB_USERNAME"),
        "password": os.getenv("DB_PASSWORD"),
        "host": os.getenv("DB_HOST"),
        "database": os.getenv("DB_NAME"),
        "port": int(os.getenv("DB_PORT")),
    }

    seller_ids = args.seller_ids.split(',')
    with MySqlConnection(
        **source_connection_data
    ) as source_connection, MySqlConnection(**dest_conn_str) as destination_connection:
        # Migrations for the tables
        table_migration = MySqlTableMigration(destination_connection.connection)
        table_migration.execute()

        # Migrations for the data
        migrations = MySqlDataMigration(source_connection, destination_connection)
        migrations.execute(seller_ids)


if __name__ == "__main__":
    main(sys.argv[1:])
