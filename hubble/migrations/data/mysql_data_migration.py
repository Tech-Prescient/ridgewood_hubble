import logging
from hubble.migrations.config import TableList
from hubble.migrations.data.data_migration import BaseDataMigration
from sqlalchemy import create_engine

from hubble.migrations.data.utils import (
    DataIterator,
    DestinationDataSource,
    SourceDataSource,
    cache_engine,
)


logger = logging.getLogger(__name__)


class MySqlDataMigration(BaseDataMigration):
    @cache_engine
    def get_source_engine(self):
        return create_engine(self.get_source_connection_str("mysql", "pymysql"))

    @cache_engine
    def get_destination_engine(self):
        return create_engine(self.get_destination_connection_str("mysql", "pymysql"))

    def migrate_data(self, table, seller_ids, checkpoint_col_name, seller_col_name):

        checkpoint_result = DestinationDataSource.get_last_updated_by_sellers(
            self.destination_connection,
            table,
            seller_ids,
            checkpoint_col_name,
            seller_col_name,
        )

        queries = SourceDataSource.preprare_migration_query(
            table, seller_col_name, seller_ids, checkpoint_result, checkpoint_col_name
        )
        source_engine = self.get_source_engine()
        destination_engine = self.get_destination_engine()

        for query in queries:
            batches = 0
            for batch in DataIterator.batch_process_data(source_engine, query):
                batch.to_sql(
                    table, con=destination_engine, if_exists="append", index=False
                )
                batches += 1
                print("Batches Done", batches)

            print("Seller data migrated for", query)

    def migrate_shipper_data(self, seller_ids):
        self.migrate_data(
            table=TableList.SHIPMENTS_TABLE.value,
            seller_ids=seller_ids,
            checkpoint_col_name="CreatedAt",
            seller_col_name="sellerid",
        )

    def migrate_order_data(self, seller_ids):
        self.migrate_data(
            table=TableList.ORDER_DATA_TABLE.value,
            seller_ids=seller_ids,
            checkpoint_col_name="created_time",
            seller_col_name="seller_id",
        )

    def migrate_advertising_data(self, seller_ids):
        self.migrate_data(
            table=TableList.ADVERTISING_TABLE.value,
            seller_ids=seller_ids,
            checkpoint_col_name="date",
            seller_col_name="seller_id",
        )

    def migrate_inventory_summary_data(self, seller_ids):
        self.migrate_data(
            table=TableList.INVENTORY_SUMMARY_TABLE.value,
            seller_ids=seller_ids,
            checkpoint_col_name="CreatedAt",
            seller_col_name="sellerid",
        )

    def migrate_data_by_table_joins(
        self, ref_table, table, seller_ids, seller_column, checkpoint_column, left_join_on, right_join_on
    ):
        checkpoint_data = (
            DestinationDataSource.get_last_updated_checkpoint_by_table_join(
                self.destination_connection,
                ref_table,
                table,
                seller_ids,
                checkpoint_column,
                seller_column,
                left_join_on,
                right_join_on
            )
        )

        queries = SourceDataSource.preprare_migration_query_by_table_join(
            seller_ref_table=ref_table,
            table=table,
            seller_col_name=seller_column,
            seller_ids=seller_ids,
            destination_checkpoint=checkpoint_data,
            date_column=checkpoint_column,
            left_join_on=left_join_on,
            right_join_on=right_join_on
        )

        source_engine = self.get_source_engine()
        destination_engine = self.get_destination_engine()

        for query in queries:
            batches = 0
            for batch in DataIterator.batch_process_data(source_engine, query, 10000):
                batch.dropna(inplace=True)
                batch.to_sql(
                    table, con=destination_engine, if_exists="append", index=False
                )
                batches += 1
                print("Batches Done", batches)

            print("Seller data migrated for", query)

    def migrate_shipment_items_data(self, seller_ids):
        self.migrate_data_by_table_joins(
            ref_table=TableList.SHIPMENTS_TABLE.value,
            table=TableList.SHIPMENT_ITEMS_TABLE.value,
            seller_ids=seller_ids,
            seller_column="sellerid",
            checkpoint_column="CreatedAt",
            left_join_on='ShipmentId',
            right_join_on='ShipmentId'
        )

    def migrate_order_items_data(self, seller_ids):
        self.migrate_data_by_table_joins(
            ref_table=TableList.ORDER_DATA_TABLE.value,
            table=TableList.ORDER_ITEM_TABLE.value,
            seller_ids=seller_ids,
            seller_column="seller_id",
            checkpoint_column="created_time",
            left_join_on='AmazonOrderId',
            right_join_on='AmazonOrderId'
        )

    def execute(self, seller_ids):
        migrations = {
            TableList.ADVERTISING_TABLE.value: self.migrate_advertising_data,
            TableList.INVENTORY_SUMMARY_TABLE.value: self.migrate_inventory_summary_data,
            TableList.SHIPMENTS_TABLE.value: self.migrate_shipper_data,
            TableList.SHIPMENT_ITEMS_TABLE.value: self.migrate_shipment_items_data,
            TableList.ORDER_DATA_TABLE.value: self.migrate_order_data,
            TableList.ORDER_ITEM_TABLE.value: self.migrate_order_items_data
        }

        sucessful_migration = 0
        failed_migration = 0
        for table, migration_function in migrations.items():
            logger.info(f'Migrations started for: {table}')
            try:
                migration_function(seller_ids)
                sucessful_migration += 1
            except Exception as exp:
                logger.error(f'Migration got failed for: {table}', exc_info=True)
                failed_migration += 1
            
        logger.info(f'Migration data summary - Successful: {sucessful_migration}  Failed: {failed_migration}')
