from enum import Enum


class TableList(Enum):

    ADVERTISING_TABLE = "advertising_data"
    SHIPMENTS_TABLE = "shipments"
    INVENTORY_SUMMARY_TABLE = "inventory_summary"
    ORDER_DATA_TABLE = "order_data"
    ORDER_ITEM_TABLE = "order_item_data"
    SHIPMENT_ITEMS_TABLE = "shipment_items"
