from typing import Dict
from hubble.base.db_executor import MySqlExecutor
from hubble.migrations.config import TableList
from hubble.migrations.tables.base_table_migration import BaseTableMigration
import mysql
import logging


logger = logging.getLogger(__name__)


class MySqlTableMigration(BaseTableMigration):
    def get_advertising_table_query(self):
        return """
        CREATE TABLE IF NOT EXISTS `advertising_data` (
                `id` int NOT NULL AUTO_INCREMENT,
                `adId` varchar(50) NOT NULL,
                `cost` int DEFAULT NULL,
                `adGroupName` text,
                `campaignId` varchar(50) NOT NULL,
                `clicks` int DEFAULT NULL,
                `currency` varchar(50) DEFAULT NULL,
                `asin` varchar(50) DEFAULT NULL,
                `impressions` int DEFAULT NULL,
                `sku` varchar(50) DEFAULT NULL,
                `campaignName` text,
                `adGroupId` varchar(50) NOT NULL,
                `date` datetime DEFAULT NULL,
                `spend` int DEFAULT NULL,
                `seller_id` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
                PRIMARY KEY (`id`)
                ) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
        """

    def get_inventory_summary_table_query(self):
        return """
            CREATE TABLE IF NOT EXISTS `inventory_summary` (
            `id` int NOT NULL AUTO_INCREMENT,
            `Asin` varchar(100) NOT NULL,
            `FnSku` varchar(1000) NOT NULL,
            `SellerSku` varchar(1000) NOT NULL,
            `Condition` varchar(1000) DEFAULT NULL,
            `Details_FulfillableQuantity` int NOT NULL,
            `Details_InboundWorkingQuantity` int NOT NULL,
            `Details_InboundShippedQuantity` int NOT NULL,
            `Details_InboundReceivingQuantity` int NOT NULL,
            `Details_ReservedQuantity_TotalReservedQuantity` int NOT NULL,
            `Details_ReservedQuantity_PendingCustomerOrderQuantity` int NOT NULL,
            `Details_ReservedQuantity_PendingTransshipmentQuantity` int NOT NULL,
            `Details_ReservedQuantity__FcProcessingQuantity` int NOT NULL,
            `Details_ResearchingQuantity_TotalResearchingQuantity` int NOT NULL,
            `Details_ResearchingQuantity_ResearchingQuantityBreakdown` json DEFAULT NULL,
            `Details_UnfulfillableQuantity_TotalUnfulfillableQuantity` int NOT NULL,
            `Details_UnfulfillableQuantity_CustomerDamagedQuantity` int NOT NULL,
            `Details_UnfulfillableQuantity_WarehouseDamagedQuantity` int NOT NULL,
            `Details_UnfulfillableQuantity_DistributorDamagedQuantity` int NOT NULL,
            `Details_UnfulfillableQuantity_CarrierDamagedQuantity` int NOT NULL,
            `Details_UnfulfillableQuantity_DefectiveQuantity` int NOT NULL,
            `Details_UnfulfillableQuantity_ExpiredQuantity` int NOT NULL,
            `Details_FutureSupplyQuantity_ReservedFutureSupplyQuantity` int NOT NULL,
            `Details_FutureSupplyQuantity_FutureSupplyBuyableQuantity` int NOT NULL,
            `LastUpdatedTime` datetime DEFAULT NULL,
            `ProductName` varchar(1000) NOT NULL,
            `TotalQuantity` int NOT NULL,
            `SellerId` varchar(50) NOT NULL,
            `CreatedAt` datetime DEFAULT NULL,
            PRIMARY KEY (`id`)
            ) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
        """

    def get_order_data_table_query(self):
        return """
        CREATE TABLE IF NOT EXISTS `order_data` (
            `AmazonOrderId` varchar(50) NOT NULL,
            `EarliestShipDate` datetime DEFAULT NULL,
            `SalesChannel` varchar(50) DEFAULT NULL,
            `OrderStatus` varchar(50) DEFAULT NULL,
            `NumberOfItemsShipped` int DEFAULT NULL,
            `OrderType` varchar(50) DEFAULT NULL,
            `IsPremiumOrder` tinyint(1) DEFAULT NULL,
            `IsPrime` tinyint(1) DEFAULT NULL,
            `FulfillmentChannel` varchar(50) DEFAULT NULL,
            `NumberOfItemsUnshipped` int DEFAULT NULL,
            `IsReplacementOrder` varchar(50) DEFAULT NULL,
            `IsSoldByAB` tinyint(1) DEFAULT NULL,
            `LatestShipDate` datetime DEFAULT NULL,
            `ShipServiceLevel` varchar(50) DEFAULT NULL,
            `IsISPU` tinyint(1) DEFAULT NULL,
            `MarketplaceId` varchar(50) DEFAULT NULL,
            `PurchaseDate` datetime DEFAULT NULL,
            `ShippingAddress_StateOrRegion` varchar(50) DEFAULT NULL,
            `ShippingAddress_PostalCode` varchar(50) DEFAULT NULL,
            `ShippingAddress_City` varchar(50) DEFAULT NULL,
            `ShippingAddress_CountryCode` varchar(50) DEFAULT NULL,
            `SellerOrderId` varchar(50) DEFAULT NULL,
            `PaymentMethod` varchar(50) DEFAULT NULL,
            `IsBusinessOrder` tinyint(1) DEFAULT NULL,
            `OrderTotal_CurrencyCode` varchar(50) DEFAULT NULL,
            `OrderTotal_Amount` float DEFAULT NULL,
            `PaymentMethodDetails` json DEFAULT NULL,
            `IsGlobalExpressEnabled` tinyint(1) DEFAULT NULL,
            `LastUpdateDate` datetime DEFAULT NULL,
            `ShipmentServiceLevelCategory` varchar(50) DEFAULT NULL,
            `created_time` datetime DEFAULT NULL,
            `seller_id` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
            `modified_time` datetime DEFAULT NULL,
            PRIMARY KEY (`AmazonOrderId`)
            ) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
        """

    def get_order_item_table_query(self):
        return """
        CREATE TABLE IF NOT EXISTS `order_item_data` (
            `OrderItemId` varchar(50) NOT NULL,
            `AmazonOrderId` varchar(50) NOT NULL,
            `TaxCollection_Model` varchar(50) DEFAULT NULL,
            `TaxCollection_ResponsibleParty` varchar(50) DEFAULT NULL,
            `ProductInfo_NumberOfItems` int DEFAULT NULL,
            `ItemTax_CurrencyCode` varchar(50) DEFAULT NULL,
            `ItemTax_Amount` float DEFAULT NULL,
            `QuantityShipped` int DEFAULT NULL,
            `ItemPrice_CurrencyCode` varchar(50) DEFAULT NULL,
            `ItemPrice_Amount` float DEFAULT NULL,
            `ASIN` varchar(50) DEFAULT NULL,
            `SellerSKU` varchar(50) DEFAULT NULL,
            `Title` text,
            `IsGift` varchar(50) DEFAULT NULL,
            `IsTransparency` tinyint(1) DEFAULT NULL,
            `QuantityOrdered` int DEFAULT NULL,
            `PromotionDiscountTax_CurrencyCode` varchar(50) DEFAULT NULL,
            `PromotionDiscountTax_Amount` float DEFAULT NULL,
            `PromotionDiscount_CurrencyCode` varchar(50) DEFAULT NULL,
            `PromotionDiscount_Amount` float DEFAULT NULL,
            `created_time` datetime DEFAULT NULL,
            `modified_time` datetime DEFAULT NULL,
            PRIMARY KEY (`OrderItemId`),
            KEY `ix_order_item_data_AmazonOrderId` (`AmazonOrderId`),
            KEY `order_item_data_SellerSKU_IDX` (`SellerSKU`,`ASIN`) USING BTREE,
            CONSTRAINT `order_item_data_ibfk_1` FOREIGN KEY (`AmazonOrderId`) REFERENCES `order_data` (`AmazonOrderId`)
            ) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
        """

    def get_shipments_items_table_query(self):
        return """
        CREATE TABLE IF NOT EXISTS `shipment_items` (
            `id` int NOT NULL AUTO_INCREMENT,
            `ShipmentId` varchar(100) NOT NULL,
            `SellerSKU` varchar(1000) NOT NULL,
            `FulfillmentNetworkSKU` varchar(1000) NOT NULL,
            `QuantityShipped` int NOT NULL,
            `QuantityReceived` int NOT NULL,
            `QuantityInCase` int NOT NULL,
            `ReleaseDate` datetime DEFAULT NULL,
            `PrepDetailsList` json DEFAULT NULL,
            `CreatedAt` datetime DEFAULT NULL,
            `UpdatedAt` datetime DEFAULT NULL,
            PRIMARY KEY (`id`),
            KEY `ShipmentId` (`ShipmentId`),
            CONSTRAINT `shipment_items_ibfk_1` FOREIGN KEY (`ShipmentId`) REFERENCES `shipments` (`ShipmentId`)
            ) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
        """

    def get_shipments_table_query(self):
        return """
            CREATE TABLE IF NOT EXISTS `shipments` (
            `ShipmentId` varchar(100) NOT NULL,
            `ShipmentName` varchar(1000) DEFAULT NULL,
            `ShipFromAddress_Name` varchar(1000) DEFAULT NULL,
            `ShipFromAddress_AddressLine1` varchar(1000) DEFAULT NULL,
            `ShipFromAddress_AddressLine2` varchar(1000) DEFAULT NULL,
            `ShipFromAddress_DistrictOrCounty` varchar(1000) DEFAULT NULL,
            `ShipFromAddress_City` varchar(1000) NOT NULL,
            `ShipFromAddress_StateOrProvinceCode` varchar(1000) NOT NULL,
            `ShipFromAddress_CountryCode` varchar(1000) NOT NULL,
            `ShipFromAddress_PostalCode` varchar(1000) DEFAULT NULL,
            `DestinationFulfillmentCenterId` varchar(1000) NOT NULL,
            `ShipmentStatus` varchar(1000) NOT NULL,
            `LabelPrepType` varchar(1000) DEFAULT NULL,
            `AreCasesRequired` tinyint(1) DEFAULT NULL,
            `BoxContentsSource` varchar(1000) DEFAULT NULL,
            `ApiFromDate` datetime DEFAULT NULL,
            `ApiToDate` datetime DEFAULT NULL,
            `SellerId` varchar(50) NOT NULL,
            `CreatedAt` datetime DEFAULT NULL,
            `UpdatedAt` datetime DEFAULT NULL,
            `IsSeeded` tinyint(1) NOT NULL DEFAULT '0',
            PRIMARY KEY (`ShipmentId`)
            ) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
        """

    def get_queries(self) -> Dict[TableList, str]:
        return {
            TableList.ADVERTISING_TABLE.value: self.get_advertising_table_query(),
            TableList.INVENTORY_SUMMARY_TABLE.value: self.get_inventory_summary_table_query(),
            TableList.SHIPMENTS_TABLE.value: self.get_shipments_table_query(),
            TableList.SHIPMENT_ITEMS_TABLE.value: self.get_shipments_items_table_query(),
            TableList.ORDER_DATA_TABLE.value: self.get_order_data_table_query(),
            TableList.ORDER_ITEM_TABLE.value: self.get_order_item_table_query(),
        }
