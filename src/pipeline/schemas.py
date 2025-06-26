"""Data schemas for e-commerce dataset"""

from pyspark.sql.types import *

class EcommerceSchema:
    """Schema definitions for e-commerce data"""
    
    @staticmethod
    def sales_schema():
        """Schema for e-commerce transaction data"""
        return StructType([
            StructField("InvoiceNo", StringType(), True),
            StructField("StockCode", StringType(), True),
            StructField("Description", StringType(), True),
            StructField("Quantity", IntegerType(), True),
            StructField("InvoiceDate", StringType(), True),
            StructField("UnitPrice", DoubleType(), True),
            StructField("CustomerID", StringType(), True),
            StructField("Country", StringType(), True)
        ])
    
    @staticmethod
    def standardized_columns():
        """Standardized column names"""
        return {
            "InvoiceNo": "invoice_no",
            "StockCode": "product_id",
            "Description": "product_description",
            "Quantity": "quantity",
            "InvoiceDate": "invoice_date",
            "UnitPrice": "unit_price",
            "CustomerID": "customer_id",
            "Country": "country"
        }
