"""Data quality checks module"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Dict, List, Tuple
from src.utils.logger import logger
from datetime import datetime

class DataQualityChecker:
    """Perform data quality checks"""
    
    def __init__(self):
        self.checks_passed = []
        self.checks_failed = []
    
    def run_all_checks(self, df: DataFrame) -> Tuple[bool, Dict]:
        """Run all data quality checks"""
        logger.info("Starting data quality checks")
        
        # Reset results
        self.checks_passed = []
        self.checks_failed = []
        
        # Run individual checks
        self._check_row_count(df)
        self._check_duplicates(df)
        self._check_null_values(df)
        self._check_data_ranges(df)
        self._check_date_consistency(df)
        self._check_business_rules(df)
        
        # Prepare results
        total_checks = len(self.checks_passed) + len(self.checks_failed)
        pass_rate = len(self.checks_passed) / total_checks * 100 if total_checks > 0 else 0
        
        results = {
            "total_checks": total_checks,
            "passed": len(self.checks_passed),
            "failed": len(self.checks_failed),
            "pass_rate": pass_rate,
            "passed_checks": self.checks_passed,
            "failed_checks": self.checks_failed,
            "timestamp": datetime.now().isoformat()
        }
        
        # Log summary
        if self.checks_failed:
            logger.warning(f"Data quality: {len(self.checks_failed)} checks failed")
            for check in self.checks_failed:
                logger.warning(f"  ❌ {check['name']}: {check['message']}")
        else:
            logger.success("All data quality checks passed!")
        
        return len(self.checks_failed) == 0, results
    
    def _check_row_count(self, df: DataFrame, min_rows: int = 1000):
        """Check if dataset has minimum required rows"""
        row_count = df.count()
        
        if row_count >= min_rows:
            self.checks_passed.append({
                "name": "Row Count Check",
                "status": "passed",
                "details": f"Found {row_count:,} rows (minimum: {min_rows:,})"
            })
        else:
            self.checks_failed.append({
                "name": "Row Count Check",
                "status": "failed",
                "message": f"Only {row_count:,} rows found (minimum: {min_rows:,})"
            })
    
    def _check_duplicates(self, df: DataFrame):
        """Check for duplicate records"""
        total_count = df.count()
        
        # Check which columns exist
        dup_cols = ["invoice_no", "product_id"]
        if "invoice_datetime" in df.columns:
            dup_cols.append("invoice_datetime")
        elif "invoice_date" in df.columns:
            dup_cols.append("invoice_date")
        
        unique_count = df.dropDuplicates(dup_cols).count()
        duplicate_count = total_count - unique_count
        
        if duplicate_count == 0:
            self.checks_passed.append({
                "name": "Duplicate Check",
                "status": "passed",
                "details": "No duplicates found"
            })
        else:
            dup_pct = duplicate_count / total_count * 100
            if dup_pct < 5:  # Allow up to 5% duplicates
                self.checks_passed.append({
                    "name": "Duplicate Check",
                    "status": "passed",
                    "details": f"Found {duplicate_count:,} duplicates ({dup_pct:.1f}%) - within tolerance"
                })
            else:
                self.checks_failed.append({
                    "name": "Duplicate Check",
                    "status": "failed",
                    "message": f"Found {duplicate_count:,} duplicates ({dup_pct:.1f}%) - exceeds 5% threshold"
                })
    
    def _check_null_values(self, df: DataFrame):
        """Check for null values in critical columns"""
        critical_columns = ["invoice_no", "product_id", "quantity", "unit_price", "invoice_datetime"]
        
        for col in critical_columns:
            if col in df.columns:
                null_count = df.filter(F.col(col).isNull()).count()
                
                if null_count == 0:
                    self.checks_passed.append({
                        "name": f"Null Check: {col}",
                        "status": "passed",
                        "details": f"No nulls in {col}"
                    })
                else:
                    null_pct = null_count / df.count() * 100
                    self.checks_failed.append({
                        "name": f"Null Check: {col}",
                        "status": "failed",
                        "message": f"{null_count:,} nulls in {col} ({null_pct:.1f}%)"
                    })
    
    def _check_data_ranges(self, df: DataFrame):
        """Check if numeric values are within expected ranges"""
        # Quantity should be positive (we already filtered negatives)
        zero_qty = df.filter(F.col("quantity") == 0).count()
        if zero_qty == 0:
            self.checks_passed.append({
                "name": "Quantity Check",
                "status": "passed",
                "details": "All quantities are positive"
            })
        else:
            self.checks_failed.append({
                "name": "Quantity Check",
                "status": "failed",
                "message": f"Found {zero_qty:,} records with zero quantity"
            })
        
        # Unit price should be positive
        negative_price = df.filter(F.col("unit_price") <= 0).count()
        if negative_price == 0:
            self.checks_passed.append({
                "name": "Price Check",
                "status": "passed",
                "details": "All unit prices are positive"
            })
        else:
            self.checks_failed.append({
                "name": "Price Check",
                "status": "failed",
                "message": f"Found {negative_price:,} records with invalid unit price"
            })
        
        # Check for extreme values
        extreme_prices = df.filter(F.col("unit_price") > 10000).count()
        if extreme_prices < 10:  # Allow a few extreme values
            self.checks_passed.append({
                "name": "Extreme Price Check",
                "status": "passed",
                "details": f"Found {extreme_prices} extreme prices (acceptable)"
            })
        else:
            self.checks_failed.append({
                "name": "Extreme Price Check",
                "status": "failed",
                "message": f"Found {extreme_prices:,} products with price > 10,000"
            })
    
    def _check_date_consistency(self, df: DataFrame):
        """Check date consistency"""
        # Get date range
        date_stats = df.select(
            F.min("invoice_date").alias("min_date"),
            F.max("invoice_date").alias("max_date")
        ).collect()[0]
        
        min_date = date_stats["min_date"]
        max_date = date_stats["max_date"]
        
        # Check if dates are reasonable (2010-2011 for this dataset)
        if min_date.year >= 2010 and max_date.year <= 2012:
            self.checks_passed.append({
                "name": "Date Range Check",
                "status": "passed",
                "details": f"Date range: {min_date} to {max_date}"
            })
        else:
            self.checks_failed.append({
                "name": "Date Range Check",
                "status": "failed",
                "message": f"Unexpected date range: {min_date} to {max_date}"
            })
        
        # Check for future dates
        future_dates = df.filter(F.col("invoice_date") > F.current_date()).count()
        if future_dates == 0:
            self.checks_passed.append({
                "name": "Future Date Check",
                "status": "passed",
                "details": "No future dates found"
            })
        else:
            self.checks_failed.append({
                "name": "Future Date Check",
                "status": "failed",
                "message": f"Found {future_dates:,} records with future dates"
            })
    
    def _check_business_rules(self, df: DataFrame):
        """Check business-specific rules"""
        # Check if total_price = quantity * unit_price
        price_mismatch = df.filter(
            F.abs(F.col("total_price") - (F.col("quantity") * F.col("unit_price"))) > 0.01
        ).count()
        
        if price_mismatch == 0:
            self.checks_passed.append({
                "name": "Price Calculation Check",
                "status": "passed",
                "details": "All price calculations are correct"
            })
        else:
            self.checks_failed.append({
                "name": "Price Calculation Check",
                "status": "failed",
                "message": f"Found {price_mismatch:,} records with incorrect price calculation"
            })
        
        # Check customer distribution
        customer_stats = df.agg(
            F.countDistinct("customer_id").alias("unique_customers"),
            F.count("*").alias("total_transactions")
        ).collect()[0]
        
        avg_trans_per_customer = customer_stats["total_transactions"] / customer_stats["unique_customers"]
        
        if avg_trans_per_customer > 1:
            self.checks_passed.append({
                "name": "Customer Distribution Check",
                "status": "passed",
                "details": f"Average {avg_trans_per_customer:.1f} transactions per customer"
            })
        else:
            self.checks_failed.append({
                "name": "Customer Distribution Check",
                "status": "failed",
                "message": "Low repeat purchase rate"
            })
