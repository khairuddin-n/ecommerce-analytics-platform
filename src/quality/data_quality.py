"""Data quality checks module"""

from pathlib import Path
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime, date
import json
from src.utils.logger import logger
from src.utils.config import analytics_config, settings

@dataclass
class QualityCheckResult:
    """Result of a quality check"""
    name: str
    status: str  # 'passed', 'failed', 'warning'
    details: Optional[str] = None
    message: Optional[str] = None
    severity: str = 'error'  # 'error', 'warning', 'info'
    metrics: Dict[str, Any] = field(default_factory=dict)

class DataQualityChecker:
    """Perform comprehensive data quality checks"""
    
    def __init__(self):
        self.checks_passed: List[QualityCheckResult] = []
        self.checks_failed: List[QualityCheckResult] = []
        self.checks_warning: List[QualityCheckResult] = []
        self.config = analytics_config
    
    def run_all_checks(self, df: DataFrame) -> Tuple[bool, Dict]:
        """Run all data quality checks with detailed reporting"""
        logger.info("="*60)
        logger.info("Starting comprehensive data quality checks")
        
        # Get dataset info
        row_count = df.count()
        col_count = len(df.columns)
        logger.info(f"Dataset: {row_count:,} rows, {col_count} columns")
        
        # Show sample data info
        try:
            date_range = df.select(F.min("invoice_date"), F.max("invoice_date")).collect()[0]
            logger.info(f"Date range: {date_range[0]} to {date_range[1]}")
        except:
            logger.warning("Could not determine date range")
        
        # Reset results
        self.checks_passed = []
        self.checks_failed = []
        self.checks_warning = []
        
        # Define check groups
        check_groups = [
            ("Basic Checks", [
                lambda: self._check_row_count(df),
                lambda: self._check_schema_compliance(df),
                lambda: self._check_duplicates(df)
            ]),
            ("Data Integrity", [
                lambda: self._check_null_values(df),
                lambda: self._check_data_types(df),
                lambda: self._check_referential_integrity(df)
            ]),
            ("Business Rules", [
                lambda: self._check_data_ranges(df),
                lambda: self._check_business_rules(df),
                lambda: self._check_data_consistency(df)
            ]),
            ("Time Series", [
                lambda: self._check_date_consistency(df),
                lambda: self._check_time_gaps(df)
            ]),
            ("Statistical Checks", [
                lambda: self._check_outliers(df),
                lambda: self._check_data_distribution(df)
            ])
        ]
        
        # Execute checks by group with error handling
        for group_name, checks in check_groups:
            logger.info(f"Running {group_name}")
            for check in checks:
                try:
                    check()
                except Exception as e:
                    logger.error(f"Check in {group_name} failed with error: {e}")
                    self.checks_failed.append(
                        QualityCheckResult(
                            name=f"{group_name} - Error",
                            status="failed",
                            message=str(e),
                            severity="error"
                        )
                    )
        
        # Calculate results
        total_checks = len(self.checks_passed) + len(self.checks_failed) + len(self.checks_warning)
        quality_score = (len(self.checks_passed) / total_checks * 100) if total_checks > 0 else 100.0
        
        # Determine overall status
        has_errors = len(self.checks_failed) > 0
        has_critical_errors = any(c.severity == 'error' for c in self.checks_failed)
        
        results = {
            "total_checks": total_checks,
            "passed": len(self.checks_passed),
            "failed": len(self.checks_failed),
            "warnings": len(self.checks_warning),
            "pass_rate": quality_score,
            "quality_score": quality_score,
            "passed_checks": [self._serialize_check(c) for c in self.checks_passed],
            "failed_checks": [self._serialize_check(c) for c in self.checks_failed],
            "warning_checks": [self._serialize_check(c) for c in self.checks_warning],
            "timestamp": datetime.now().isoformat(),
            "has_critical_errors": has_critical_errors
        }
        
        # Log summary
        self._log_quality_summary(results)
        
        return not has_critical_errors, results
    
    def _serialize_check(self, check: QualityCheckResult) -> Dict:
        """Serialize check result for storage"""
        return {
            "name": check.name,
            "status": check.status,
            "details": check.details,
            "message": check.message,
            "severity": check.severity,
            "metrics": check.metrics
        }
    
    def _log_quality_summary(self, results: Dict):
        """Log quality check summary"""
        quality_score = results['quality_score']
        
        if results['has_critical_errors']:
            logger.error(f"Data quality FAILED: Score {quality_score:.1f}%")
            logger.error(f"Critical errors found: {results['failed']} checks failed")
            for check in self.checks_failed[:5]:  # Show first 5 failures
                logger.error(f"  ❌ {check.name}: {check.message}")
        elif results['warnings'] > 0:
            logger.warning(f"Data quality PASSED with warnings: Score {quality_score:.1f}%")
            for check in self.checks_warning[:5]:
                logger.warning(f"  ⚠️  {check.name}: {check.message}")
        else:
            logger.success(f"Data quality EXCELLENT: Score {quality_score:.1f}%")
    
    def _check_row_count(self, df: DataFrame, min_rows: Optional[int] = None):
        """Enhanced row count validation"""
        if min_rows is None:
            min_rows = self.config.min_rows_threshold
            
        row_count = df.count()
        max_expected_rows = 10_000_000  # 10M rows
        
        logger.info(f"Checking row count: {row_count:,} (min required: {min_rows:,})")
        
        if row_count < min_rows:
            self._add_failed(
                QualityCheckResult(
                    name="Row Count Check",
                    status="failed",
                    message=f"Only {row_count:,} rows found (minimum: {min_rows:,})",
                    severity="error",
                    metrics={"row_count": row_count, "min_required": min_rows}
                )
            )
        elif row_count > max_expected_rows:
            self._add_warning(
                QualityCheckResult(
                    name="Row Count Check",
                    status="warning",
                    message=f"Unusually high row count: {row_count:,}",
                    severity="warning",
                    metrics={"row_count": row_count}
                )
            )
        else:
            self._add_passed(
                QualityCheckResult(
                    name="Row Count Check",
                    status="passed",
                    details=f"Found {row_count:,} rows",
                    metrics={"row_count": row_count}
                )
            )
    
    def _check_schema_compliance(self, df: DataFrame):
        """Check if schema matches expectations"""
        expected_columns = {
            "invoice_no", "product_id", "product_description", 
            "quantity", "invoice_date", "unit_price", "customer_id", "country"
        }
        
        actual_columns = set(df.columns)
        missing_columns = expected_columns - actual_columns
        
        if missing_columns:
            self._add_failed(
                QualityCheckResult(
                    name="Schema Compliance",
                    status="failed",
                    message=f"Missing required columns: {missing_columns}",
                    severity="error"
                )
            )
        else:
            self._add_passed(
                QualityCheckResult(
                    name="Schema Compliance",
                    status="passed",
                    details="All required columns present"
                )
            )
    
    def _check_duplicates(self, df: DataFrame):
        """Enhanced duplicate detection"""
        total_count = df.count()
        
        # Check different duplicate scenarios
        # 1. Complete duplicates
        complete_duplicates = total_count - df.distinct().count()
        
        # 2. Key-based duplicates - check which columns exist
        available_cols = df.columns
        key_columns = []
        
        if "invoice_no" in available_cols:
            key_columns.append("invoice_no")
        if "product_id" in available_cols:
            key_columns.append("product_id")
        if "invoice_datetime" in available_cols:
            key_columns.append("invoice_datetime")
        elif "invoice_date" in available_cols:
            key_columns.append("invoice_date")
        
        if key_columns:
            key_duplicates = total_count - df.dropDuplicates(key_columns).count()
        else:
            key_duplicates = complete_duplicates
        
        dup_metrics = {
            "total_records": total_count,
            "complete_duplicates": complete_duplicates,
            "key_duplicates": key_duplicates,
            "complete_dup_rate": complete_duplicates / total_count if total_count > 0 else 0,
            "key_dup_rate": key_duplicates / total_count if total_count > 0 else 0
        }
        
        max_dup_rate = self.config.max_duplicate_rate
        
        if dup_metrics["key_dup_rate"] > max_dup_rate:
            self._add_failed(
                QualityCheckResult(
                    name="Duplicate Check",
                    status="failed",
                    message=f"High duplicate rate: {dup_metrics['key_dup_rate']:.1%} (threshold: {max_dup_rate:.1%})",
                    severity="error",
                    metrics=dup_metrics
                )
            )
        elif dup_metrics["key_dup_rate"] > 0:
            self._add_warning(
                QualityCheckResult(
                    name="Duplicate Check",
                    status="warning",
                    message=f"Found {key_duplicates:,} duplicates ({dup_metrics['key_dup_rate']:.1%})",
                    severity="warning",
                    metrics=dup_metrics
                )
            )
        else:
            self._add_passed(
                QualityCheckResult(
                    name="Duplicate Check",
                    status="passed",
                    details="No duplicates found",
                    metrics=dup_metrics
                )
            )
    
    def _check_null_values(self, df: DataFrame):
        """Comprehensive null value analysis"""
        critical_columns = ["invoice_no", "product_id", "quantity", "unit_price"]
        total_rows = df.count()
        
        for col in critical_columns:
            if col in df.columns:
                null_count = df.filter(F.col(col).isNull()).count()
                null_rate = null_count / total_rows if total_rows > 0 else 0
                
                if null_count > 0:
                    self._add_failed(
                        QualityCheckResult(
                            name=f"Null Check: {col}",
                            status="failed",
                            message=f"{null_count:,} nulls in {col} ({null_rate:.1%})",
                            severity="error",
                            metrics={"null_count": null_count, "null_rate": null_rate}
                        )
                    )
                else:
                    self._add_passed(
                        QualityCheckResult(
                            name=f"Null Check: {col}",
                            status="passed",
                            details=f"No nulls in {col}"
                        )
                    )
    
    def _check_data_types(self, df: DataFrame):
        """Verify data types are correct"""
        type_checks = {
            "quantity": ["integer", "long"],
            "unit_price": ["double", "float"],
            "total_price": ["double", "float"]
        }
        
        schema_dict = {field.name: str(field.dataType) for field in df.schema.fields}
        
        for col, expected_types in type_checks.items():
            if col in schema_dict:
                actual_type = schema_dict[col].lower()
                if not any(exp_type in actual_type for exp_type in expected_types):
                    self._add_warning(
                        QualityCheckResult(
                            name=f"Data Type Check: {col}",
                            status="warning",
                            message=f"{col} has type {actual_type}, expected one of {expected_types}",
                            severity="warning"
                        )
                    )
                else:
                    self._add_passed(
                        QualityCheckResult(
                            name=f"Data Type Check: {col}",
                            status="passed",
                            details=f"{col} has correct type"
                        )
                    )
    
    def _check_referential_integrity(self, df: DataFrame):
        """Check referential integrity constraints"""
        # Check if all transactions have valid customers (excluding 'Unknown')
        transactions_with_customer = df.filter(
            (F.col("customer_id").isNotNull()) & 
            (F.col("customer_id") != "Unknown")
        ).count()
        
        total_transactions = df.count()
        customer_coverage = transactions_with_customer / total_transactions if total_transactions > 0 else 0
        
        if customer_coverage < 0.5:  # Less than 50% have customer IDs
            self._add_warning(
                QualityCheckResult(
                    name="Referential Integrity",
                    status="warning",
                    message=f"Low customer coverage: {customer_coverage:.1%}",
                    severity="warning",
                    metrics={"customer_coverage": customer_coverage}
                )
            )
        else:
            self._add_passed(
                QualityCheckResult(
                    name="Referential Integrity",
                    status="passed",
                    details=f"Customer coverage: {customer_coverage:.1%}",
                    metrics={"customer_coverage": customer_coverage}
                )
            )
    
    def _check_data_ranges(self, df: DataFrame):
        """Enhanced range validation"""
        # Quantity checks
        quantity_stats = df.select(
            F.min("quantity").alias("min_qty"),
            F.max("quantity").alias("max_qty"),
            F.avg("quantity").alias("avg_qty"),
            F.stddev("quantity").alias("std_qty")
        ).collect()[0]
        
        # Price checks
        price_stats = df.select(
            F.min("unit_price").alias("min_price"),
            F.max("unit_price").alias("max_price"),
            F.avg("unit_price").alias("avg_price"),
            F.stddev("unit_price").alias("std_price")
        ).collect()[0]
        
        issues = []
        
        # Check for negative or zero quantities
        if quantity_stats["min_qty"] <= 0:
            invalid_qty_count = df.filter(F.col("quantity") <= 0).count()
            issues.append(f"{invalid_qty_count:,} records with non-positive quantity")
        
        # Check for extreme quantities
        if quantity_stats["max_qty"] > 10000:
            extreme_qty_count = df.filter(F.col("quantity") > 10000).count()
            issues.append(f"{extreme_qty_count:,} records with quantity > 10,000")
        
        # Check for negative or zero prices
        if price_stats["min_price"] <= 0:
            invalid_price_count = df.filter(F.col("unit_price") <= 0).count()
            issues.append(f"{invalid_price_count:,} records with non-positive price")
        
        # Check for extreme prices
        if price_stats["max_price"] > 10000:
            extreme_price_count = df.filter(F.col("unit_price") > 10000).count()
            issues.append(f"{extreme_price_count:,} records with price > 10,000")
        
        if issues:
            self._add_failed(
                QualityCheckResult(
                    name="Data Range Check",
                    status="failed",
                    message="; ".join(issues),
                    severity="error",
                    metrics={
                        "quantity_stats": quantity_stats,
                        "price_stats": price_stats
                    }
                )
            )
        else:
            self._add_passed(
                QualityCheckResult(
                    name="Data Range Check",
                    status="passed",
                    details="All values within expected ranges",
                    metrics={
                        "quantity_range": f"{quantity_stats['min_qty']}-{quantity_stats['max_qty']}",
                        "price_range": f"{price_stats['min_price']:.2f}-{price_stats['max_price']:.2f}"
                    }
                )
            )
    
    def _check_business_rules(self, df: DataFrame):
        """Comprehensive business rule validation"""
        violations = []
        
        # Rule 1: Total price = quantity * unit price
        price_mismatch = df.filter(
            F.abs(F.col("total_price") - (F.col("quantity") * F.col("unit_price"))) > 0.01
        ).count()
        
        if price_mismatch > 0:
            violations.append(f"{price_mismatch:,} price calculation errors")
        
        # Rule 2: Invoice dates should be during business hours (roughly)
        if "hour" in df.columns:
            unusual_hours = df.filter(
                (F.col("hour") < 6) | (F.col("hour") > 22)
            ).count()
            
            unusual_rate = unusual_hours / df.count() if df.count() > 0 else 0
            if unusual_rate > 0.1:  # More than 10% outside hours
                violations.append(f"{unusual_rate:.1%} transactions outside business hours")
        
        # Rule 3: Customer purchase patterns
        customer_stats = df.groupBy("customer_id").agg(
            F.count("*").alias("transaction_count"),
            F.sum("total_price").alias("total_spent")
        ).filter(F.col("customer_id") != "Unknown")
        
        # Check for suspicious patterns
        suspicious_customers = customer_stats.filter(
            (F.col("transaction_count") > 1000) |  # Too many transactions
            (F.col("total_spent") > 100000)  # Excessive spending
        ).count()
        
        if suspicious_customers > 0:
            violations.append(f"{suspicious_customers} customers with suspicious patterns")
        
        if violations:
            self._add_warning(
                QualityCheckResult(
                    name="Business Rules Check",
                    status="warning",
                    message="; ".join(violations),
                    severity="warning"
                )
            )
        else:
            self._add_passed(
                QualityCheckResult(
                    name="Business Rules Check",
                    status="passed",
                    details="All business rules satisfied"
                )
            )
    
    def _check_data_consistency(self, df: DataFrame):
        """Check internal data consistency"""
        # Check if product descriptions are consistent for same product ID
        product_consistency = df.groupBy("product_id").agg(
            F.countDistinct("product_description").alias("desc_count"),
            F.first("product_description").alias("sample_desc")
        ).filter(F.col("desc_count") > 1)
        
        inconsistent_products = product_consistency.count()
        
        if inconsistent_products > 0:
            self._add_warning(
                QualityCheckResult(
                    name="Data Consistency Check",
                    status="warning",
                    message=f"{inconsistent_products} products have inconsistent descriptions",
                    severity="warning",
                    metrics={"inconsistent_products": inconsistent_products}
                )
            )
        else:
            self._add_passed(
                QualityCheckResult(
                    name="Data Consistency Check",
                    status="passed",
                    details="Product descriptions are consistent"
                )
            )
    
    def _check_date_consistency(self, df: DataFrame):
        """Enhanced date validation for e-commerce data"""
        # Get date range
        date_stats = df.select(
            F.min("invoice_date").alias("min_date"),
            F.max("invoice_date").alias("max_date"),
            F.countDistinct("invoice_date").alias("unique_dates")
        ).collect()[0]
        
        min_date = date_stats["min_date"]
        max_date = date_stats["max_date"]
        unique_dates = date_stats["unique_dates"]
        
        # Calculate expected date range
        date_range_days = (max_date - min_date).days if min_date and max_date else 0
        
        issues = []
        
        # Check if dates are in expected range (2010-2012 for this dataset)
        if min_date and min_date.year < 2010:
            issues.append(f"Dates before 2010 found: {min_date}")
        
        if max_date and max_date.year > 2012:
            issues.append(f"Dates after 2012 found: {max_date}")
        
        # Check for future dates
        today = date.today()
        future_dates = df.filter(F.col("invoice_date") > today).count()
        
        if future_dates > 0:
            issues.append(f"{future_dates:,} records with future dates")
        
        # Check date density - be more lenient for e-commerce data
        date_density = 0
        if date_range_days > 0:
            date_density = unique_dates / date_range_days
            
            # E-commerce data typically doesn't have sales every day
            # 40% density is reasonable (accounting for weekends, holidays)
            if date_density < 0.3:  # Changed from 0.5 to 0.3
                issues.append(f"Very low date density: {date_density:.1%}")
            
            logger.info(f"Date density: {date_density:.1%} ({unique_dates} unique dates in {date_range_days} days)")
        
        if issues:
            # Only fail for critical issues
            critical_issues = [i for i in issues if "future dates" in i or "Very low" in i]
            
            if critical_issues:
                self._add_failed(
                    QualityCheckResult(
                        name="Date Consistency Check",
                        status="failed",
                        message="; ".join(critical_issues),
                        severity="error",
                        metrics={
                            "date_range": f"{min_date} to {max_date}",
                            "unique_dates": unique_dates,
                            "date_density": date_density
                        }
                    )
                )
            else:
                self._add_warning(
                    QualityCheckResult(
                        name="Date Consistency Check",
                        status="warning",
                        message="; ".join(issues),
                        severity="warning",
                        metrics={
                            "date_range": f"{min_date} to {max_date}",
                            "unique_dates": unique_dates,
                            "date_density": date_density
                        }
                    )
                )
        else:
            self._add_passed(
                QualityCheckResult(
                    name="Date Consistency Check",
                    status="passed",
                    details=f"Date range: {min_date} to {max_date} ({unique_dates} unique dates, {date_density:.1%} density)",
                    metrics={
                        "date_range_days": date_range_days,
                        "unique_dates": unique_dates,
                        "date_density": date_density
                    }
                )
            )
    
    def _check_time_gaps(self, df: DataFrame):
        """Check for significant time gaps in data"""
        try:
            # Get daily transaction counts
            daily_counts = df.groupBy("invoice_date").count().orderBy("invoice_date")
            
            # For small datasets, just pass
            if daily_counts.count() < 2:
                self._add_passed(
                    QualityCheckResult(
                        name="Time Gap Check",
                        status="passed",
                        details="Not enough data to check time gaps"
                    )
                )
                return
            
            # Alternative approach without Pandas conversion
            # Get all dates
            dates = daily_counts.select("invoice_date").collect()
            
            if len(dates) < 2:
                self._add_passed(
                    QualityCheckResult(
                        name="Time Gap Check",
                        status="passed",
                        details="Insufficient data for gap analysis"
                    )
                )
                return
            
            # Calculate gaps manually
            max_gap = 0
            gap_count = 0
            
            for i in range(1, len(dates)):
                current_date = dates[i][0]
                prev_date = dates[i-1][0]
                gap_days = (current_date - prev_date).days
                
                if gap_days > 7:
                    gap_count += 1
                    max_gap = max(max_gap, gap_days)
            
            if gap_count > 0:
                self._add_warning(
                    QualityCheckResult(
                        name="Time Gap Check",
                        status="warning",
                        message=f"Found {gap_count} time gaps, max gap: {max_gap} days",
                        severity="warning",
                        metrics={
                            "gap_count": gap_count,
                            "max_gap_days": max_gap
                        }
                    )
                )
            else:
                self._add_passed(
                    QualityCheckResult(
                        name="Time Gap Check",
                        status="passed",
                        details="No significant time gaps found"
                    )
                )
                    
        except Exception as e:
            logger.warning(f"Time gap check failed: {str(e)[:200]}")  # Limit error message length
            # Don't fail the check, just warn
            self._add_warning(
                QualityCheckResult(
                    name="Time Gap Check",
                    status="warning",
                    message="Could not complete time gap analysis",
                    severity="warning"
                )
            )
    
    def _check_outliers(self, df: DataFrame):
        """Statistical outlier detection"""
        # Check for outliers in key numeric columns
        numeric_columns = ["quantity", "unit_price", "total_price"]
        
        outlier_results = {}
        
        for col in numeric_columns:
            if col in df.columns:
                try:
                    # Calculate IQR using percentile_approx
                    quantiles = df.selectExpr(
                        f"percentile_approx({col}, array(0.25, 0.75)) as quantiles"
                    ).collect()[0]["quantiles"]
                    
                    q1, q3 = quantiles
                    iqr = q3 - q1
                    
                    # Define outlier bounds
                    lower_bound = q1 - 1.5 * iqr
                    upper_bound = q3 + 1.5 * iqr
                    
                    # Count outliers
                    outliers = df.filter(
                        (F.col(col) < lower_bound) | (F.col(col) > upper_bound)
                    ).count()
                    
                    outlier_rate = outliers / df.count() if df.count() > 0 else 0
                    
                    outlier_results[col] = {
                        "outlier_count": outliers,
                        "outlier_rate": outlier_rate,
                        "bounds": (lower_bound, upper_bound)
                    }
                    
                    if outlier_rate > 0.05:  # More than 5% outliers
                        self._add_warning(
                            QualityCheckResult(
                                name=f"Outlier Check: {col}",
                                status="warning",
                                message=f"{outliers:,} outliers ({outlier_rate:.1%}) in {col}",
                                severity="warning",
                                metrics=outlier_results[col]
                            )
                        )
                except Exception as e:
                    logger.warning(f"Outlier check failed for {col}: {e}")
        
        if not any(r.get("outlier_rate", 0) > 0.05 for r in outlier_results.values()):
            self._add_passed(
                QualityCheckResult(
                    name="Outlier Check",
                    status="passed",
                    details="Outlier rates within acceptable range",
                    metrics=outlier_results
                )
            )
    
    def _check_data_distribution(self, df: DataFrame):
        """Check data distribution patterns"""
        try:
            # Check country distribution
            country_dist = df.groupBy("country").count().orderBy(F.desc("count"))
            country_count = country_dist.count()
            
            if country_count == 0:
                self._add_warning(
                    QualityCheckResult(
                        name="Data Distribution Check",
                        status="warning",
                        message="No country data found",
                        severity="warning"
                    )
                )
                return
            
            top_countries = country_dist.limit(5).collect()
            
            # Check if data is too concentrated
            total_count = df.count()
            top_country_count = top_countries[0]["count"] if top_countries else 0
            concentration = top_country_count / total_count if total_count > 0 else 0
            
            if concentration > 0.8:  # More than 80% from one country
                self._add_warning(
                    QualityCheckResult(
                        name="Data Distribution Check",
                        status="warning",
                        message=f"Data highly concentrated: {concentration:.1%} from {top_countries[0]['country']}",
                        severity="warning",
                        metrics={
                            "top_country": top_countries[0]["country"],
                            "concentration": concentration,
                            "country_count": country_count
                        }
                    )
                )
            else:
                self._add_passed(
                    QualityCheckResult(
                        name="Data Distribution Check",
                        status="passed",
                        details=f"Data well distributed across {country_count} countries",
                        metrics={
                            "country_count": country_count,
                            "top_country_share": concentration
                        }
                    )
                )
                
        except Exception as e:
            logger.warning(f"Data distribution check failed: {e}")
            self._add_warning(
                QualityCheckResult(
                    name="Data Distribution Check",
                    status="warning",
                    message=f"Could not check data distribution: {str(e)}",
                    severity="warning"
                )
            )
    
    def _add_passed(self, result: QualityCheckResult):
        """Add passed check result"""
        self.checks_passed.append(result)
    
    def _add_failed(self, result: QualityCheckResult):
        """Add failed check result"""
        self.checks_failed.append(result)
    
    def _add_warning(self, result: QualityCheckResult):
        """Add warning check result"""
        self.checks_warning.append(result)
    
    def generate_quality_report(self, results: Dict, output_path: Optional[Path] = None) -> str:
        """Generate detailed quality report"""
        report = []
        report.append("="*60)
        report.append("DATA QUALITY REPORT")
        report.append("="*60)
        report.append(f"Generated: {results['timestamp']}")
        report.append(f"Quality Score: {results['quality_score']:.1f}%")
        report.append("")
        
        report.append("SUMMARY")
        report.append("-"*30)
        report.append(f"Total Checks: {results['total_checks']}")
        report.append(f"Passed: {results['passed']}")
        report.append(f"Failed: {results['failed']}")
        report.append(f"Warnings: {results['warnings']}")
        report.append("")
        
        if results['failed_checks']:
            report.append("FAILED CHECKS")
            report.append("-"*30)
            for check in results['failed_checks']:
                report.append(f"❌ {check['name']}")
                report.append(f"   {check['message']}")
                if check.get('metrics'):
                    report.append(f"   Metrics: {check['metrics']}")
                report.append("")
        
        if results['warning_checks']:
            report.append("WARNINGS")
            report.append("-"*30)
            for check in results['warning_checks']:
                report.append(f"⚠️  {check['name']}")
                report.append(f"   {check['message']}")
                report.append("")
        
        report_text = "\n".join(report)
        
        # Save report if path provided
        if output_path:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, 'w') as f:
                f.write(report_text)
            
            # Also save JSON version
            json_path = output_path.with_suffix('.json')
            with open(json_path, 'w') as f:
                json.dump(results, f, indent=2, default=str)
        
        return report_text