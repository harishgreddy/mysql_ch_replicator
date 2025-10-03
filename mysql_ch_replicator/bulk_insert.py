#!/usr/bin/env python3

import argparse
import logging
import time
import sys
import mysql.connector
import clickhouse_connect
from dataclasses import dataclass
from typing import Optional

from .config import Settings


@dataclass
class BulkInsertStats:
    total_records_processed: int = 0
    total_batches_processed: int = 0
    start_time: float = 0
    errors: int = 0

    def __post_init__(self):
        if self.start_time == 0:
            self.start_time = time.time()

    def get_elapsed_time(self):
        return time.time() - self.start_time

    def get_records_per_second(self):
        elapsed = self.get_elapsed_time()
        return self.total_records_processed / elapsed if elapsed > 0 else 0


class HardcodedBulkInserter:
    """
    Hardcoded bulk insert utility that uses raw SQL without touching existing APIs.
    Specifically designed for aggregated_stock_summary_wrapper table.
    """

    DEFAULT_BATCH_SIZE = 100000

    def __init__(self, config: Settings, source_db: str, target_db: str,
                 batch_size: int = DEFAULT_BATCH_SIZE, resume: bool = True):
        self.config = config
        self.source_db = source_db
        self.target_db = target_db
        self.batch_size = batch_size
        self.resume = resume

        # Statistics tracking
        self.stats = BulkInsertStats()

        # Resume position
        self.resume_position: Optional[tuple] = None

        self.logger = logging.getLogger(__name__)

        # MySQL connection
        self.mysql_conn = None
        self.mysql_cursor = None

        # ClickHouse connection
        self.clickhouse_client = None

    def setup_logging(self, log_level: str = 'info'):
        """Setup logging for the bulk insert process."""
        log_levels = {
            'critical': logging.CRITICAL,
            'error': logging.ERROR,
            'warning': logging.WARNING,
            'info': logging.INFO,
            'debug': logging.DEBUG,
        }

        level = log_levels.get(log_level.lower(), logging.INFO)
        logging.basicConfig(
            level=level,
            format='[HARDCODED_BULK_INSERT %(asctime)s %(levelname)8s] %(message)s',
            handlers=[logging.StreamHandler(sys.stderr)]
        )

    def connect_mysql(self):
        """Connect to MySQL"""
        try:
            self.mysql_conn = mysql.connector.connect(
                host=self.config.mysql.host,
                port=self.config.mysql.port,
                user=self.config.mysql.user,
                password=self.config.mysql.password,
                database=self.source_db
            )
            self.mysql_cursor = self.mysql_conn.cursor()
            self.logger.info("Connected to MySQL")
        except Exception as e:
            self.logger.error(f"Failed to connect to MySQL: {e}")
            raise

    def connect_clickhouse(self):
        """Connect to ClickHouse"""
        try:
            self.clickhouse_client = clickhouse_connect.get_client(
                host=self.config.clickhouse.host,
                port=self.config.clickhouse.port,
                username=self.config.clickhouse.user,
                password=self.config.clickhouse.password,
                connect_timeout=self.config.clickhouse.connection_timeout,
                send_receive_timeout=self.config.clickhouse.send_receive_timeout,
            )
            self.logger.info("Connected to ClickHouse")
        except Exception as e:
            self.logger.error(f"Failed to connect to ClickHouse: {e}")
            raise

    def ensure_clickhouse_database_and_table(self):
        """Ensure ClickHouse database and table exist"""
        # Create database if not exists
        self.clickhouse_client.command(f'CREATE DATABASE IF NOT EXISTS `{self.target_db}`')

        # Create table if not exists
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS `{self.target_db}`.`aggregated_stock_summary_wrapper`
        (
            code                           String,
            pattern                        String,
            transaction_date               Date32,
            financial_year                 Int32,
            item_code                      String,
            warehouse_id                   Int64,
            flock_id                       Nullable(Int64),
            age                            Nullable(Float64),
            chick_grade_id                 Nullable(Int64),
            egg_grade_id                   Nullable(Int64),
            production_date                Nullable(Date32),
            opening_stock                  Float64,
            closing_stock                  Float64,
            total_purchased                Float64,
            total_purchased_value          Float64,
            total_received                 Float64,
            total_received_value           Float64,
            total_stock_adjusted_in        Float64,
            total_stock_adjusted_in_value  Float64,
            total_stock_adjusted_out       Float64,
            total_stock_adjusted_out_value Float64,
            total_sales_return             Float64,
            total_sales_return_value       Float64,
            total_produced                 Float64,
            total_produced_value           Float64,
            total_consumed                 Float64,
            total_consumed_value           Float64,
            total_transferred              Float64,
            total_transferred_value        Float64,
            total_sold                     Float64,
            total_sold_value               Float64,
            avg_unit_price                 Float64,
            transactional                  Bool,
            last_modified_date             DateTime64(6),
            opening_rate                   Float64,
            closing_rate                   Float64,
            _version                       UInt64
        )
        ENGINE = ReplacingMergeTree(_version)
        ORDER BY (code, financial_year)
        SETTINGS index_granularity = 8192
        """

        self.clickhouse_client.command(create_table_sql)
        self.logger.info("ClickHouse table ensured")

    def get_total_mysql_records(self) -> int:
        """Get total records in MySQL table"""
        self.mysql_cursor.execute("SELECT COUNT(*) FROM aggregated_stock_summary_wrapper")
        result = self.mysql_cursor.fetchone()
        return result[0] if result else 0

    def get_clickhouse_record_count(self) -> int:
        """Get total records in ClickHouse table"""
        try:
            result = self.clickhouse_client.query(
                f"SELECT COUNT(*) FROM `{self.target_db}`.`aggregated_stock_summary_wrapper`")
            return result.result_rows[0][0] if result.result_rows else 0
        except Exception as e:
            self.logger.warning(f"Could not get ClickHouse record count: {e}")
            return 0

    def get_resume_position(self) -> Optional[tuple]:
        """Get resume position from ClickHouse"""
        if not self.resume:
            return None

        try:
            result = self.clickhouse_client.query(
                f"SELECT code, financial_year FROM `{self.target_db}`.`aggregated_stock_summary_wrapper` "
                f"ORDER BY code, financial_year DESC LIMIT 1"
            )
            if result.result_rows:
                return tuple(result.result_rows[0])
            return None
        except Exception as e:
            self.logger.warning(f"Could not determine resume position: {e}")
            return None

    def process_batch(self, offset: int) -> int:
        """Process a batch using LIMIT/OFFSET"""
        try:
            # Use simple LIMIT/OFFSET to avoid WHERE clause issues
            query = f"""
            SELECT 
                code, pattern, transaction_date, financial_year, item_code, warehouse_id,
                flock_id, age, chick_grade_id, egg_grade_id, production_date,
                opening_stock, closing_stock, total_purchased, total_purchased_value,
                total_received, total_received_value, total_stock_adjusted_in, total_stock_adjusted_in_value,
                total_stock_adjusted_out, total_stock_adjusted_out_value, total_sales_return, total_sales_return_value,
                total_produced, total_produced_value, total_consumed, total_consumed_value,
                total_transferred, total_transferred_value, total_sold, total_sold_value,
                avg_unit_price, transactional, last_modified_date, opening_rate, closing_rate
            FROM aggregated_stock_summary_wrapper 
            ORDER BY code, financial_year 
            LIMIT {self.batch_size} OFFSET {offset}
            """

            self.mysql_cursor.execute(query)
            records = self.mysql_cursor.fetchall()

            if not records:
                return 0

            # Get current version for ClickHouse
            current_version = int(time.time() * 1000000)  # microsecond timestamp

            # Prepare data for ClickHouse insert
            clickhouse_records = []
            for record in records:
                # Convert MySQL record to ClickHouse format
                ch_record = list(record)
                ch_record.append(current_version)  # Add _version
                current_version += 1
                clickhouse_records.append(tuple(ch_record))

            # Insert into ClickHouse
            self.clickhouse_client.insert(
                f'`{self.target_db}`.`aggregated_stock_summary_wrapper`',
                clickhouse_records
            )

            return len(records)

        except Exception as e:
            self.logger.error(f"Batch processing error at offset {offset}: {e}")
            raise

    def update_stats(self, records_processed: int):
        """Update statistics."""
        self.stats.total_records_processed += records_processed
        self.stats.total_batches_processed += 1

    def log_progress(self, total_records: int):
        """Log current progress."""
        elapsed = self.stats.get_elapsed_time()
        rps = self.stats.get_records_per_second()
        progress_pct = (self.stats.total_records_processed / total_records) * 100 if total_records > 0 else 0

        eta_seconds = 0
        if rps > 0:
            remaining_records = total_records - self.stats.total_records_processed
            eta_seconds = remaining_records / rps

        eta_str = f"{int(eta_seconds // 3600):02d}:{int((eta_seconds % 3600) // 60):02d}:{int(eta_seconds % 60):02d}"

        self.logger.info(
            f"Progress: {self.stats.total_records_processed:,}/{total_records:,} records "
            f"({progress_pct:.1f}%) | {self.stats.total_batches_processed:,} batches | "
            f"{rps:,.0f} records/sec | ETA: {eta_str}"
        )

    def run_bulk_insert(self):
        """Run the bulk insert using LIMIT/OFFSET approach"""
        total_records = self.get_total_mysql_records()
        clickhouse_records = self.get_clickhouse_record_count()

        self.logger.info(f"MySQL records: {total_records:,}")
        self.logger.info(f"ClickHouse records: {clickhouse_records:,}")

        if total_records <= clickhouse_records:
            self.logger.info("ClickHouse already has all records, nothing to process")
            return

        # Start from where ClickHouse left off
        start_offset = clickhouse_records

        self.logger.info(f"Starting from offset: {start_offset:,}")
        self.logger.info(f"Estimated records to process: {total_records - start_offset:,}")

        offset = start_offset
        last_log_time = time.time()

        while offset < total_records:
            try:
                records_processed = self.process_batch(offset)

                if records_processed == 0:
                    break

                self.update_stats(records_processed)
                offset += records_processed

                # Log progress every 30 seconds
                if time.time() - last_log_time > 30:
                    self.log_progress(total_records)
                    last_log_time = time.time()

            except Exception as e:
                self.logger.error(f"Batch processing error: {e}")
                self.stats.errors += 1
                if self.stats.errors > 10:
                    raise Exception("Too many errors, stopping bulk insert")
                time.sleep(1)

        self.log_final_stats(total_records)

    def log_final_stats(self, total_records: int):
        """Log final statistics."""
        elapsed = self.stats.get_elapsed_time()
        rps = self.stats.get_records_per_second()

        self.logger.info("=" * 60)
        self.logger.info("BULK INSERT COMPLETED")
        self.logger.info(f"Total records processed: {self.stats.total_records_processed:,}")
        self.logger.info(f"Total batches: {self.stats.total_batches_processed:,}")
        self.logger.info(f"Total time: {elapsed:.1f} seconds")
        self.logger.info(f"Average rate: {rps:,.0f} records/second")
        self.logger.info(f"Errors: {self.stats.errors}")
        self.logger.info("=" * 60)

    def run(self):
        """Main execution method."""
        try:
            self.logger.info(
                f"Starting hardcoded bulk insert: {self.source_db}.aggregated_stock_summary_wrapper -> {self.target_db}.aggregated_stock_summary_wrapper")

            # Connect to databases
            self.connect_mysql()
            self.connect_clickhouse()

            # Run bulk insert
            self.run_bulk_insert()

            self.logger.info("Bulk insert completed successfully!")

        except Exception as e:
            self.logger.error(f"Bulk insert failed: {e}")
            raise
        finally:
            # Cleanup
            if self.mysql_cursor:
                self.mysql_cursor.close()
            if self.mysql_conn:
                self.mysql_conn.close()


# Function to be called from main.py
def run_hardcoded_bulk_insert(args, config: Settings):
    """Entry point for hardcoded bulk insert when called from main.py"""
    # Use source_db if provided, otherwise fall back to db
    source_db = getattr(args, 'source_db', None) or args.db
    if not source_db:
        raise Exception("--source_db or --db argument is required for bulk_insert mode")

    # Use target_db if provided, otherwise use same as source_db
    target_db = args.target_db or source_db

    # Set up logging
    log_tag = f'hardcoded_bulk_insert {source_db}.aggregated_stock_summary_wrapper'
    from .main import set_logging_config
    set_logging_config(log_tag, log_level_str=config.log_level)

    # Create and run bulk inserter
    inserter = HardcodedBulkInserter(
        config=config,
        source_db=source_db,
        target_db=target_db,
        batch_size=getattr(args, 'batch_size', HardcodedBulkInserter.DEFAULT_BATCH_SIZE),
        resume=not getattr(args, 'no_resume', False)
    )

    inserter.run()