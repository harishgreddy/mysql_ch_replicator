#!/usr/bin/env python3

import argparse
import logging
import time
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from dataclasses import dataclass
from typing import Optional

from .config import Settings
from .mysql_api import MySQLApi
from .clickhouse_api import ClickhouseApi
from .converter import MysqlToClickhouseConverter
from .table_structure import TableStructure


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


class BulkInserter:
    """
    Standalone bulk insert utility for MySQL to ClickHouse data migration.

    This operates independently of the existing replication system and can handle
    excluded tables without affecting binlog replication, optimizer, or state files.
    """

    DEFAULT_BATCH_SIZE = 50000
    DEFAULT_THREADS = 4

    def __init__(self, config: Settings, source_db: str, target_db: str, table_name: str,
                 batch_size: int = DEFAULT_BATCH_SIZE, threads: int = DEFAULT_THREADS,
                 drop_existing: bool = False, resume: bool = True):
        self.config = config
        self.source_db = source_db
        self.target_db = target_db
        self.table_name = table_name
        self.batch_size = batch_size
        self.threads = threads
        self.drop_existing = drop_existing
        self.resume = resume

        # Initialize APIs
        self.mysql_api = MySQLApi(database=source_db, mysql_settings=config.mysql)
        self.clickhouse_api = ClickhouseApi(database=target_db, clickhouse_settings=config.clickhouse)

        # Create converter instance (without db_replicator dependency for basic conversion)
        self.converter = MysqlToClickhouseConverter()
        self.converter.config = config  # Set config directly for type mappings

        # Statistics tracking
        self.stats = BulkInsertStats()
        self.stats_lock = Lock()

        # Table structures
        self.mysql_structure: Optional[TableStructure] = None
        self.clickhouse_structure: Optional[TableStructure] = None

        # Resume position
        self.resume_position: Optional[tuple] = None

        self.logger = logging.getLogger(__name__)

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
            format='[BULK_INSERT %(asctime)s %(levelname)8s] %(message)s',
            handlers=[logging.StreamHandler(sys.stderr)]
        )

    def analyze_table_structure(self):
        """Analyze source table structure and create ClickHouse equivalent."""
        self.logger.info(f"Analyzing table structure for {self.source_db}.{self.table_name}")

        # Get MySQL table structure
        mysql_create_statement = self.mysql_api.get_table_create_statement(self.table_name)
        self.mysql_structure = self.converter.parse_mysql_table_structure(
            mysql_create_statement, required_table_name=self.table_name
        )

        # Convert to ClickHouse structure
        self.clickhouse_structure = self.converter.convert_table_structure(self.mysql_structure)

        self.logger.info(f"Table has {len(self.mysql_structure.fields)} fields")
        self.logger.info(f"Primary keys: {self.mysql_structure.primary_keys}")

        # Validate primary key exists
        if not self.mysql_structure.primary_keys:
            raise Exception(
                f"Table {self.table_name} has no primary key. Bulk insert requires a primary key for ordering.")

    def create_clickhouse_table(self):
        """Create the target table in ClickHouse."""
        self.logger.info(f"Creating ClickHouse table {self.target_db}.{self.table_name}")

        # Ensure target database exists
        if self.target_db not in self.clickhouse_api.get_databases():
            self.logger.info(f"Creating target database: {self.target_db}")
            self.clickhouse_api.create_database(self.target_db)

        # Check if table already exists
        existing_tables = self.clickhouse_api.get_tables()
        table_exists = self.table_name in existing_tables

        # Drop existing table if requested
        if self.drop_existing and table_exists:
            self.logger.info(f"Dropping existing table {self.target_db}.{self.table_name}")
            self.clickhouse_api.execute_command(
                f'DROP TABLE IF EXISTS `{self.target_db}`.`{self.table_name}`'
            )
            table_exists = False

        # Create table if it doesn't exist
        if not table_exists:
            # Get any custom indexes/partitions from config
            indexes = self.config.get_indexes(self.target_db, self.table_name)
            partition_bys = self.config.get_partition_bys(self.target_db, self.table_name)

            # Create table
            self.clickhouse_structure.if_not_exists = True
            self.clickhouse_api.create_table(
                self.clickhouse_structure,
                additional_indexes=indexes,
                additional_partition_bys=partition_bys
            )
        else:
            self.logger.info(f"Table {self.target_db}.{self.table_name} already exists")

        # Determine resume position if resume is enabled
        if self.resume and not self.drop_existing:
            clickhouse_count = self.get_clickhouse_record_count()
            if clickhouse_count > 0:
                self.resume_position = self.get_resume_position()
                self.logger.info(f"Found {clickhouse_count:,} existing records in ClickHouse")
                if self.resume_position:
                    self.logger.info(f"Will resume from position: {self.resume_position}")
                else:
                    self.logger.warning("Could not determine resume position, will start from beginning")
            else:
                self.logger.info("No existing records found in ClickHouse, starting from beginning")

    def get_total_record_count(self) -> int:
        """Get total number of records in source table."""
        self.mysql_api.execute(f"SELECT COUNT(*) FROM `{self.table_name}`")
        result = self.mysql_api.cursor.fetchone()
        return result[0] if result else 0

    def get_clickhouse_record_count(self) -> int:
        """Get total number of records in target ClickHouse table."""
        try:
            result = self.clickhouse_api.query(f"SELECT COUNT(*) FROM `{self.target_db}`.`{self.table_name}`")
            return result.result_rows[0][0] if result.result_rows else 0
        except Exception as e:
            self.logger.warning(f"Could not get ClickHouse record count: {e}")
            return 0

    def get_resume_position(self) -> Optional[tuple]:
        """
        Get the position to resume from based on existing ClickHouse records.
        Returns the maximum primary key value from ClickHouse table.
        """
        try:
            primary_keys = self.clickhouse_structure.primary_keys
            if not primary_keys:
                return None

            # Build query to get max primary key values
            if len(primary_keys) == 1:
                query = f"SELECT MAX(`{primary_keys[0]}`) FROM `{self.target_db}`.`{self.table_name}`"
            else:
                # For composite primary keys, we need to get the maximum tuple
                key_list = ', '.join(f'`{key}`' for key in primary_keys)
                query = f"SELECT {key_list} FROM `{self.target_db}`.`{self.table_name}` ORDER BY {key_list} DESC LIMIT 1"

            result = self.clickhouse_api.query(query)
            if not result.result_rows or not result.result_rows[0] or result.result_rows[0][0] is None:
                return None

            if len(primary_keys) == 1:
                return (result.result_rows[0][0],)
            else:
                return tuple(result.result_rows[0])

        except Exception as e:
            self.logger.warning(f"Could not determine resume position: {e}")
            return None

    def process_batch(self, worker_id: int, start_value: Optional[tuple] = None) -> tuple[int, Optional[tuple]]:
        """
        Process a single batch of records.

        Returns:
            tuple: (records_processed, last_primary_key_value)
        """
        try:
            # Get records for this batch
            records = self.mysql_api.get_records(
                table_name=self.table_name,
                order_by=self.mysql_structure.primary_keys,
                limit=self.batch_size,
                start_value=start_value,
                worker_id=worker_id,
                total_workers=self.threads
            )

            if not records:
                return 0, None

            # Convert records to ClickHouse format
            converted_records = self.converter.convert_records(
                records, self.mysql_structure, self.clickhouse_structure
            )

            # Insert to ClickHouse
            self.clickhouse_api.insert(
                self.table_name, converted_records, table_structure=self.clickhouse_structure
            )

            # Get the last primary key value for continuation
            last_record = records[-1]
            primary_key_ids = self.mysql_structure.primary_key_ids
            last_primary_key = tuple(last_record[idx] for idx in primary_key_ids)

            return len(records), last_primary_key

        except Exception as e:
            self.logger.error(f"Worker {worker_id} batch processing error: {e}")
            with self.stats_lock:
                self.stats.errors += 1
            raise

    def update_stats(self, records_processed: int):
        """Thread-safe stats update."""
        with self.stats_lock:
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
            f"{rps:,.0f} records/sec | ETA: {eta_str} | Errors: {self.stats.errors}"
        )

    def run_parallel_insert(self):
        """Run bulk insert with parallel workers."""
        self.logger.info(f"Starting bulk insert with {self.threads} threads, batch size {self.batch_size}")

        total_records = self.get_total_record_count()
        clickhouse_records = self.get_clickhouse_record_count()

        self.logger.info(f"MySQL records: {total_records:,}")
        self.logger.info(f"ClickHouse records: {clickhouse_records:,}")

        if total_records <= clickhouse_records:
            self.logger.info("ClickHouse already has all records, nothing to process")
            return

        estimated_remaining = total_records - clickhouse_records
        self.logger.info(f"Estimated records to process: {estimated_remaining:,}")

        # Use ThreadPoolExecutor for parallel processing
        with ThreadPoolExecutor(max_workers=self.threads) as executor:
            # Submit initial workers
            futures = {}
            for worker_id in range(self.threads):
                future = executor.submit(self.process_worker_batches, worker_id, total_records)
                futures[future] = worker_id

            # Process completed workers and monitor progress
            last_log_time = time.time()
            for future in as_completed(futures):
                worker_id = futures[future]
                try:
                    worker_stats = future.result()
                    self.logger.info(f"Worker {worker_id} completed: {worker_stats}")
                except Exception as e:
                    self.logger.error(f"Worker {worker_id} failed: {e}")

                # Log progress every 30 seconds
                if time.time() - last_log_time > 30:
                    self.log_progress(total_records)
                    last_log_time = time.time()

        # Final statistics
        self.log_final_stats(total_records)

    def process_worker_batches(self, worker_id: int, total_records: int) -> dict:
        """Process batches for a single worker using hash-based partitioning."""
        worker_stats = {'records': 0, 'batches': 0, 'errors': 0}

        # For parallel workers, start from resume position or beginning
        start_value = self.resume_position if worker_id == 0 else None

        while True:
            try:
                records_processed, last_primary_key = self.process_batch(worker_id, start_value)

                if records_processed == 0:
                    break

                worker_stats['records'] += records_processed
                worker_stats['batches'] += 1

                self.update_stats(records_processed)
                start_value = last_primary_key

                # Check if we should continue (simple termination check)
                if records_processed < self.batch_size:
                    break

            except Exception as e:
                worker_stats['errors'] += 1
                if worker_stats['errors'] > 5:  # Stop worker after too many errors
                    self.logger.error(f"Worker {worker_id} stopping due to too many errors")
                    break
                time.sleep(1)  # Brief pause before retry

        return worker_stats

    def run_single_threaded_insert(self):
        """Run bulk insert with single thread (simpler, more reliable)."""
        self.logger.info(f"Starting single-threaded bulk insert, batch size {self.batch_size}")

        total_records = self.get_total_record_count()
        clickhouse_records = self.get_clickhouse_record_count()

        self.logger.info(f"MySQL records: {total_records:,}")
        self.logger.info(f"ClickHouse records: {clickhouse_records:,}")

        if total_records <= clickhouse_records:
            self.logger.info("ClickHouse already has all records, nothing to process")
            return

        estimated_remaining = total_records - clickhouse_records
        self.logger.info(f"Estimated records to process: {estimated_remaining:,}")

        start_value = self.resume_position
        last_log_time = time.time()

        while True:
            try:
                records_processed, last_primary_key = self.process_batch(0, start_value)

                if records_processed == 0:
                    break

                self.update_stats(records_processed)
                start_value = last_primary_key

                # Log progress every 30 seconds
                if time.time() - last_log_time > 30:
                    self.log_progress(total_records)
                    last_log_time = time.time()

                # Check if we should continue
                if records_processed < self.batch_size:
                    break

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
        if self.stats.total_records_processed != total_records:
            self.logger.warning(f"Expected {total_records:,} records, processed {self.stats.total_records_processed:,}")
        self.logger.info("=" * 60)

    def run(self):
        """Main execution method."""
        try:
            self.logger.info(
                f"Starting bulk insert: {self.source_db}.{self.table_name} -> {self.target_db}.{self.table_name}")

            # Analyze source table
            self.analyze_table_structure()

            # Create target table
            self.create_clickhouse_table()

            # Run the bulk insert
            if self.threads > 1:
                self.run_parallel_insert()
            else:
                self.run_single_threaded_insert()

            self.logger.info("Bulk insert completed successfully!")

        except Exception as e:
            self.logger.error(f"Bulk insert failed: {e}")
            raise
        finally:
            # Cleanup
            if hasattr(self, 'mysql_api'):
                self.mysql_api.close()


def main():
    parser = argparse.ArgumentParser(description="Bulk insert utility for MySQL to ClickHouse")
    parser.add_argument("--config", required=True, help="Config file path")
    parser.add_argument("--source-db", required=True, help="Source MySQL database name")
    parser.add_argument("--target-db", required=True, help="Target ClickHouse database name")
    parser.add_argument("--table", required=True, help="Table name to bulk insert")
    parser.add_argument("--batch-size", type=int, default=BulkInserter.DEFAULT_BATCH_SIZE,
                        help=f"Batch size for processing (default: {BulkInserter.DEFAULT_BATCH_SIZE})")
    parser.add_argument("--threads", type=int, default=BulkInserter.DEFAULT_THREADS,
                        help=f"Number of parallel threads (default: {BulkInserter.DEFAULT_THREADS})")
    parser.add_argument("--drop-existing", action="store_true",
                        help="Drop existing table in ClickHouse before creating")
    parser.add_argument("--no-resume", action="store_true",
                        help="Don't resume from existing records, start from beginning")
    parser.add_argument("--log-level", default="info", choices=['debug', 'info', 'warning', 'error'],
                        help="Logging level (default: info)")

    args = parser.parse_args()

    # Load configuration
    config = Settings()
    config.load(args.config)

    # Create and run bulk inserter
    inserter = BulkInserter(
        config=config,
        source_db=args.source_db,
        target_db=args.target_db,
        table_name=args.table,
        batch_size=args.batch_size,
        threads=args.threads,
        drop_existing=args.drop_existing,
        resume=not args.no_resume  # resume is True unless --no-resume is specified
    )

    inserter.setup_logging(args.log_level)
    inserter.run()


# Function to be called from main.py
def run_bulk_insert(args, config: Settings):
    """Entry point for bulk insert when called from main.py"""
    # Use source_db if provided, otherwise fall back to db
    source_db = getattr(args, 'source_db', None) or args.db
    if not source_db:
        raise Exception("--source_db or --db argument is required for bulk_insert mode")

    # Use target_db if provided, otherwise use same as source_db
    target_db = args.target_db or source_db

    if not args.table:
        raise Exception("--table argument is required for bulk_insert mode")

    # Set up logging
    log_tag = f'bulk_insert {source_db}.{args.table}'
    from .main import set_logging_config
    set_logging_config(log_tag, log_level_str=config.log_level)

    # Create and run bulk inserter
    inserter = BulkInserter(
        config=config,
        source_db=source_db,
        target_db=target_db,
        table_name=args.table,
        batch_size=getattr(args, 'batch_size', BulkInserter.DEFAULT_BATCH_SIZE),
        threads=getattr(args, 'threads', BulkInserter.DEFAULT_THREADS),
        drop_existing=getattr(args, 'drop_existing', False),
        resume=not getattr(args, 'no_resume', False)
    )

    inserter.run()


if __name__ == '__main__':
    main()