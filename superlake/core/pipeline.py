"""Pipeline management for SuperLake."""

import pyspark.sql.functions as F
import time

class SuperPipeline:
    """Pipeline management for SuperLake."""
    def __init__(self, 
                 superlake_dt, 
                 bronze_table, 
                 silver_table, 
                 cdc_function, 
                 tra_function, 
                 logger, 
                 spark,
                 environment="dev"
                 ):
        self.superlake_dt = superlake_dt
        self.bronze_table = bronze_table
        self.silver_table = silver_table
        self.cdc_function = cdc_function
        self.tra_function = tra_function
        self.logger = logger
        self.spark = spark
        self.environment = environment


    def execute(self):

        start_time = time.time()
        self.logger.info("Starting SuperPipeline execution.")

        # 1. Run CDC function to get latest data from source
        t0 = time.time()
        cdc_df = self.cdc_function(self.spark)
        cdc_count = cdc_df.count()
        t1 = time.time()
        self.logger.info(f"CDC function completed. Rows: {cdc_count}. Duration: {t1-t0:.2f}s")
        if self.environment == "dev": 
            print("\nCDC function output:")
            cdc_df.show()

        # 2. Save to bronze
        self.bronze_table.save(
            cdc_df.withColumn("superlake_dt", F.lit(self.superlake_dt).cast("timestamp")),
            mode=str(self.bronze_table.table_save_mode.value),
            spark=self.spark
        )
        t2 = time.time()
        self.logger.info(f"Saved to bronze. Duration: {t2-t1:.2f}s")

        # 3. Filter bronze for current superlake_dt
        bronze_df = self.bronze_table.read(self.spark)
        bronze_filtered = bronze_df.filter(F.col("superlake_dt") == self.superlake_dt)
        bronze_filtered_count = bronze_filtered.count()
        t3 = time.time()
        self.logger.info(f"Filtered bronze for superlake_dt. Rows: {bronze_filtered_count}. Duration: {t3-t2:.2f}s")
        if self.environment == "dev": 
            print("\nBronze df output:")
            bronze_filtered.show()

        # 4. Apply transformation
        silver_df = self.tra_function(bronze_filtered)
        silver_count = silver_df.count()
        t4 = time.time()
        self.logger.info(f"Transformation completed. Rows: {silver_count}. Duration: {t4-t3:.2f}s")
        if self.environment == "dev": 
            print("\nSilver df output:")
            silver_df.show()

        # 5. Save to silver
        self.silver_table.save(
            silver_df.withColumn("superlake_dt", F.lit(self.superlake_dt).cast("timestamp")),
            mode=str(self.silver_table.table_save_mode.value),
            spark=self.spark
        )
        t5 = time.time()
        self.logger.info(f"Saved to silver. Duration: {t5-t4:.2f}s")
        if self.environment == "test": 
            print("\nBronze table output:")
            self.bronze_table.read(self.spark).show()
            print("\nSilver table output:")
            self.silver_table.read(self.spark).show()

        # 6. Log metrics
        total_duration = t5 - start_time
        self.logger.info(f"SuperPipeline completed. Total duration: {total_duration:.2f}s")
        self.logger.info(f"Rows ingested in bronze: {cdc_count}, rows transformed into silver: {silver_count}")
        self.logger.metric("bronze_row_count", cdc_count)
        self.logger.metric("silver_row_count", silver_count)
        self.logger.metric("total_duration_sec", round(total_duration, 2))
        self.logger.metric("cdc_duration_sec", round(t1-t0, 2))
        self.logger.metric("bronze_save_duration_sec", round(t2-t1, 2))
        self.logger.metric("bronze_filter_duration_sec", round(t3-t2, 2))
        self.logger.metric("transformation_duration_sec", round(t4-t3, 2))
        self.logger.metric("silver_save_duration_sec", round(t5-t4, 2))

class SuperGoldPipeline:
    """Gold layer pipeline for SuperLake: runs a gold_function(spark, superlake_dt) and saves to gold_table."""
    def __init__(self, gold_function, gold_table, logger, spark, superlake_dt=None, environment=None):
        self.gold_function = gold_function
        self.gold_table = gold_table
        self.logger = logger
        self.spark = spark
        self.superlake_dt = superlake_dt
        self.environment = environment

    def execute(self):
        self.logger.info("Starting SuperGoldPipeline execution.")
        gold_df = self.gold_function(self.spark, self.superlake_dt)
        self.logger.info(f"Gold function completed. Rows: {gold_df.count()}")
        self.gold_table.save(gold_df, mode='overwrite', spark=self.spark)
        self.logger.info("Saved to gold table.")
        self.logger.info("SuperGoldPipeline completed.")
        if self.environment == "test": 
            print("\nGold table output:")
            self.gold_table.read(self.spark).show()