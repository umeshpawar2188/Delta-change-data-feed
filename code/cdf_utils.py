from pyspark.sql.functions import col, rank, current_timestamp
from pyspark.sql.window import Window

def get_last_processed_version(spark, metadata_table, table_name):
    return (
        spark.sql(f"""
            SELECT last_processed_version
            FROM {metadata_table}
            WHERE table_name = '{table_name}'
        """)
        .collect()[0][0]
    )

def update_last_processed_version(spark, metadata_table, table_name, new_version):
    spark.sql(f"""
        UPDATE {metadata_table}
        SET last_processed_version = {new_version},
            last_processed_timestamp = current_timestamp()
        WHERE table_name = '{table_name}'
    """)

def get_latest_changes(spark, source_table, start_version, key_col):
    spark.sql(f"""
        CREATE OR REPLACE TEMPORARY VIEW latest_changes AS
        SELECT * FROM (
            SELECT *, 
                rank() OVER (PARTITION BY {key_col} ORDER BY _commit_version DESC) as rank
            FROM table_changes('{source_table}', {start_version})
            WHERE _change_type != 'update_preimage'
        )
        WHERE rank = 1
    """)