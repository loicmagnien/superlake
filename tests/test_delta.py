# -------------------------------------------------------------------------------------
#                                   Imports
# -------------------------------------------------------------------------------------

import sys
import os
import shutil
import pytest
import pyspark.sql.types as T
from datetime import datetime
# fix the path to include the superlake package
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from superlake.core import SuperDeltaTable, TableSaveMode, SchemaEvolution, SuperSpark


# -------------------------------------------------------------------------------------
#                                   Fixtures
# -------------------------------------------------------------------------------------

WAREHOUSE_DIR = "./tests/data/spark-warehouse"
EXTERNAL_PATH = "./tests/data/external-table"
CATALOG_NAME = "spark_catalog"

TABLE_SCHEMA = T.StructType([
        T.StructField("id", T.IntegerType(), False),
        T.StructField("key", T.StringType(), True),
        T.StructField("value", T.StringType(), True),
        T.StructField("superlake_dt", T.TimestampType(), True)
    ])


@pytest.fixture(scope="module")
def super_spark():
    return SuperSpark(
        session_name="SparkSession for SuperLake",
        warehouse_dir=WAREHOUSE_DIR,
        external_path=EXTERNAL_PATH,
        catalog_name=CATALOG_NAME
    )


@pytest.fixture(scope="module")
def spark(super_spark):
    return super_spark.spark


@pytest.fixture
def managed_table(super_spark):
    return SuperDeltaTable(
        super_spark=super_spark,
        catalog_name=CATALOG_NAME,
        schema_name="test_schema",
        table_name="managed_table",
        table_schema=TABLE_SCHEMA,
        table_save_mode=TableSaveMode.Append,
        primary_keys=["id"],
        partition_cols=["superlake_dt"],
        managed=True,
    )


@pytest.fixture
def external_table(super_spark):
    schema = T.StructType([
        T.StructField("id", T.IntegerType(), False),
        T.StructField("key", T.StringType(), True),
        T.StructField("value", T.StringType(), True),
        T.StructField("superlake_dt", T.TimestampType(), True)
    ])
    return SuperDeltaTable(
        super_spark=super_spark,
        catalog_name=CATALOG_NAME,
        schema_name="test_schema",
        table_name="external_table",
        table_schema=schema,
        table_save_mode=TableSaveMode.Append,
        primary_keys=["id"],
        partition_cols=["superlake_dt"],
        managed=False,
    )


# remove the folders for the external table and warehouse and all subfolders
for path in [EXTERNAL_PATH, WAREHOUSE_DIR]:
    try:
        shutil.rmtree(os.path.abspath(path))
    except Exception as e:
        print(f"Exception removing folder {path}: {e}")


# ------------------------------------------------------------------------------------------------
#                                   Testing strategy
# ------------------------------------------------------------------------------------------------

#  +------+------------------------------------------------------------------------------------+
#  | Part |                                   description                                      |
#  +------+------------------------------------------------------------------------------------+
#  |  0   | globally, tests need to be executed for both managed and external tables           |
#  |  I   | check the functions that do not depend on the table existence                      |
#  |  II  | create a table and test the functions that depend on the table existence           |
#  |  III | drop the table and test the functions that depend on the table existence           |
#  |  IV  | test all the functions used in the save function                                   |
#  |  V   | test the save function with all the modes, create specific dataframe for each mode |
#  +------+------------------------------------------------------------------------------------+

# ------------------------------------------------------------------------------------------------
#                                             Data
# ------------------------------------------------------------------------------------------------

SUPERLAKE_DT = datetime.now()
TEST_DATA = [(1, "foo", "bar", SUPERLAKE_DT)]
TEST_COLUMNS = ["id", "key", "value", "superlake_dt"]
TEST_SCHEMA = T.StructType([
    T.StructField("id", T.IntegerType(), False),
    T.StructField("key", T.StringType(), True),
    T.StructField("value", T.StringType(), True),
    T.StructField("superlake_dt", T.TimestampType(), True)
])

NEW_TABLE_SCHEMA = T.StructType([
        T.StructField("id", T.IntegerType(), False),
        T.StructField("key", T.StringType(), True),
        T.StructField("value", T.StringType(), True),
        T.StructField("superlake_dt", T.TimestampType(), True),
        T.StructField("new_col", T.StringType(), True)
    ])
NEW_DATA = [(1, "foo", "bar", SUPERLAKE_DT, "new_value")]


# ------------------------------------------------------------------------------------------------
#                                             PART I
# ------------------------------------------------------------------------------------------------

# [full_table_name]
@pytest.mark.parametrize(
    "table_fixture_name,expected",
    [
        ("managed_table", f"{CATALOG_NAME}.test_schema.managed_table"),
        ("external_table", f"{CATALOG_NAME}.test_schema.external_table"),
    ],
    ids=["full_table_name_managed", "full_table_name_external"]
)
def test_full_table_name(table_fixture_name, expected, request):
    table = request.getfixturevalue(table_fixture_name)
    assert table.full_table_name() == expected


# [forname_table_name]
@pytest.mark.parametrize(
    "table_fixture_name,expected",
    [
        ("managed_table", "test_schema.managed_table"),
        ("external_table", "test_schema.external_table"),
    ],
    ids=["forname_table_name_managed", "forname_table_name_external"]
)
def test_forname_table_name(table_fixture_name, expected, request):
    table = request.getfixturevalue(table_fixture_name)
    assert table.forname_table_name() == expected


# [get_table_path]
@pytest.mark.parametrize(
    "table_fixture_name,expected",
    [
        ("managed_table", os.path.abspath(f"{WAREHOUSE_DIR}/test_schema.db/managed_table")),
        ("external_table", os.path.abspath(f"{EXTERNAL_PATH}/test_schema/external_table")),
    ],
    ids=["get_table_path_managed", "get_table_path_external"]
)
def test_get_table_path(table_fixture_name, expected, request, spark):
    table = request.getfixturevalue(table_fixture_name)
    assert table.get_table_path(spark) == expected


# [get_schema_path]
@pytest.mark.parametrize(
    "table_fixture_name,expected",
    [
        ("managed_table", os.path.abspath(f"{WAREHOUSE_DIR}/test_schema.db")),
        ("external_table", os.path.abspath(f"{EXTERNAL_PATH}/test_schema")),
    ],
    ids=["get_schema_path_managed", "get_schema_path_external"]
)
def test_get_schema_path(table_fixture_name, expected, request, spark):
    table = request.getfixturevalue(table_fixture_name)
    assert table.get_schema_path(spark) == expected

# ------------------------------------------------------------------------------------------------
#                                       PART II
# ------------------------------------------------------------------------------------------------


# [save] with append mode
@pytest.mark.parametrize("table_fixture_name", ["managed_table", "external_table"])
def test_save_append(table_fixture_name, request, spark):
    table = request.getfixturevalue(table_fixture_name)
    df = spark.createDataFrame(TEST_DATA, TEST_COLUMNS)
    table.save(df, mode="append", spark=spark)
    assert spark.sql(f"SELECT * FROM {table.full_table_name()}").count() == 1


# [register_table_in_catalog] for managed table
def test_register_table_in_catalog_managed(managed_table, spark):
    # Note: DROP TABLE on managed tables deletes the data, so running a SELECT on the table should return:
    # pyspark.errors.exceptions.captured.AnalysisException: [DELTA_READ_TABLE_WITHOUT_COLUMNS]
    # we would need to insert the data into the table, etc. already done in the test_save_append_managed
    spark.sql(f"DROP TABLE IF EXISTS {managed_table.full_table_name()}")
    assert spark.sql(f"SHOW TABLES IN {managed_table.schema_name} LIKE '{managed_table.table_name}'").count() == 0
    managed_table.ensure_table_exists(spark)
    assert spark.sql(f"SHOW TABLES IN {managed_table.schema_name} LIKE '{managed_table.table_name}'").count() == 1
    assert spark.sql(f"SELECT * FROM {managed_table.full_table_name()}").count() == 0


# as the data was deleted for the managed table when dropping, we need to re-append the data
def test_save_append_managed_after_drop(managed_table, spark):
    df = spark.createDataFrame(TEST_DATA, TEST_COLUMNS)
    managed_table.save(df, mode="append", spark=spark)
    assert spark.sql(f"SELECT * FROM {managed_table.full_table_name()}").count() == 1


# [register_table_in_catalog] for external table
def test_register_table_in_catalog_external(external_table, spark):
    # Note: DROP on external table does not delete the data
    # running the register_table_in_catalog will load the data from the external path
    spark.sql(f"DROP TABLE IF EXISTS {external_table.full_table_name()}")
    assert spark.sql(f"SHOW TABLES IN {external_table.schema_name} LIKE '{external_table.table_name}'").count() == 0
    external_table.register_table_in_catalog(spark)
    assert spark.sql(f"SHOW TABLES IN {external_table.schema_name} LIKE '{external_table.table_name}'").count() == 1
    assert spark.sql(f"SELECT * FROM {external_table.full_table_name()}").count() == 1


# [is_delta_table_path]
@pytest.mark.parametrize("table_fixture_name", ["managed_table", "external_table"])
def test_is_delta_table_path(table_fixture_name, request, spark):
    table = request.getfixturevalue(table_fixture_name)
    assert table.is_delta_table_path(spark)


# [table_exists]
@pytest.mark.parametrize("table_fixture_name", ["managed_table", "external_table"])
def test_table_exists(table_fixture_name, request, spark):
    table = request.getfixturevalue(table_fixture_name)
    assert table.table_exists(spark)


# [data_exists]
@pytest.mark.parametrize("table_fixture_name", ["managed_table", "external_table"])
def test_data_exists(table_fixture_name, request, spark):
    table = request.getfixturevalue(table_fixture_name)
    assert table.data_exists(spark)


# [schema_exists]
@pytest.mark.parametrize("table_fixture_name", ["managed_table", "external_table"])
def test_schema_exists(table_fixture_name, request, spark):
    table = request.getfixturevalue(table_fixture_name)
    assert table.schema_exists(spark)


# [schema_and_table_exists]
@pytest.mark.parametrize("table_fixture_name", ["managed_table", "external_table"])
def test_schema_and_table_exists(table_fixture_name, request, spark):
    table = request.getfixturevalue(table_fixture_name)
    assert table.schema_and_table_exists(spark)


# [ensure_schema_exists]
@pytest.mark.parametrize("table_fixture_name", ["managed_table", "external_table"])
def test_ensure_schema_exists(table_fixture_name, request, spark):
    table = request.getfixturevalue(table_fixture_name)
    table.ensure_schema_exists(spark)
    assert table.schema_exists(spark)


# [check_table_schema]
@pytest.mark.parametrize("table_fixture_name", ["managed_table", "external_table"])
def test_check_table_schema(table_fixture_name, request):
    table = request.getfixturevalue(table_fixture_name)
    assert table.check_table_schema(check_nullability=False)
    # change the schema of the table
    table.table_schema = T.StructType([
        T.StructField("id", T.StringType(), False),
        T.StructField("key", T.StringType(), True),
        T.StructField("value", T.StringType(), True),
        T.StructField("superlake_dt", T.TimestampType(), True)
    ])
    assert not table.check_table_schema(check_nullability=False)
    # change the schema back to the original one
    table.table_schema = TABLE_SCHEMA
    assert table.check_table_schema(check_nullability=False)


# [read]
@pytest.mark.parametrize("table_fixture_name", ["managed_table", "external_table"])
def test_read(table_fixture_name, request, spark):
    table = request.getfixturevalue(table_fixture_name)
    df_from_table = table.read()
    assert df_from_table.count() == 1
    assert df_from_table.collect()[0] == (1, "foo", "bar", SUPERLAKE_DT)


# [ensure_table_exists]
@pytest.mark.parametrize("table_fixture_name", ["managed_table", "external_table"])
def test_ensure_table_exists(table_fixture_name, request, spark):
    table = request.getfixturevalue(table_fixture_name)
    table.ensure_table_exists(spark)
    assert table.table_exists(spark)

# ------------------------------------------------------------------------------------------------
#                                        PART III
# ------------------------------------------------------------------------------------------------


# [drop]
@pytest.mark.parametrize("table_fixture_name", ["managed_table", "external_table"])
def test_drop_managed_external(table_fixture_name, request, spark):
    table = request.getfixturevalue(table_fixture_name)
    table.drop(spark)
    assert not table.table_exists(spark)


# [is_delta_table_path]
@pytest.mark.parametrize("table_fixture_name", ["managed_table", "external_table"])
def test_post_drop_is_delta_table_path(table_fixture_name, request, spark):
    table = request.getfixturevalue(table_fixture_name)
    assert not table.is_delta_table_path(spark)


# [data_exists]
@pytest.mark.parametrize("table_fixture_name", ["managed_table", "external_table"])
def test_post_drop_data_exists(table_fixture_name, request, spark):
    table = request.getfixturevalue(table_fixture_name)
    assert not table.data_exists(spark)


# [schema_exists]
@pytest.mark.parametrize("table_fixture_name", ["managed_table", "external_table"])
def test_post_drop_schema_exists(table_fixture_name, request, spark):
    table = request.getfixturevalue(table_fixture_name)
    # the schema should still exist because the DROP TABLE does not delete the schema
    assert table.schema_exists(spark)


# [schema_and_table_exists]
@pytest.mark.parametrize("table_fixture_name", ["managed_table", "external_table"])
def test_post_drop_schema_and_table_exists(table_fixture_name, request, spark):
    table = request.getfixturevalue(table_fixture_name)
    assert not table.schema_and_table_exists(spark)


# [ensure_schema_exists]
@pytest.mark.parametrize("table_fixture_name", ["managed_table", "external_table"])
def test_post_drop_ensure_schema_exists(table_fixture_name, request, spark):
    table = request.getfixturevalue(table_fixture_name)
    # the schema should still exist because the DROP TABLE does not delete the schema
    assert table.schema_exists(spark)


# [check_table_schema]
@pytest.mark.parametrize("table_fixture_name", ["managed_table", "external_table"])
def test_post_drop_check_table_schema(table_fixture_name, request):
    table = request.getfixturevalue(table_fixture_name)
    assert not table.check_table_schema(check_nullability=False)


# post drop [read] with error handling for managed table
def test_post_drop_read_managed(managed_table, spark):
    try:
        managed_table.read().count()
    except Exception as e:
        assert ("TABLE_OR_VIEW_NOT_FOUND") in str(e)


# post drop [read] with error handling for external table
def test_post_drop_read_external(external_table, spark):
    try:
        external_table.read().count()
    except Exception as e:
        assert ("PATH_NOT_FOUND") in str(e)


# ------------------------------------------------------------------------------------------------
#                                        PART IV
# ------------------------------------------------------------------------------------------------


# [evolve_schema_if_needed]
@pytest.mark.parametrize("table_fixture_name", ["managed_table", "external_table"])
def test_evolve_schema_if_needed(table_fixture_name, request, spark):
    table = request.getfixturevalue(table_fixture_name)
    # first we create a table with the original schema
    table.drop(spark)
    df = spark.createDataFrame(TEST_DATA, TEST_COLUMNS)
    table.save(df, mode="append", spark=spark)
    # change the schema evolution option of the table
    table.schema_evolution_option = SchemaEvolution.Merge
    new_df = spark.createDataFrame(NEW_DATA, schema=NEW_TABLE_SCHEMA)
    table.evolve_schema_if_needed(new_df, spark)
    print(table.get_delta_table(spark).toDF().schema)
    assert not table.check_table_schema(check_nullability=False)
    # change the schema attribute of the table to run the check_table_schema
    table.table_schema = NEW_TABLE_SCHEMA
    assert table.check_table_schema(check_nullability=False)


# [align_df_to_table_schema]
@pytest.mark.parametrize("table_fixture_name", ["managed_table", "external_table"])
def test_align_df_to_table_schema(table_fixture_name, request, spark):
    table = request.getfixturevalue(table_fixture_name)
    # create a table with the original schema
    table.drop(spark)
    df = spark.createDataFrame(TEST_DATA, TEST_COLUMNS)
    table.save(df, mode="append", spark=spark)
    # create a new dataframe with the new schema
    new_df = spark.createDataFrame(NEW_DATA, schema=NEW_TABLE_SCHEMA)
    # do not align the dataframe to the table schema
    table.schema_evolution_option = SchemaEvolution.Merge
    aligned_df = table.align_df_to_table_schema(new_df, spark)
    assert aligned_df.count() == 1
    assert not aligned_df.collect()[0].asDict() == dict(zip(df.columns, NEW_DATA[0]))
    # align the dataframe to the table schema
    table.schema_evolution_option = SchemaEvolution.Keep
    aligned_df = table.align_df_to_table_schema(new_df, spark)
    assert aligned_df.count() == 1
    assert aligned_df.collect()[0].asDict() == dict(zip(df.columns, NEW_DATA[0]))


# [get_delta_table]
@pytest.mark.parametrize("table_fixture_name", ["managed_table", "external_table"])
def test_get_delta_table(table_fixture_name, request, spark):
    table = request.getfixturevalue(table_fixture_name)
    table.drop(spark)
    try:
        table.get_delta_table(spark)
    except Exception as e:
        assert "DELTA_MISSING_DELTA_TABLE" in str(e)
    df = spark.createDataFrame(TEST_DATA, TEST_COLUMNS)
    table.save(df, mode="append", spark=spark)
    assert table.get_delta_table(spark) is not None


# [write_df]
@pytest.mark.parametrize("table_fixture_name", ["managed_table", "external_table"])
def test_write_df(table_fixture_name, request, spark):
    table = request.getfixturevalue(table_fixture_name)
    table.drop(spark)
    df = spark.createDataFrame(TEST_DATA, TEST_COLUMNS)
    table.save(df, mode="append", spark=spark)
    table.schema_evolution_option = SchemaEvolution.Merge
    merge_schema = table.schema_evolution_option == SchemaEvolution.Merge
    table.write_df(df, mode="append", merge_schema=merge_schema, overwrite_schema=False)
    assert table.get_delta_table(spark).toDF().count() == 2


# [get_merge_condition_and_updates]
@pytest.mark.parametrize("table_fixture_name", ["managed_table", "external_table"])
def test_get_merge_condition_and_updates(table_fixture_name, request, spark):
    table = request.getfixturevalue(table_fixture_name)
    df = spark.createDataFrame(TEST_DATA, TEST_COLUMNS)
    cond, updates, change_cond = table.get_merge_condition_and_updates(df)
    assert cond == "target.id=source.id"
    assert updates == {"id": "source.id", "key": "source.key", "value": "source.value", "superlake_dt": "source.superlake_dt"}
    assert change_cond == "target.key <> source.key OR target.value <> source.value OR target.superlake_dt <> source.superlake_dt"
    # for SCD type table
    table.table_save_mode = TableSaveMode.MergeSCD
    cond, updates, change_cond = table.get_merge_condition_and_updates(df, scd_change_cols=["key"])
    assert cond == "target.id=source.id"
    assert updates == {"id": "source.id", "key": "source.key", "value": "source.value", "superlake_dt": "source.superlake_dt"}
    assert change_cond == "target.key <> source.key"


# [post_drop_read]
@pytest.mark.parametrize("table_fixture_name", ["managed_table", "external_table"])
def test_post_drop_read(table_fixture_name, request, spark):
    table = request.getfixturevalue(table_fixture_name)
    try:
        table.read().count()
    except Exception as e:
        if table_fixture_name == "managed_table":
            assert ("TABLE_OR_VIEW_NOT_FOUND") in str(e)
        else:
            assert ("PATH_NOT_FOUND") in str(e)


# ------------------------------------------------------------------------------------------------
#                              PART V : Save Function will all modes
# ------------------------------------------------------------------------------------------------

# ----------------------
# Example timestamps
# ----------------------
EXAMPLE_DT1 = datetime(2024, 1, 1, 12, 0, 0)
EXAMPLE_DT2 = datetime(2024, 1, 2, 12, 0, 0)
EXAMPLE_DT3 = datetime(2024, 1, 3, 12, 0, 0)

# ----------------------
# Single PK Schemas & Data
# ----------------------
SINGLE_PK_SCHEMA = T.StructType([
    T.StructField("id", T.IntegerType(), False),
    T.StructField("key", T.StringType(), True),
    T.StructField("value", T.StringType(), True),
    T.StructField("superlake_dt", T.TimestampType(), True)
])

SINGLE_PK_DATA = [
    (1, "foo", "bar", EXAMPLE_DT1),
    (2, "baz", "qux", EXAMPLE_DT2),
]

# For schema evolution: add a new column
SINGLE_PK_SCHEMA_EVOLVED = T.StructType([
    T.StructField("id", T.IntegerType(), False),
    T.StructField("key", T.StringType(), True),
    T.StructField("value", T.StringType(), True),
    T.StructField("superlake_dt", T.TimestampType(), True),
    T.StructField("new_col", T.StringType(), True)
])

SINGLE_PK_DATA_EVOLVED = [
    (1, "foo", "bar", EXAMPLE_DT1, "extra1"),
    (2, "baz", "qux", EXAMPLE_DT2, "extra2"),
]

# ----------------------
# Composite PK Schemas & Data
# ----------------------
COMPOSITE_PK_SCHEMA = T.StructType([
    T.StructField("id", T.IntegerType(), False),
    T.StructField("sub_id", T.IntegerType(), False),
    T.StructField("key", T.StringType(), True),
    T.StructField("value", T.StringType(), True),
    T.StructField("superlake_dt", T.TimestampType(), True)
])

COMPOSITE_PK_DATA = [
    (1, 10, "foo", "bar", EXAMPLE_DT1),
    (1, 20, "baz", "qux", EXAMPLE_DT2),
]

COMPOSITE_PK_SCHEMA_EVOLVED = T.StructType([
    T.StructField("id", T.IntegerType(), False),
    T.StructField("sub_id", T.IntegerType(), False),
    T.StructField("key", T.StringType(), True),
    T.StructField("value", T.StringType(), True),
    T.StructField("superlake_dt", T.TimestampType(), True),
    T.StructField("new_col", T.StringType(), True)
])

COMPOSITE_PK_DATA_EVOLVED = [
    (1, 10, "foo", "bar", EXAMPLE_DT1, "extra1"),
    (1, 20, "baz", "qux", EXAMPLE_DT2, "extra2"),
]

# ----------------------
# SCD Schemas & Data (single PK)
# ----------------------
SCD_SCHEMA = T.StructType([
    T.StructField("id", T.IntegerType(), False),
    T.StructField("key", T.StringType(), True),
    T.StructField("value", T.StringType(), True),
    T.StructField("superlake_dt", T.TimestampType(), True),
    T.StructField("scd_start_dt", T.TimestampType(), True),
    T.StructField("scd_end_dt", T.TimestampType(), True),
    T.StructField("scd_is_current", T.BooleanType(), True),
])

SCD_DATA_ALLCOLS = [
    (1, "foo", "bar", EXAMPLE_DT1, EXAMPLE_DT1, None, True),
    (2, "baz", "qux", EXAMPLE_DT2, EXAMPLE_DT2, None, True),
]

# SCD with subset of change columns (e.g., only 'key')
SCD_DATA_SUBSETCOLS = [
    (1, "foo", "bar", EXAMPLE_DT1, EXAMPLE_DT1, None, True),
    (2, "baz", "qux", EXAMPLE_DT2, EXAMPLE_DT2, None, True),
]
# scd_change_cols = ["key"]

# SCD with no explicit change columns (should default to all non-PK, non-SCD cols)
SCD_DATA_NOCOL = SCD_DATA_ALLCOLS
# scd_change_cols = []

# ----------------------
# SCD Schemas & Data (composite PK)
# ----------------------
SCD_COMPOSITE_SCHEMA = T.StructType([
    T.StructField("id", T.IntegerType(), False),
    T.StructField("sub_id", T.IntegerType(), False),
    T.StructField("key", T.StringType(), True),
    T.StructField("value", T.StringType(), True),
    T.StructField("superlake_dt", T.TimestampType(), True),
    T.StructField("scd_start_dt", T.TimestampType(), True),
    T.StructField("scd_end_dt", T.TimestampType(), True),
    T.StructField("scd_is_current", T.BooleanType(), True),
])

SCD_COMPOSITE_DATA_ALLCOLS = [
    (1, 10, "foo", "bar", EXAMPLE_DT1, EXAMPLE_DT1, None, True),
    (1, 20, "baz", "qux", EXAMPLE_DT2, EXAMPLE_DT2, None, True),
]

SCD_COMPOSITE_DATA_SUBSETCOLS = [
    (1, 10, "foo", "bar", EXAMPLE_DT1, EXAMPLE_DT1, None, True),
    (1, 20, "baz", "qux", EXAMPLE_DT2, EXAMPLE_DT2, None, True),
]
# scd_change_cols = ["key"]

SCD_COMPOSITE_DATA_NOCOL = SCD_COMPOSITE_DATA_ALLCOLS
# scd_change_cols = []


# ------------------------------------------------------------------------------------------------
#           Paramtrized tests for [save] with all modes and schema evolution options
# ------------------------------------------------------------------------------------------------
@pytest.mark.parametrize("table_type", ["managed", "external"])
@pytest.mark.parametrize("schema_evolution_option", [
    SchemaEvolution.Overwrite, SchemaEvolution.Merge, SchemaEvolution.Keep
    ])
@pytest.mark.parametrize("pk_type", ["single", "composite"])
@pytest.mark.parametrize("save_mode", ["append", "overwrite", "merge"])
def test_save_modes_and_schema_evolution(super_spark, table_type, schema_evolution_option, pk_type, save_mode):
    # Select schema/data based on PK type
    if pk_type == "single":
        schema = SINGLE_PK_SCHEMA
        evolved_schema = SINGLE_PK_SCHEMA_EVOLVED
        data = SINGLE_PK_DATA
        evolved_data = SINGLE_PK_DATA_EVOLVED
        primary_keys = ["id"]
        partition_cols = ["superlake_dt"]
    else:
        schema = COMPOSITE_PK_SCHEMA
        evolved_schema = COMPOSITE_PK_SCHEMA_EVOLVED
        data = COMPOSITE_PK_DATA
        evolved_data = COMPOSITE_PK_DATA_EVOLVED
        primary_keys = ["id", "sub_id"]
        partition_cols = ["superlake_dt"]

    TableClass = SuperDeltaTable
    table = TableClass(
        super_spark=super_spark,
        catalog_name=CATALOG_NAME,
        schema_name="test_schema",
        table_name=f"{table_type}_{save_mode}_{schema_evolution_option.value}_{pk_type}",
        table_schema=schema,
        table_save_mode=TableSaveMode[save_mode.capitalize()],
        primary_keys=primary_keys,
        partition_cols=partition_cols,
        managed=(table_type == "managed"),
        schema_evolution_option=schema_evolution_option,
    )
    spark = super_spark.spark
    table.drop(spark)

    # Initial save
    df = spark.createDataFrame(data, schema)
    table.save(df, mode=save_mode, spark=spark)
    assert table.get_delta_table(spark).toDF().count() == len(data)

    # Evolve schema and save again (for mergeSchema/overwriteSchema)
    if schema_evolution_option == SchemaEvolution.Overwrite and save_mode != "overwrite":
        pytest.skip(
            "overwriteSchema is only supported with overwrite mode, Delta Lake only allows schema "
            "overwrite when you are overwriting the table, not when appending or merging."
        )
    if schema_evolution_option == SchemaEvolution.Merge and save_mode == "overwrite":
        pytest.skip(
            "mergeSchema is not supported with overwrite mode, merging schema (adding new columns) "
            "is only supported when appending or merging, not when overwriting."
        )
    if (
        table_type == "managed"
        and (
            (save_mode == "append" and schema_evolution_option == SchemaEvolution.Merge)
            or (save_mode == "overwrite" and schema_evolution_option == SchemaEvolution.Overwrite)
            or (save_mode == "merge" and schema_evolution_option == SchemaEvolution.Merge)
        )
    ):
        pytest.skip(
            "Skipping known failing managed table schema evolution cases due to Github CI."
        )
    if schema_evolution_option in (SchemaEvolution.Merge, SchemaEvolution.Overwrite):
        evolved_df = spark.createDataFrame(evolved_data, evolved_schema)
        table.table_schema = evolved_schema
        table.save(evolved_df, mode=save_mode, spark=spark)
        # Should have only the evolved data if overwrite, or both if append/merge
        if save_mode == "overwrite":
            expected_count = len(evolved_data)
        elif save_mode == "merge":
            # If PKs match, only updates happen, so row count stays the same
            expected_count = len(data)
        else:
            expected_count = len(data) + len(evolved_data)
        assert table.get_delta_table(spark).toDF().count() == expected_count

    # Check if the new column is present in the result schema
    result_schema = table.get_delta_table(spark).toDF().schema
    if "new_col" in evolved_schema.fieldNames():
        if schema_evolution_option != SchemaEvolution.Keep:
            assert "new_col" in result_schema.fieldNames()
        else:
            assert "new_col" not in result_schema.fieldNames()


# ------------------------------------------------------------------------------------------------
#           Paramtrized tests for [save] with mode merge_scd
# ------------------------------------------------------------------------------------------------
@pytest.mark.parametrize("table_type", ["managed", "external"])
@pytest.mark.parametrize("schema_evolution_option", [
    SchemaEvolution.Overwrite, SchemaEvolution.Merge
])
@pytest.mark.parametrize("pk_type", ["single", "composite"])
@pytest.mark.parametrize("scd_change_cols, data, schema", [
    (None, SCD_DATA_ALLCOLS, SCD_SCHEMA),
    (["key"], SCD_DATA_SUBSETCOLS, SCD_SCHEMA),
    ([], SCD_DATA_NOCOL, SCD_SCHEMA),
])
def test_save_merge_scd(super_spark, table_type, schema_evolution_option, pk_type, scd_change_cols, data, schema):
    # Select PKs and schema for composite
    if pk_type == "single":
        primary_keys = ["id"]
        partition_cols = ["superlake_dt"]
        scd_schema = SCD_SCHEMA
        scd_data = data
    else:
        primary_keys = ["id", "sub_id"]
        partition_cols = ["superlake_dt"]
        scd_schema = SCD_COMPOSITE_SCHEMA
        if data is SCD_DATA_ALLCOLS:
            scd_data = SCD_COMPOSITE_DATA_ALLCOLS
        elif data is SCD_DATA_SUBSETCOLS:
            scd_data = SCD_COMPOSITE_DATA_SUBSETCOLS
        else:
            scd_data = SCD_COMPOSITE_DATA_NOCOL

    TableClass = SuperDeltaTable
    table = TableClass(
        super_spark=super_spark,
        catalog_name=CATALOG_NAME,
        schema_name="test_schema",
        table_name=f"{table_type}_merge_scd_{schema_evolution_option.value}_{pk_type}",
        table_schema=scd_schema,
        table_save_mode=TableSaveMode.MergeSCD,
        primary_keys=primary_keys,
        partition_cols=partition_cols,
        managed=(table_type == "managed"),
        schema_evolution_option=schema_evolution_option,
        scd_change_cols=scd_change_cols,
    )
    spark = super_spark.spark
    table.drop(spark)

    df = spark.createDataFrame(scd_data, scd_schema)
    table.save(df, mode="merge_scd", spark=spark)
    # All rows should be current
    result = table.get_delta_table(spark).toDF().filter("scd_is_current = true")
    assert result.count() == len(scd_data)
