import sys
import os
import pytest
import pyspark.sql.types as T
import pyspark.sql.functions as F

# for local testing
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from superlake.core.dataframe import SuperDataframe

WAREHOUSE_DIR = "./tests/data/spark-warehouse"
EXTERNAL_PATH = "./tests/data/external-table"
CATALOG_NAME = "spark_catalog"

@pytest.fixture(scope="module")
def spark():
    from superlake.core import SuperSpark
    return SuperSpark(
        session_name="SparkSession for SuperLake",
        warehouse_dir=WAREHOUSE_DIR,
        external_path=EXTERNAL_PATH,
        catalog_name=CATALOG_NAME
    ).spark


# test_clean_columns_names
def test_clean_columns_names(spark):
    data = [(1, 'Alice'), (2, 'Bob')]
    columns = ['id#', 'Name (Full)']
    df = spark.createDataFrame(data, columns)
    sdf = SuperDataframe(df)
    cleaned_df = sdf.clean_columns_names()
    # check the count of rows is the same
    assert cleaned_df.count() == 2
    # check the columns are the same
    assert cleaned_df.columns == ['id', 'Name_Full']


# test_clean_column_values
def test_clean_column_values(spark):
    data = [
        (1, 'value__', 'value'),
        (2, 'value_2_!', 'value_2')
    ]
    columns = ['id', 'column_to_clean', 'column_cleaned']
    df = spark.createDataFrame(data, columns)
    sdf = SuperDataframe(df)
    cleaned_df = sdf.clean_column_values(columns=['column_to_clean'])
    # check the count of rows is the same
    assert cleaned_df.count() == 2
    # check the values in the column_to_clean are the same as the values in the column_cleaned
    assert cleaned_df.select(F.col("column_to_clean")).collect() == df.select(F.col("column_cleaned")).collect()


# test_cast_columns
def test_cast_columns(spark):
    data = [(1, 'Alice'), (2, 'Bob')]
    columns = ['id', 'Name']
    schema = T.StructType([
        T.StructField("id", T.IntegerType(), True),
        T.StructField("Name", T.StringType(), True)
    ])
    df = spark.createDataFrame(data, columns, schema)
    target_schema = T.StructType([
        T.StructField("id", T.StringType(), True),
        T.StructField("Name", T.StringType(), True)
    ])
    sdf = SuperDataframe(df)
    casted_df = sdf.cast_columns(target_schema)
    # check the count of rows is the same
    assert casted_df.count() == 2
    # check the schema has been casted correctly
    assert casted_df.schema == target_schema


# test_drop_columns_basic
def test_drop_columns_basic(spark):
    data = [(1, 'Alice', 10.0), (2, 'Bob', 20.0)]
    columns = ['id', 'name', 'score']
    df = spark.createDataFrame(data, columns)
    sdf = SuperDataframe(df)
    dropped_df = sdf.drop_columns(['score'])
    # check the count of rows is the same
    assert dropped_df.count() == 2
    # check the columns are the same
    assert dropped_df.columns == ['id', 'name']


# test_drop_columns_multiple
def test_drop_columns_multiple(spark):
    data = [(1, 'Alice', 10.0, 'A'), (2, 'Bob', 20.0, 'B')]
    columns = ['id', 'name', 'score', 'grade']
    df = spark.createDataFrame(data, columns)
    sdf = SuperDataframe(df)
    dropped_df = sdf.drop_columns(['score', 'grade'])
    # check the count of rows is the same
    assert dropped_df.count() == 2
    # check the columns are the same
    assert dropped_df.columns == ['id', 'name']


# test_drop_columns_non_existent
def test_drop_columns_non_existent(spark):
    data = [(1, 'Alice'), (2, 'Bob')]
    columns = ['id', 'name']
    df = spark.createDataFrame(data, columns)
    sdf = SuperDataframe(df)
    # Spark ignores non-existent columns in drop
    dropped_df = sdf.drop_columns(['age'])
    # check the count of rows is the same
    assert dropped_df.count() == 2
    # check the columns are the same
    assert dropped_df.columns == ['id', 'name']


# test_drop_columns_all
def test_drop_columns_all(spark):
    data = [(1, 'Alice')]
    columns = ['id', 'name']
    df = spark.createDataFrame(data, columns)
    sdf = SuperDataframe(df)
    dropped_df = sdf.drop_columns(['id', 'name'])
    # check the count of rows is the same
    assert dropped_df.count() == 1
    # check the columns are the same
    assert dropped_df.columns == []


# test_drop_columns_none
def test_drop_columns_none(spark):
    data = [(1, 'Alice')]
    columns = ['id', 'name']
    df = spark.createDataFrame(data, columns)
    sdf = SuperDataframe(df)
    dropped_df = sdf.drop_columns([])
    # check the count of rows is the same
    assert dropped_df.count() == 1
    # check the columns are the same
    assert dropped_df.columns == ['id', 'name']


# test_drop_columns_empty_dataframe
def test_drop_columns_empty_dataframe(spark):
    df = spark.createDataFrame([], schema=T.StructType([
        T.StructField('id', T.IntegerType(), True),
        T.StructField('name', T.StringType(), True)
    ]))
    sdf = SuperDataframe(df)
    dropped_df = sdf.drop_columns(['name'])
    # check the count of rows is the same
    assert dropped_df.count() == 0
    # check the columns are the same
    assert dropped_df.columns == ['id']


# test_drop_columns_special_characters
def test_drop_columns_special_characters(spark):
    data = [(1, 'Alice', 10.0)]
    columns = ['id', 'name', 'score#1']
    df = spark.createDataFrame(data, columns)
    sdf = SuperDataframe(df)
    dropped_df = sdf.drop_columns(['score#1'])
    # check the count of rows is the same
    assert dropped_df.count() == 1
    # check the columns are the same
    assert dropped_df.columns == ['id', 'name']


# test_rename_columns
def test_rename_columns(spark):
    data = [(1, 'Alice', 10.0, 42, 'foo', 'bar', 99)]
    columns = [
        'id',
        'name',
        'score#1',
        'weird space',
        'dollar$sign',
        'hyphen-name',
        'UPPERCASE'
    ]
    df = spark.createDataFrame(data, columns)
    sdf = SuperDataframe(df)
    renamed_df = sdf.rename_columns({
        'name': 'full_name',
        'score#1': 'score_number_one',
        'weird space': 'weird_space',
        'dollar$sign': 'dollar_sign',
        'hyphen-name': 'hyphen_name',
        'UPPERCASE': 'lowercase',
        'id': 'identifier'
    })
    assert renamed_df.columns == [
        'identifier',
        'full_name',
        'score_number_one',
        'weird_space',
        'dollar_sign',
        'hyphen_name',
        'lowercase'
    ]


# test_replace_null_values
def test_replace_null_values(spark):
    data = [
        (1, 'Alice', None),
        (2, None, 10),
        (3, 'Bob', None),
        (4, None, None)
    ]
    columns = ['id', 'name', 'score']
    df = spark.createDataFrame(data, columns)
    sdf = SuperDataframe(df)
    replaced_df = sdf.replace_null_values({'name': 'missing', 'score': 0})
    result = replaced_df.collect()
    # Check that all nulls in 'name' and 'score' are replaced by 'missing' and 0 respectively
    for row in result:
        assert row['name'] is not None
        assert row['score'] is not None


# test_drop_duplicates
def test_drop_duplicates(spark):
    data = [
        (1, 'Alice', 10),
        (2, 'Bob', 20),
        (1, 'Alice', 10),
        (3, 'Alice', 10)
    ]
    columns = ['id', 'name', 'score']
    df = spark.createDataFrame(data, columns)
    sdf = SuperDataframe(df)
    # Drop duplicates based on all columns
    dropped_df = sdf.drop_duplicates(['id', 'name', 'score'])
    assert dropped_df.count() == 3
    # Drop duplicates based on 'name' and 'score' only
    dropped_df2 = sdf.drop_duplicates(['name', 'score'])
    # Only unique (name, score) pairs remain
    assert dropped_df2.count() == 2


# test_drop_null_values
def test_drop_null_values(spark):
    data = [
        (1, 'Alice', 10),
        (2, None, 20),
        (3, 'Bob', None),
        (4, None, None)
    ]
    columns = ['id', 'name', 'score']
    df = spark.createDataFrame(data, columns)
    sdf = SuperDataframe(df)
    # Drop rows where 'name' is null
    dropped_df = sdf.drop_null_values(['name'])
    assert dropped_df.count() == 2
    # Drop rows where 'score' is null
    dropped_df2 = sdf.drop_null_values(['score'])
    assert dropped_df2.count() == 2
    # Drop rows where either 'name' or 'score' is null
    dropped_df3 = sdf.drop_null_values(['name', 'score'])
    assert dropped_df3.count() == 1


def test_super_union(spark):
    data1 = [(1, "Alice"), (2, "Bob")]
    columns1 = ["id", "name"]
    df1 = spark.createDataFrame(data1, columns1)

    data2 = [(3, "Charlie", 100), (4, "David", 200)]
    columns2 = ["id", "name", "score"]
    df2 = spark.createDataFrame(data2, columns2)

    # Only df2 has 'score', df1 does not
    unioned_df = SuperDataframe.super_union_by_name([df1, df2])

    # The resulting columns should be ['id', 'name', 'score']
    assert set(unioned_df.columns) == {"id", "name", "score"}
    # There should be 4 rows
    assert unioned_df.count() == 4

    # Rows from df1 should have score == None
    rows = unioned_df.filter(F.col("id").isin([1, 2])).collect()
    for row in rows:
        assert row["score"] is None

    # Rows from df2 should have their original score
    rows = unioned_df.filter(F.col("id").isin([3, 4])).collect()
    for row in rows:
        assert row["score"] in [100, 200]


def test_super_union_type_promotion(spark):
    import pyspark.sql.types as T

    # IntegerType and StringType -> StringType
    df1 = spark.createDataFrame([(1,)], ["value"])
    df2 = spark.createDataFrame([("2",)], ["value"])
    unioned = SuperDataframe.super_union_by_name([df1, df2])
    assert unioned.schema["value"].dataType == T.StringType()
    assert set([row["value"] for row in unioned.collect()]) == {"1", "2"}

    # IntegerType and DoubleType -> DoubleType
    df1 = spark.createDataFrame([(1,)], ["value"])
    df2 = spark.createDataFrame([(2.5,)], ["value"])
    unioned = SuperDataframe.super_union_by_name([df1, df2])
    assert unioned.schema["value"].dataType == T.DoubleType()
    values = set([row["value"] for row in unioned.collect()])
    assert values == {1.0, 2.5}

    # IntegerType and LongType -> LongType
    df1 = spark.createDataFrame([(1,)], ["value"])
    df2 = spark.createDataFrame([(2**33,)], ["value"])  # Large int, will be LongType
    unioned = SuperDataframe.super_union_by_name([df1, df2])
    assert unioned.schema["value"].dataType == T.LongType()
    values = set([row["value"] for row in unioned.collect()])
    assert values == {1, 2**33}

    # BooleanType only -> BooleanType
    df1 = spark.createDataFrame([(True,)], ["value"])
    df2 = spark.createDataFrame([(False,)], ["value"])
    unioned = SuperDataframe.super_union_by_name([df1, df2])
    assert unioned.schema["value"].dataType == T.BooleanType()
    values = set([row["value"] for row in unioned.collect()])
    assert values == {True, False}
