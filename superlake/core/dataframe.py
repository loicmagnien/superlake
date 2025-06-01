import re
from typing import List, Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql import types as T
from pyspark.sql import functions as F


class SuperDataframe:
    def __init__(self, df: DataFrame, logger: Any = None) -> None:
        self.df = df
        self.logger = logger

    def clean_columns_names(self) -> DataFrame:
        """
        Cleans DataFrame column names by replacing invalid characters.
        Args:
            None
        Returns:
            DataFrame with cleaned column names.
        """
        new_cols = [re.sub(r'[^a-zA-Z0-9_]+', '_', c).strip('_') for c in self.df.columns]
        df_clean = self.df.toDF(*new_cols)
        return df_clean

    def clean_column_values(self, columns: List[str]) -> DataFrame:
        """
        Cleans DataFrame partition values by replacing invalid characters.
        Args:
            columns: List of column names to clean.
        Returns:
            DataFrame with cleaned column values.
        """
        df_clean = self.df
        for column in columns:
            df_clean = df_clean.withColumn(
                column,
                F.regexp_replace(
                    F.regexp_replace(F.col(column), r'[^a-zA-Z0-9_]', '_'),
                    r'^_+|_+$', ''
                )
            )
        return df_clean

    def cast_columns(self, schema: T.StructType) -> DataFrame:
        """
        Cast the columns of a Spark DataFrame based on provided target schema.
        Args:
            schema: StructType of the target schema.
        Returns:
            DataFrame with casted columns.
        """
        df_casted = self.df
        for field in schema.fields:
            if field.name in df_casted.columns:
                df_casted = (
                    df_casted.withColumn(field.name, F.col(field.name).cast(field.dataType))
                )
        return df_casted

    def drop_columns(self, columns: List[str]) -> DataFrame:
        """
        Drop the columns of a Spark DataFrame based on provided list of column names.
        Args:
            columns: List of column names to drop.
        Returns:
            DataFrame with dropped columns.
        """
        df_dropped = self.df.drop(*columns)
        return df_dropped

    def rename_columns(self, columns: Dict[str, str]) -> DataFrame:
        """
        Rename the columns of a Spark DataFrame based on provided dictionary of column names.
        Args:
            columns: Dictionary of column names to rename.
        Returns:
            DataFrame with renamed columns.
        """
        df_renamed = self.df.toDF(
            *[columns.get(c, c) for c in self.df.columns]
        )
        return df_renamed

    def replace_null_values(self, column_value_dict: Dict[str, Any]) -> DataFrame:
        """
        Replace the null values of a Spark DataFrame based on
        provided dictionary of column names and values.
        Args:
            column_value_dict: Dictionary of column names and values to replace null values.
        Returns:
            DataFrame with replaced null values.
        """
        df_replaced = self.df.na.fill(column_value_dict)
        return df_replaced

    def drop_duplicates(self, columns: List[str]) -> DataFrame:
        """
        Drop the duplicate rows of a Spark DataFrame based on provided list of column names.
        Args:
            columns: List of column names to drop duplicates.
        Returns:
            DataFrame with dropped duplicates.
        """
        df_dropped = self.df.dropDuplicates(subset=columns)
        return df_dropped

    def drop_null_values(self, columns: List[str]) -> DataFrame:
        """
        Drop the rows of a Spark DataFrame based on provided list of
        column names where the values are null.
        Args:
            columns: List of column names to drop null values.
        Returns:
            DataFrame with dropped null values.
        """
        df_dropped = self.df.na.drop(subset=columns)
        return df_dropped

    def distributed_pivot(
            self,
            pivoted_column: str,
            pivoted_columns_list: List[str],
            pivoted_value: str,
            pivoted_join_keys: List[str],
            pivoted_join_how: str
         ) -> DataFrame:
        """
        Distributed pivot on the DataFrame.
        Args:
            pivoted_column: The column to pivot on (str)
            pivoted_columns_list: List of values in the pivoted_column to become columns (List[str])
            pivoted_value: The column whose values fill the pivoted columns (str)
            pivoted_join_keys: List of columns to join on (List[str])
            pivoted_join_how: Join type (str, e.g. 'full', 'outer', 'left')
        Returns:
            Pivoted DataFrame

        Suppose you have the following df_to_pivot Spark DataFrame:
        +----+------------+-----+-------+
        | id | date       | key | value |
        +----+------------+-----+-------+
        | 1  | 2024-06-01 | foo | 10    |
        | 1  | 2024-06-01 | bar | 5     |
        | 1  | 2024-06-01 | nop | 97    |
        | 2  | 2024-06-01 | foo | 8     |
        | 2  | 2024-06-01 | bar | 7     |
        | 2  | 2024-06-01 | nop | 99    |
        +----+------------+-----+-------+

        If you want to pivot the key column, you can run the following:
        pivoted_df = SuperDataframe.distributed_pivot(
            pivoted_column       = "key",          # this column will become the columns in the pivoted DataFrame
            pivoted_columns_list = ["foo", "bar"], # subset or all values in the pivoted_column
            pivoted_value        = "value",        # column to pivot on
            pivoted_join_keys    = ["id", "date"], # columns to join on, usually primary keys minus the pivoted_column
            pivoted_join_how     = 'full'          # join type, e.g. 'full', 'outer', 'left'
        )

        The output will be the following pivoted_df Spark DataFrame:
        +----+------------+-----+-----+
        | id | date       | foo | bar |
        +----+------------+-----+-----+
        | 1  | 2024-06-01 | 10  | 5   |
        | 2  | 2024-06-01 | 8   | 7   |
        +----+------------+-----+-----+
        """
        if self.logger:
            self.logger.info('Running distributed Pivot and processing metrics...')
        # Pre-process
        number_rows_before_filtering = self.df.count()
        df_to_pivot = self.df.filter(F.col(pivoted_column).isin(pivoted_columns_list))
        # Pivot
        first = True
        for column in pivoted_columns_list:
            df = (
                df_to_pivot
                .filter(F.col(pivoted_column) == column)
                .withColumnRenamed(pivoted_value, column)
                .drop(pivoted_value, pivoted_column)
            )
            if first:
                pivoted_df = df
                first = False
            else:
                pivoted_df = pivoted_df.join(df, on=pivoted_join_keys, how=pivoted_join_how)
        # Metrics
        number_of_rows_to_pivot = df_to_pivot.count()
        number_of_pivoted_rows = pivoted_df.count()
        number_of_pivoted_columns = int(len(pivoted_columns_list))
        total_pivoted_values = number_of_pivoted_rows * number_of_pivoted_columns
        total_null_values_in_pivot = total_pivoted_values - number_of_rows_to_pivot
        if self.logger:
            self.logger.metric("distributed_pivot_number_rows_original", number_rows_before_filtering)
            self.logger.metric("distributed_pivot_number_rows_to_pivot", number_of_rows_to_pivot)
            self.logger.metric("distributed_pivot_number_pivoted_rows", number_of_pivoted_rows)
            self.logger.metric("distributed_pivot_number_pivoted_columns", number_of_pivoted_columns)
            self.logger.metric("distributed_pivot_number_pivoted_values", total_pivoted_values)
            self.logger.metric("distributed_pivot_number_pivoted_nulls", total_null_values_in_pivot)
        return pivoted_df

    def generate_surrogate_key(
            self,
            field_list: list,
            key_column_name: str = "surrogate_key"
            ) -> DataFrame:
        """
        Adds a surrogate key column using SHA-256 hash of specified fields.
        Returns null if any input field is null.

        Args:
            df (DataFrame): Source PySpark DataFrame
            field_list (list): List of column names to use as input
            key_column_name (str): Name of the output surrogate key column

        Returns:
            DataFrame: with an added surrogate key column
        """
        # Build null check condition
        null_condition = None
        for field in field_list:
            condition = F.col(field).isNull()
            null_condition = condition if null_condition is None else (null_condition | condition)
        # Concatenate fields for hashing
        concatenated_cols = F.concat_ws("||", *[F.col(f).cast(T.StringType()) for f in field_list])
        # Conditional SHA2 hash: null if any field is null
        df_with_key = self.df.withColumn(
            key_column_name,
            F.when(null_condition, F.lit(None)).otherwise(F.sha2(concatenated_cols, 256))
        )
        return df_with_key

    @staticmethod
    def super_union_by_name(dfs: List[DataFrame]) -> DataFrame:
        """
        Union multiple DataFrames with different schemas by aligning columns and filling missing columns with nulls.
        Promotes types for columns that appear in multiple DataFrames with different types.
        Args:
            dfs: List of PySpark DataFrames to union.
        Returns:
            A single DataFrame that is the union of all input DataFrames.
        """
        if not dfs:
            raise ValueError("No DataFrames provided for union.")
        import pyspark.sql.types as T
        # Collect all types for each column
        col_types = {}
        for df in dfs:
            for field in df.schema.fields:
                col_types.setdefault(field.name, set()).add(type(field.dataType))

        # Type promotion logic
        def promote_type(types_set):
            if T.StringType in types_set:
                return T.StringType()
            if T.DoubleType in types_set or T.FloatType in types_set:
                return T.DoubleType()
            if T.LongType in types_set or T.IntegerType in types_set or T.ShortType in types_set:
                return T.LongType()
            if T.BooleanType in types_set:
                return T.BooleanType()
            # Default: use the first type
            return list(types_set)[0]()

        promoted_types = {col: promote_type(types) for col, types in col_types.items()}
        all_columns = list(promoted_types.keys())
        # Cast and align columns for each DataFrame
        aligned_dfs = []
        for df in dfs:
            select_exprs = []
            for col in all_columns:
                if col in df.columns:
                    select_exprs.append(F.col(col).cast(promoted_types[col]).alias(col))
                else:
                    select_exprs.append(F.lit(None).cast(promoted_types[col]).alias(col))
            aligned_dfs.append(df.select(*select_exprs))
        # Union all DataFrames
        from functools import reduce
        unioned_df = reduce(DataFrame.unionByName, aligned_dfs)
        return unioned_df
