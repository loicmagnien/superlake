from pyspark.sql import DataFrame
from pyspark.sql import types as T
from pyspark.sql import functions as F
import re
from typing import List, Dict, Any

class SuperDataframe:
    def __init__(self, df: DataFrame):
        self.df = df
    
    
    def clean_columns_names(self) -> DataFrame:
        """Cleans DataFrame column names by replacing invalid characters."""
        new_cols = [re.sub(r'[^a-zA-Z0-9_]+', '_', c).strip('_') for c in self.df.columns]
        df_clean = self.df.toDF(*new_cols)
        return df_clean
    

    def clean_column_values(self, columns: List[str]) -> DataFrame:
        """Cleans DataFrame partition values by replacing invalid characters."""
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
        """Cast the columns of a Spark DataFrame based on provided target schema."""
        df_casted = self.df
        for field in schema.fields:
            if field.name in df_casted.columns:
                df_casted = df_casted.withColumn(field.name, F.col(field.name).cast(field.dataType))
        return df_casted 
    

    def drop_columns(self, columns: List[str]) -> DataFrame:
        """Drop the columns of a Spark DataFrame based on provided list of column names."""
        df_dropped = self.df.drop(*columns)
        return df_dropped
    
    
    def rename_columns(self, columns: Dict[str, str]) -> DataFrame:
        """Rename the columns of a Spark DataFrame based on provided dictionary of column names."""
        df_renamed = self.df.toDF(*[columns.get(c, c) for c in self.df.columns])
        return df_renamed
    
    
    def replace_null_values(self, columns: List[str], value: any) -> DataFrame:
        """Replace the null values of a Spark DataFrame based on provided list of column names and value."""
        df_replaced = self.df.na.fill(value, subset=columns)
        return df_replaced
    
    
    def drop_duplicates(self, columns: List[str]) -> DataFrame:
        """Drop the duplicate rows of a Spark DataFrame based on provided list of column names."""
        df_dropped = self.df.dropDuplicates(subset=columns)
        return df_dropped
    

    def drop_null_values(self, columns: List[str]) -> DataFrame:
        """Drop the rows of a Spark DataFrame based on provided list of column names where the values are null."""
        df_dropped = self.df.na.drop(subset=columns)
        return df_dropped   
    
    
    
    
    
    
    
    
    
    
