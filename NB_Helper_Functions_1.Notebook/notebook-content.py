# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7e8c8353-7a1a-4234-aa14-9635a68020c9",
# META       "default_lakehouse_name": "metadata_lakehouse",
# META       "default_lakehouse_workspace_id": "ac6fdc87-ceee-40c4-bc75-8d80fccab569",
# META       "known_lakehouses": [
# META         {
# META           "id": "7e8c8353-7a1a-4234-aa14-9635a68020c9"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # NB_Helper_Functions_1 - Core Data Engineering Utilities
# 
# ## Overview
# This notebook provides the primary suite of reusable helper functions for the Fabric Data Platform Accelerator. These functions are designed to streamline data engineering workflows, enforce best practices, and ensure robust, scalable, and maintainable pipelines across all medallion architecture layers (Bronze, Silver, Gold).
# 
# ### Key Functional Areas
# - **Surrogate Key Management**: Automated generation and assignment of surrogate keys for dimension tables.
# - **Foreign Key Integration**: Linking fact tables to dimensions using surrogate keys.
# - **Data Cleansing**: Standardization of column names, null handling, and duplicate removal.
# - **Metadata Enrichment**: Addition of system columns for audit, lineage, and change tracking.
# - **Data Quality Validation**: Configurable checks with support for warnings, quarantining, and failures.
# - **Transformation Pipeline**: Metadata-driven and custom transformation logic.
# - **Merge Operations**: Advanced merge patterns, including SCD Type 2 and partitioned overwrites.
# - **Table Management**: Dynamic DDL generation and Delta table optimization.
# - **Utility Functions**: Schema comparison, metadata handling, and API interactions.
# 
# > **Note:** All functions are designed for modularity, reusability, and performance in distributed Spark environments.


# CELL ********************

# Import all required libraries for data engineering, Spark, and Delta Lake operations.
# Each import is grouped and commented for clarity.

# --- Standard Python Libraries ---
import ast           # For safely evaluating Python literals from strings
import json          # For JSON serialization/deserialization
from datetime import timezone, timedelta, datetime, date # For date and time operations
import os            # For OS-level operations
import re            # For regular expressions
import sys           # For system-specific parameters and functions
import time          # For time-related operations (sleep, timing)

# --- PySpark and Delta Lake Libraries ---
from pyspark import errors
from io import StringIO
from delta import DeltaTable
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.window import Window, WindowSpec
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    NullType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    TimestampNTZType
)
from pyspark.sql import functions as f
from pyspark.sql import DataFrame

# --- PySpark ML Libraries (for entity resolution) ---
from pyspark.ml.feature import Tokenizer, HashingTF, MinHashLSH, NGram

# --- Additional Utilities ---
import uuid          # For generating unique identifiers
import pandas as pd  # For DataFrame operations outside Spark
import glob          # For file pattern matching
import importlib.util
from pathlib import Path
from decimal import Decimal
from typing import List, Optional, Union, Tuple, Any, Dict

# Core Python libraries
import requests                  # HTTP operations for API calls
import struct                    # Binary data operations
import pyodbc                    # Database connectivity
import hashlib                   # MD5 hashing for schema comparison

# Import libaries for Fabric Warehouse operations
import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. Surrogate Key, Foreign Key, and Null Row Functions
# 
# These functions automate the creation and management of surrogate keys for dimension tables, establish foreign key relationships in fact tables, and ensure referential integrity by inserting standardized "unknown" member rows. This is essential for robust star schema modeling and analytics.

# CELL ********************

def _sk_prepare_existing_data(
    first_run: bool,
    new_data: bool,
    target_table_name: str
):
    """Load existing dimension data from target table."""
    if new_data and not first_run:
        prior = spark.sql(f"SELECT * FROM {target_table_name}")
    else:
        prior = None
    return prior

def _calculate_base_key(
    prior,
    dimension_table_key_column_name: str,
    start_at: int
) -> int:
    """Compute max existing key (or baseline from start_at)."""
    if prior is not None:
        max_sk = prior.agg(f.max(f.col(dimension_table_key_column_name)).alias("m")).select("m").first()[0]
        base = max(start_at - 1, int(max_sk) if max_sk is not None else start_at - 1)
    else:
        base = start_at - 1
    return base

def _prepare_prior_keys(
    prior,
    primary_keys: list,
    dimension_table_key_column_name: str
):
    """Prepare existing key mappings with SCD2 filtering and validation."""
    if prior is not None:
        # For SCD2 dimensions, only consider currently active records for key mapping
        if "scd_active" in prior.columns:
            prior = prior.filter(f.col("scd_active") == 1)

        prior = prior.select(*(primary_keys + [dimension_table_key_column_name]))

        # Ensure uniqueness in prior after SCD2 filtering to avoid fan-out joins
        prior_dup_cnt = (
            prior
            .groupBy(*primary_keys)
            .count()
            .filter(f.col("count") > 1)
            .limit(1)
            .count()
        )
        if prior_dup_cnt > 0:
            raise Exception(
                "Multiple active rows for the primary keys detected in existing data. "
                "Ensure at most one active row per key before assigning surrogate keys."
            )
    
    return prior

def _validate_input_duplicates(
    df,
    primary_keys: list
) -> None:
    """Guard: duplicates in incoming df on primary_keys."""
    dupes = df.groupBy(*primary_keys).count().filter(f.col("count") > 1)
    if dupes.take(1):
        raise Exception(f"Duplicate business keys detected on {primary_keys}. Deduplicate upstream before assigning surrogate keys.")

def _join_with_existing_keys(
    df,
    prior,
    primary_keys: list,
    dimension_table_key_column_name: str
):
    """Perform null-safe join to bring in existing surrogate keys."""
    if prior is not None:
        left_df = df.alias("left")
        right_df = prior.alias("right")
        join_cond = None
        for k in primary_keys:
            cond_k = f.col(f"left.{k}").eqNullSafe(f.col(f"right.{k}"))
            join_cond = cond_k if join_cond is None else (join_cond & cond_k)

        # Select all original left columns plus the surrogate key from the right
        left_cols = [f.col(f"left.{c}") for c in df.columns]
        right_sk = f.col(f"right.{dimension_table_key_column_name}").alias(dimension_table_key_column_name)
        joined = left_df.join(right_df, join_cond, "left").select(*left_cols, right_sk)
    else:
        joined = df.withColumn(dimension_table_key_column_name, f.lit(None).cast("int"))

    return joined

def _assign_new_keys(
    joined,
    new_data: bool,
    primary_keys: list,
    dimension_table_key_column_name: str,
    base: int
):
    """Assign new keys only to rows missing a surrogate key."""
    new_rows = joined.filter(f.col(dimension_table_key_column_name).isNull())
    if new_data:
        w = Window.orderBy(*[f.col(c) for c in primary_keys])
        new_rows_with_sk = (
            new_rows
            .withColumn("__rn", f.row_number().over(w))
            .withColumn(dimension_table_key_column_name, f.col("__rn") + f.lit(base))
            .drop("__rn")
        )
        # Keep the rows that already had a key and union the newly assigned
        assigned = joined.filter(f.col(dimension_table_key_column_name).isNotNull())
        out = assigned.unionByName(new_rows_with_sk, allowMissingColumns = True)
    else:
        out = joined
    
    return out

def _reorder_columns_with_sk_first(
    df: DataFrame,
    dimension_table_key_column_name: str
):
    """Put surrogate key first for convenience and ensure delta statistics run on column."""
    cols = [dimension_table_key_column_name] + [c for c in df.columns if c != dimension_table_key_column_name]
    return df.select(*cols)

def surr_keys_dim(
    df,
    target_table_name: str,
    primary_keys: list,
    dimension_table_key_column_name: str,
    first_run: bool
):
    """
    Generate and assign surrogate keys for dimension table records.

    This function implements a robust surrogate key generation strategy that:
    - Preserves existing surrogate keys for unchanged records
    - Assigns new sequential keys for new/modified records
    - Handles both initial loads and incremental updates
    - Supports SCD2 dimensions by considering only active records

    Args:
        df (DataFrame): Input DataFrame containing dimension records to process
        target_table_name (str): Fully qualified name of the target dimension table
        primary_keys (list): List of column names forming the natural/business key
        dimension_table_key_column_name (str): Name of the surrogate key column
        first_run (bool): Flag indicating if this is the initial table load

    Returns:
        DataFrame: Enhanced DataFrame with surrogate keys assigned and positioned as first column

    Raises:
        Exception: If duplicate primary keys are detected in the input data
    """

    start_at = 1

    # Determine if there are rows to process
    new_data = df.isEmpty() == False

    # Handle incremental load - retrieve existing surrogate key mappings
    prior = _sk_prepare_existing_data(
        first_run = first_run, 
        new_data = new_data, 
        target_table_name = target_table_name
    )
        
    if not primary_keys:
        raise Exception("primary_keys is required and cannot be empty.")
        
    # Compute max existing key (or baseline from start_at)
    base = _calculate_base_key(
        prior = prior, 
        dimension_table_key_column_name = dimension_table_key_column_name, 
        start_at = start_at
    )
    
    # Prepare prior keys with SCD2 filtering and validation
    prior = _prepare_prior_keys(
        prior = prior, 
        primary_keys = primary_keys, 
        dimension_table_key_column_name = dimension_table_key_column_name
    )

    # Validate input for duplicates
    if prior is not None:
        _validate_input_duplicates(df = df, primary_keys = primary_keys)

    # Left-join to bring in any existing keys
    joined = _join_with_existing_keys(
        df = df, 
        prior = prior, 
        primary_keys = primary_keys, 
        dimension_table_key_column_name = dimension_table_key_column_name
    )

    # Assign new keys only to rows missing a surrogate key
    df = _assign_new_keys(
        joined = joined, 
        new_data = new_data, 
        primary_keys = primary_keys, 
        dimension_table_key_column_name = dimension_table_key_column_name, 
        base = base
    )

    # Put surrogate key first for convenience and ensure delta statistics run on column
    return _reorder_columns_with_sk_first(
        df = df, 
        dimension_table_key_column_name = dimension_table_key_column_name
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def _extract_surrogate_key_configuration(
    surrogate_key_logic: dict, 
    dimension_table_key_column_name: str
) -> dict:
    """Extract and process surrogate key configuration."""
    dimension_table_name = surrogate_key_logic['dimension_table_name']
    dimension_join_logic = surrogate_key_logic['dimension_table_join_logic']
    
    dimension_join_logic = dimension_join_logic.replace('=', ' = ') \
                                        .replace('< = >', ' <=> ') \
                                        .replace('> = ', ' >= ') \
                                        .replace('< = ', ' <= ')

    dimension_table_key_column_name = surrogate_key_logic.get('dimension_table_key_column_name', dimension_table_key_column_name)

    dimension_key_output_column_name = surrogate_key_logic['dimension_key_output_column_name']
    
    dimension_columns_to_add_to_fact = surrogate_key_logic.get('dimension_columns_to_add_to_fact', [])
    
    return {
        'dimension_table_name': dimension_table_name,
        'dimension_join_logic': dimension_join_logic,
        'dimension_table_key_column_name': dimension_table_key_column_name,
        'dimension_key_output_column_name': dimension_key_output_column_name,
        'dimension_columns_to_add_to_fact': dimension_columns_to_add_to_fact
    }

def _setup_temp_views(
    df: DataFrame, 
    dimension_table_name: str
) -> DataFrame:
    """Register temp views for SQL join operations."""
    df.createOrReplaceTempView("new_data_view")
    dimension_table = spark.sql(f"select * from {dimension_table_name}")
    
    if "scd_active" in dimension_table.columns:
        dimension_table = dimension_table.filter(f.col("scd_active") == 1)
    dimension_table.createOrReplaceTempView("dim_view")
    
    return dimension_table

def _extract_dimension_columns(
    dimension_join_logic: str
) -> str:
    """Extract dimension column names from join logic."""
    pattern = r'(b\.`[^`]+`|b\.[a-zA-Z0-9_\-]+)'
    dimension_columns = re.findall(pattern, dimension_join_logic, re.IGNORECASE)
    dimension_columns = [re.sub(r'(?i)^b\.', '', match) for match in dimension_columns]
    join_columns = ", ".join(dimension_columns)

    extracted_join_columns_log_info = f"Extracted dimension join columns are: {join_columns}."
    log_and_print(extracted_join_columns_log_info)

    return join_columns

def _prepare_additional_columns(
    dimension_columns_to_add_to_fact: list
) -> str:
    """Prepare additional dimension columns for fact table."""
    if dimension_columns_to_add_to_fact:
        adding_columns_log_info = f'Adding the following columns, {dimension_columns_to_add_to_fact}, from the dimension table to the fact table.'
        log_and_print(adding_columns_log_info)
        
        dimension_columns_to_add_to_fact = dimension_columns_to_add_to_fact.split(",")
        dimension_columns_to_add_to_fact = [f"b.`{col.strip()}`" for col in dimension_columns_to_add_to_fact]
        dimension_columns_to_add_to_fact_sql = ', ' + ', '.join(dimension_columns_to_add_to_fact)
    else:
        dimension_columns_to_add_to_fact_sql = ""
    
    return dimension_columns_to_add_to_fact_sql

def _validate_join_uniqueness(
    dimension_table_name: str, 
    join_columns: str, 
    dimension_join_logic: str
) -> None:
    """Validate uniqueness of join keys in dimension table."""
    duplicate_join_values = spark.sql(f"SELECT {join_columns} FROM {dimension_table_name} GROUP BY {join_columns} HAVING COUNT(*) > 1")
    if duplicate_join_values.count() > 0:
        display(duplicate_join_values)
        raise Exception(f"Many to many mappings for {dimension_join_logic} on this Dimension: {dimension_table_name}")

def _execute_surrogate_key_join(
    dimension_table_key_column_name: str,
    dimension_key_output_column_name: str,
    dimension_columns_to_add_to_fact_sql: str, 
    dimension_join_logic: str
) -> DataFrame:
    """Execute the join to attach surrogate keys."""
    query = f"""    SELECT      a.*, b.{dimension_table_key_column_name} as {dimension_key_output_column_name} {dimension_columns_to_add_to_fact_sql}
                    FROM        new_data_view a 
                    LEFT JOIN   dim_view b on {dimension_join_logic}
            """
    new_data = spark.sql(query)
    
    new_data = new_data.withColumn(
        dimension_key_output_column_name, f.when(f.col(dimension_key_output_column_name).isNull(), -1).otherwise(f.col(dimension_key_output_column_name))
    )
    
    return new_data

def _attach_dimension_surrogate_key(
    df: DataFrame,
    surrogate_key_logic: dict,
    all_metadata: dict,
    dimension_table_key_column_name: str
) -> DataFrame:
    """
    Attach Surrogate Key for a DataFrame of a Fact Table linking it to a dimension table.

    Args:
        df (DataFrame): The input DataFrame of the Fact table.
        surrogate_key_logic (dict): The tables and joining conditions for the reference table (dimension_join_logic should include any SCD2 logic)
        all_metadata (dict): Metadata dictionary containing workspace variables and configuration
        dimension_table_key_column_name (str): The column name of the surrogate key in the table.
    Returns:
        DataFrame: df after addition of surrogate key column(s).
    """
    inserting_surrogate_key_log_info = "Inserting surrogate key from reference table."
    log_and_print(inserting_surrogate_key_log_info)

    # Extract configuration
    config = _extract_surrogate_key_configuration(
        surrogate_key_logic = surrogate_key_logic, 
        dimension_table_key_column_name = dimension_table_key_column_name
    )
    
    creating_surrogate_key_log_info = f"Creating surrogate key column, {config['dimension_key_output_column_name']}, in fact table from dimension table, {config['dimension_table_name']}, using column {config['dimension_table_key_column_name']}."
    log_and_print(creating_surrogate_key_log_info)

    join_logic_log_info = f"Join logic is: {config['dimension_join_logic']}."
    log_and_print(join_logic_log_info)

    dimension_datastore_name = config['dimension_table_name'].split('.')[0].lower()

    target_workspace_name = _get_datastore_config(all_metadata['datastore_config'], dimension_datastore_name, 'Workspace_Name')
    
    full_dimension_table_name = f"`{target_workspace_name}`.{config['dimension_table_name']}"

    # Check if fact key column already exists (case-insensitive check since Spark is case-insensitive)
    existing_columns_lower = [col.lower() for col in df.columns]
    if config['dimension_key_output_column_name'].lower() in existing_columns_lower:
        error_message = f"The column `{config['dimension_key_output_column_name']}` already exists in the DataFrame. Please rename the existing column or choose a different name for the surrogate key column."
        log_and_print(error_message, "error")
        raise ValueError(error_message)

    # Setup temp views
    dimension_table = _setup_temp_views(
        df = df, 
        dimension_table_name = full_dimension_table_name
    )
    
    # Extract dimension columns
    join_columns = _extract_dimension_columns(
        dimension_join_logic = config['dimension_join_logic']
    )
    
    # Prepare additional columns
    dimension_columns_to_add_to_fact_sql = _prepare_additional_columns(
        dimension_columns_to_add_to_fact = config['dimension_columns_to_add_to_fact']
    )
    
    # Validate join uniqueness
    _validate_join_uniqueness(
        dimension_table_name = full_dimension_table_name, 
        join_columns = join_columns, 
        dimension_join_logic = config['dimension_join_logic']
    )
    
    # Execute surrogate key join (dimension_join_logic should include any SCD2 logic if needed)
    df = _execute_surrogate_key_join(
        dimension_table_key_column_name = config['dimension_table_key_column_name'],
        dimension_key_output_column_name = config['dimension_key_output_column_name'],
        dimension_columns_to_add_to_fact_sql = dimension_columns_to_add_to_fact_sql, 
        dimension_join_logic = config['dimension_join_logic']
    )
    
    # Reorder columns
    df = _reorder_columns_with_sk_first(
        df = df,
        dimension_table_key_column_name = config['dimension_key_output_column_name']
    )

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# This function creates the null row for a given Dataframe
def insert_null_row(
    new_data: DataFrame, 
    column_to_mark_source_data_deletion: str, 
    dimension_table_key_column_name: str
) -> DataFrame:
    """
    Create and append a standardized "unknown" member row to dimension tables.

    This function generates a null/unknown row with:
    - Surrogate key of -1 (standard unknown member identifier)
    - Default values for all non-nullable columns
    - NULL values for nullable columns
    - Active status for SCD tracking

    Args:
        new_data (DataFrame): The dimension table DataFrame
        column_to_mark_source_data_deletion (str): Column indicating deletion status
        dimension_table_key_column_name (str): Name of the surrogate key column

    Returns:
        DataFrame: Original data with appended null row

    Note:
        The null row ensures referential integrity when fact records 
        cannot be matched to dimension members.
    """

    schema = new_data.schema
    fields = new_data.schema.fields
    
    # Standard dates for unknown member
    old_timestamp = datetime(1900, 1, 1)
    old_date = date(1900, 1, 1)

    null_row = []
    
    for field in fields:
        field_name = field.name
        datatype = field.dataType.simpleString()
        nullable = field.nullable
        
        # Determine appropriate default value based on column type and nullability
        is_required_field = (
            not nullable or 
            field_name in ("scd_active", column_to_mark_source_data_deletion, dimension_table_key_column_name)
        )
        
        if is_required_field:
            # Handle special columns with specific values
            if field_name == "scd_active":
                null_row.append(1)  # Unknown member is always active
            elif field_name == column_to_mark_source_data_deletion:
                null_row.append(0)  # Unknown member is never deleted
            elif field_name == dimension_table_key_column_name:
                null_row.append(-1)  # Standard unknown member SK
            
            # Handle numeric types
            elif "double" in datatype:
                null_row.append(-1.0)
            elif "decimal" in datatype:
                null_row.append(Decimal(-1))
            elif datatype in ('tinyint', 'smallint', 'int', 'bigint'):
                null_row.append(-1)
            
            # Handle date/time types
            elif 'timestamp' in datatype:
                null_row.append(old_timestamp)
            elif 'date' in datatype:
                null_row.append(old_date)
            
            # Default to string representation for other types
            else:
                null_row.append('-1')
        else:
            # Nullable fields can be NULL for unknown member
            null_row.append(None)

    # Create and append the null row
    null_row = Row(*null_row)
    null_df = spark.createDataFrame([null_row], schema=schema)
    new_data = new_data.union(null_df)

    return new_data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Data Cleansing Functions
# 
# These functions standardize and clean data to ensure consistency across the platform. They handle column naming conventions, null value handling, and duplicate removal, which are critical for downstream data quality and analytics.

# CELL ********************

def _trim_column_names(
    columns: list,
    trim_enabled: bool
) -> list:
    """
    Trim leading and trailing whitespace from column names.
    
    Args:
        columns (list): List of column names to process
        trim_enabled (bool): Whether to apply trimming
    
    Returns:
        list: Column names with whitespace trimmed (system columns preserved)
    """
    if not trim_enabled:
        return columns
    
    trim_log_info = "Trimming leading and trailing whitespace from column names."
    log_and_print(trim_log_info)
    
    return [col if 'delta__' in col else col.strip() for col in columns]


def _exact_string_replacement(
    columns: list,
    find_strings: str,
    replace_string: str
) -> list:
    """
    Replace exact string matches in column names.
    
    Args:
        columns (list): List of column names to process
        find_strings (str): Comma-separated list of strings to find
        replace_string (str): Replacement string (single value to replace all matches) or 
                             comma-separated list of replacement strings (matched by position to find_strings)
    
    Returns:
        list: Column names with exact strings replaced (system columns preserved)
    
    Raises:
        ValueError: If replacement list has multiple items but count doesn't match find list count
    
    Notes:
        - If replace_string is a single value, all find_strings are replaced with that value
        - If replace_string is comma-separated, replacements MUST match the count of find_strings
        - System columns (containing 'delta__') are never modified
    """
    if not find_strings or not replace_string:
        return columns
    
    exact_find_list = [s.strip() for s in find_strings.split(',') if s.strip()]
    if not exact_find_list:
        return columns
    
    # Parse replacement string - could be single value or comma-separated list
    exact_replace_list = [s.strip() for s in replace_string.split(',') if s.strip()]
    if not exact_replace_list:
        return columns
    
    # Validate counts match for positional replacement
    if len(exact_replace_list) > 1 and len(exact_replace_list) != len(exact_find_list):
        error_message = (
            f"Exact string replacement count mismatch: "
            f"Found {len(exact_find_list)} strings to find ({exact_find_list}) "
            f"but {len(exact_replace_list)} replacement values ({exact_replace_list}). "
            f"Either provide a single replacement value for all finds, "
            f"or provide exactly one replacement value for each find string."
        )
        log_and_print(error_message)
        raise ValueError(error_message)
    
    # Determine if we have positional replacements or a single replacement for all
    if len(exact_replace_list) == 1:
        # Single replacement value for all find strings
        exact_replacement_log_info = f"Applying exact string replacement: {exact_find_list} → '{exact_replace_list[0]}'."
        log_and_print(exact_replacement_log_info)
        
        new_columns = []
        for col in columns:
            if 'delta__' in col:
                new_columns.append(col)
            else:
                new_col = col
                for find_str in exact_find_list:
                    new_col = new_col.replace(find_str, exact_replace_list[0])
                new_columns.append(new_col)
    else:
        # Positional replacements - match by index (already validated counts match)
        replacement_pairs = list(zip(exact_find_list, exact_replace_list))
        exact_replacement_log_info = f"Applying exact string replacement with positional matching: {replacement_pairs}."
        log_and_print(exact_replacement_log_info)
        
        new_columns = []
        for col in columns:
            if 'delta__' in col:
                new_columns.append(col)
            else:
                new_col = col
                for find_str, replace_str in replacement_pairs:
                    new_col = new_col.replace(find_str, replace_str)
                new_columns.append(new_col)
    
    return new_columns


def _regex_replacement(
    columns: list,
    pattern: str,
    replacement: str
) -> list:
    """
    Replace column name content using regex pattern.
    
    Args:
        columns (list): List of column names to process
        pattern (str): Regex pattern to find
        replacement (str): Replacement string for pattern matches
    
    Returns:
        list: Column names with regex replacements applied (system columns preserved)
    """
    if not pattern:
        return columns
    
    try:
        regex_replacement_log_info = f"Applying regex replacement: pattern='{pattern}', replacement='{replacement}'."
        log_and_print(regex_replacement_log_info)
        
        return [
            col if 'delta__' in col else re.sub(pattern, replacement, col)
            for col in columns
        ]
    except re.error as e:
        error_log_info = f"Invalid regex pattern '{pattern}': {str(e)}. Skipping regex replacement."
        log_and_print(error_log_info)
        return columns


def _replace_non_alphanumeric(
    columns: list,
    enabled: bool
) -> list:
    """
    Replace non-alphanumeric characters with underscores.
    
    Args:
        columns (list): List of column names to process
        enabled (bool): Whether to apply replacement
    
    Returns:
        list: Column names with non-alphanumeric chars replaced (system columns preserved)
    """
    if not enabled:
        return columns
    
    special_char_replacement_log_info = "Replacing non-alphanumeric characters with underscores using regex '[^A-Za-z0-9_]'."
    log_and_print(special_char_replacement_log_info)
    
    return [
        col if 'delta__' in col else re.sub(r'[^A-Za-z0-9_]', '_', col)
        for col in columns
    ]


def _collapse_underscores(
    columns: list
) -> list:
    """
    Collapse consecutive underscores to single underscore.
    
    Args:
        columns (list): List of column names to process
    
    Returns:
        list: Column names with consecutive underscores collapsed (system columns preserved)
    """
    collapse_underscores_log_info = "Collapsing consecutive underscores to single underscore."
    log_and_print(collapse_underscores_log_info)
    
    return [
        col if 'delta__' in col else re.sub(r'_+', '_', col)
        for col in columns
    ]

def _apply_case_conversion(
    columns: list,
    case_type: str
) -> list:
    """
    Apply case conversion to column names.
    
    Args:
        columns (list): List of column names to process
        case_type (str): Case type - 'title', 'lower', 'upper', or '' (no change)
    
    Returns:
        list: Column names with case conversion applied (system columns preserved)
    """
    if case_type == 'title':
        title_case_log_info = "Converting column names to title case."
        log_and_print(title_case_log_info)
        return [col if 'delta__' in col else col.title() for col in columns]
    
    elif case_type == 'lower':
        lower_case_log_info = "Converting column names to lowercase."
        log_and_print(lower_case_log_info)
        return [col if 'delta__' in col else col.lower() for col in columns]
    
    elif case_type == 'upper':
        upper_case_log_info = "Converting column names to uppercase."
        log_and_print(upper_case_log_info)
        return [col if 'delta__' in col else col.upper() for col in columns]
    
    else:
        column_names_kept_log_info = "Column name casing is kept the same."
        log_and_print(column_names_kept_log_info)
        return columns


def column_name_cleansing(
    columns,
    advanced_processing_config: dict
):
    """
    Standardize column names according to enterprise naming conventions.

    This function orchestrates a series of transformations to ensure:
    - Consistent naming patterns across all tables
    - Removal of problematic special characters
    - Compliance with Spark/Delta column naming rules
    - Preservation of system columns (delta__*)

    Args:
        columns (list): List of original column names
        advanced_processing_config (dict): Configuration dict containing column name standardization settings:
            - trim_column_names (bool): Trim leading/trailing whitespace from column names
            - apply_case_column_names (str): Case transformation to apply - 'lower', 'upper', 'title', or '' (no change)
            - replace_non_alphanumeric_chars_with_underscore_in_column_names (bool): Replace non-alphanumeric characters with underscores
            - regex_find_in_column_names (str): Regex pattern to find in column names
            - regex_replace_in_column_names (str): Replacement value for regex matches
            - exact_find_in_column_names (str): Comma-separated exact strings to find
            - exact_replace_in_column_names (str): Single replacement value for all finds OR comma-separated list with exact count match

    Returns:
        list: Cleansed column names following naming standards

    Transformations applied (in order):
        1. Trim leading/trailing whitespace (if enabled)
        2. Exact string replacement (if configured)
        3. Regex pattern replacement (if configured)
        4. Replace non-alphanumeric characters with underscores (if enabled)
        5. Collapse consecutive underscores to single underscore
        6. Apply case conversion (title, lower, upper, or none)
    """
    # Extract column name standardization settings from config
    trim_column_names = advanced_processing_config['trim_column_names']
    apply_case_column_names = advanced_processing_config['apply_case_column_names']
    replace_non_alphanumeric_chars_with_underscore_in_column_names = advanced_processing_config['replace_non_alphanumeric_chars_with_underscore_in_column_names']
    regex_find_in_column_names = advanced_processing_config['regex_find_in_column_names']
    regex_replace_in_column_names = advanced_processing_config['regex_replace_in_column_names']
    exact_find_in_column_names = advanced_processing_config['exact_find_in_column_names']
    exact_replace_in_column_names = advanced_processing_config['exact_replace_in_column_names']
    
    # Start with original columns
    cleansed_columns = columns
    
    # Step 1: Trim leading/trailing whitespace
    cleansed_columns = _trim_column_names(
        columns=cleansed_columns,
        trim_enabled=trim_column_names
    )
    
    # Step 2: Exact string replacement
    cleansed_columns = _exact_string_replacement(
        columns=cleansed_columns,
        find_strings=exact_find_in_column_names,
        replace_string=exact_replace_in_column_names
    )
    
    # Step 3: Regex pattern replacement
    cleansed_columns = _regex_replacement(
        columns=cleansed_columns,
        pattern=regex_find_in_column_names,
        replacement=regex_replace_in_column_names
    )
    
    # Step 4: Replace non-alphanumeric characters with underscores
    cleansed_columns = _replace_non_alphanumeric(
        columns=cleansed_columns,
        enabled=replace_non_alphanumeric_chars_with_underscore_in_column_names
    )
    
    # Step 5: Collapse consecutive underscores (only if step 4 was applied)
    if replace_non_alphanumeric_chars_with_underscore_in_column_names:
        cleansed_columns = _collapse_underscores(columns=cleansed_columns)
    
    # Step 6: Apply case conversion
    cleansed_columns = _apply_case_conversion(
        columns=cleansed_columns,
        case_type=apply_case_column_names
    )
    
    # Create column mapping for logging
    column_mapping = list(zip(columns, cleansed_columns))
    
    # Log first 10 column mappings for debugging
    first_ten_mappings = column_mapping[:10]
    column_mapping_log_info = f"First 10 column name mappings (original → cleansed): {first_ten_mappings}"
    log_and_print(column_mapping_log_info)
    
    return zip(columns, cleansed_columns)


def _apply_column_name_cleansing_to_list(
    columns: list,
    advanced_processing_config: dict,
    column_type_description: str
) -> list:
    """
    Helper function to apply column name cleansing to a list of columns.
    
    Args:
        columns (list): List of column names to cleanse
        advanced_processing_config (dict): Configuration dict containing column name standardization settings
        column_type_description (str): Description of column type for logging (e.g., "primary key", "liquid clustering")
    
    Returns:
        list: List of cleansed column names
    """
    if not columns:
        return []
    
    log_and_print(f"Applying column name standardization to {column_type_description} columns.")
    
    column_name_mapping_zip = column_name_cleansing(
        columns=columns,
        advanced_processing_config=advanced_processing_config
    )
    
    column_name_mapping = list(column_name_mapping_zip)
    cleansed_columns = [col[1] for col in column_name_mapping]
    
    return cleansed_columns


def cleanse_column_names(
    df: DataFrame,
    primary_keys: list, 
    liquid_clustering_columns: list, 
    advanced_processing_config: dict
):
    """
    Apply column name standardization to DataFrame and related metadata lists.

    This function serves as a wrapper that applies column name cleansing to:
    - The DataFrame column names
    - Primary key column references
    - Liquid clustering column references
    
    It intelligently determines the default standardization mode based on source type,
    allowing query-based sources to preserve their original naming conventions.

    Args:
        df (DataFrame): Input DataFrame with columns to be standardized
        primary_keys (list): List of primary key column names to be updated
        liquid_clustering_columns (list): List of clustering column names to be updated
        advanced_processing_config (dict): Dictionary containing processing configuration including:
            - lower_case_column_names (bool): Convert column names to lowercase
            - title_case_column_names (bool): Convert column names to title case
            - replace_non_alphanumeric_chars_with_underscore_in_column_names (bool): Replace non-alphanumeric characters with underscores

    Returns:
        tuple: (DataFrame with renamed columns, updated primary keys list, updated clustering columns list)

    Configuration:
        - Table sources default to false for all options (preserve original names)
        - File sources default to true for lowercase and replace_non_alphanumeric_chars (apply standardization)
        - Can be overridden via column_cleansing category configs: trim, apply_case, 
          replace_non_alphanumeric_with_underscore, regex_find, regex_replace, exact_find, exact_replace
    """
    # Apply column name standardization to DataFrame columns
    cleansed_column_names = _apply_column_name_cleansing_to_list(
        columns=df.columns,
        advanced_processing_config=advanced_processing_config,
        column_type_description="data"
    )
    
    # Apply column name standardization to primary key columns
    cleansed_primary_keys = _apply_column_name_cleansing_to_list(
        columns=primary_keys,
        advanced_processing_config=advanced_processing_config,
        column_type_description="primary key"
    )

    # Apply column name standardization to liquid clustering columns
    cleansed_liquid_clustering_columns = _apply_column_name_cleansing_to_list(
        columns=liquid_clustering_columns,
        advanced_processing_config=advanced_processing_config,
        column_type_description="liquid clustering"
    )

    # Rename DataFrame columns to match cleansed names
    df = df.toDF(*cleansed_column_names)

    return df, cleansed_primary_keys, cleansed_liquid_clustering_columns

def add_timestamp_metadata_columns(
    df: DataFrame,
    target_table_name: str
):
    """
    Add system-generated timestamp columns for data lineage and change tracking.

    This function adds metadata columns ONLY when writing to Delta tables (not file outputs):
    - `delta__created_datetime`: Captures when the record first lands in a Fabric Delta table.
    - `delta__modified_datetime`: Captures when the record is written/updated during the current batch.

    The function intelligently determines when to add timestamps:
    - Created datetime: Added if not already present to track initial ingestion
    - Modified datetime: Refreshed on every Delta table write so downstream consumers know when data was last updated

    Args:
        df (DataFrame): Input DataFrame to enhance with metadata
        target_table_name (str): Target Delta table name (empty string for file outputs)

    Returns:
        DataFrame: Enhanced DataFrame with timestamp columns (if target_table_name is provided), otherwise unchanged

    Notes:
        - Timestamps are ONLY added when target_table_name is truthy (Delta table writes)
        - For file outputs (target_table_name is empty/None), no timestamp columns are added
        - Modified datetime tracks when records were last written to the Delta table
        - Created datetime preserves initial Delta table ingestion time
        - Microsecond precision is maintained for accurate ordering
    """
    # Only add metadata columns if writing to a table (not a file)
            
    if target_table_name:
        date_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

        # Add created datetime when the column does not exist, ensuring the initial load timestamp is preserved
        if 'delta__created_datetime' not in df.columns:
            adding_created_datetime_log_info = "Adding created datetime metadata column."
            log_and_print(adding_created_datetime_log_info)

            # Generate timestamp with microsecond precision
            df = df.withColumn('delta__created_datetime', f.to_timestamp(f.lit(date_time), 'yyyy-MM-dd HH:mm:ss.SSSSSS'))
        
        # Always refresh modified datetime so downstream tables know when the write occurred
        adding_modified_datetime_log_info = "Setting modified datetime metadata column."
        log_and_print(adding_modified_datetime_log_info)

        # Initialize modified datetime with created datetime value
        df = df.withColumn('delta__modified_datetime', f.to_timestamp(f.lit(date_time), 'yyyy-MM-dd HH:mm:ss.SSSSSS'))

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Data Quality Category Display Name Mapping
# This mapping converts internal category names to user-friendly display names
# for reporting in the Data_Quality_Notifications table
DQ_CATEGORY_DISPLAY_NAMES = {
    "validate_condition": "Validate Condition",
    "duplicates_on_pks": "Duplicate Primary Keys",
    "table_lookups": "Referential Integrity",  # Legacy name - kept for backward compatibility
    "validate_referential_integrity": "Referential Integrity",
    "pattern_match": "Pattern Validation",  # Legacy name - kept for backward compatibility
    "validate_pattern": "Pattern Validation",
    "validate_batch_size": "Batch Size Validation",
    "validate_not_null": "Not Null Validation",
    "validate_unique": "Uniqueness Validation",
    "validate_range": "Range Validation",
    "validate_freshness": "Freshness Validation",
    "validate_completeness": "Completeness Validation",
    "validate_anomaly": "Anomaly Detection",
    "File Corruption": "File Corruption"  # Already formatted
}

def _get_dq_category_display_name(category: str) -> str:
    """Get user-friendly display name for a data quality category.
    
    Args:
        category: Internal category name (e.g., 'validate_condition', 'validate_not_null')
    
    Returns:
        str: User-friendly display name for reporting (e.g., 'Validate Condition', 'Not Null Validation')
    """
    return DQ_CATEGORY_DISPLAY_NAMES.get(category, category.replace('_', ' ').title())


def _record_dq_result(
    violation_count: int,
    dq_category: str,
    dq_message: str,
    if_not_compliant: str,
    warnings: list,
    force_failures: list,
    pass_message: str = None,
    column_name: str = None
) -> tuple:
    """
    Record data quality check results to the appropriate list based on compliance action.
    
    This centralized function handles the common pattern of recording DQ results:
    - 'warn' (default): Add warning with row impact count
    - 'quarantine': Add warning indicating rows were quarantined
    - 'fail': Add to force_failures list
    - Pass (violation_count == 0): Record successful validation
    
    Args:
        violation_count (int): Number of records that violated the DQ rule
        dq_category (str): Internal category name (will be converted to display name)
        dq_message (str): Message describing the violation (used for failures and warnings)
        if_not_compliant (str): Action to take - 'warn', 'quarantine', or 'fail'
        warnings (list): List to append warning/pass results to
        force_failures (list): List to append fail results to
        pass_message (str, optional): Custom message for pass outcome. If None, uses dq_message.
        column_name (str, optional): Column name for column-specific validations (adds 'column' key)
    
    Returns:
        tuple: (warnings, force_failures) - Updated lists after recording the result
    
    Note:
        This function only records results - it does NOT handle DataFrame quarantine operations.
        Callers must handle quarantine DataFrame logic separately before calling this function.
    """
    category_display = _get_dq_category_display_name(dq_category)
    
    if violation_count > 0:
        if if_not_compliant == 'warn':
            json_to_append = {
                "outcome": "Warning", 
                "category": category_display, 
                "message": dq_message, 
                "quarantined": "No", 
                "rows_impacted": violation_count
            }
            if column_name:
                json_to_append["column"] = column_name
            warnings.append(json_to_append)
        elif if_not_compliant == 'quarantine':
            json_to_append = {
                "outcome": "Warning", 
                "category": category_display, 
                "message": dq_message, 
                "quarantined": "Yes", 
                "rows_quarantined": violation_count
            }
            if column_name:
                json_to_append["column"] = column_name
            warnings.append(json_to_append)
        else:
            # 'fail' or any unrecognized value - add to force_failures list
            json_to_append = {
                "category": category_display, 
                "message": dq_message, 
                "rows_impacted": violation_count
            }
            if column_name:
                json_to_append["column"] = column_name
            force_failures.append(json_to_append)
    else:
        # Pass - no violations found
        json_to_append = {
            "outcome": "Pass", 
            "category": category_display, 
            "message": pass_message if pass_message else dq_message
        }
        if column_name:
            json_to_append["column"] = column_name
        warnings.append(json_to_append)
    
    return warnings, force_failures


def _quarantine_rows(
    df: DataFrame,
    df_quarantined_all: DataFrame,
    df_violations: DataFrame,
    quarantine_reason: str,
    join_keys: Union[str, list],
    join_type: str = 'inner'
) -> tuple:
    """
    Quarantine violating rows from the main DataFrame.
    
    This centralized function handles the common pattern of:
    1. Joining main df with violations to identify rows to quarantine
    2. Adding quarantine reason column
    3. Appending to accumulated quarantine DataFrame
    4. Removing quarantined rows from main DataFrame
    
    Args:
        df (DataFrame): Main DataFrame to remove quarantined rows from
        df_quarantined_all (DataFrame): Accumulated quarantined records DataFrame
        df_violations (DataFrame): DataFrame containing the violating rows/keys
        quarantine_reason (str): Reason message to add to quarantined records
        join_keys (str or list): Column name(s) to join on for identifying rows
        join_type (str): Type of join for quarantine identification ('inner' default)
                        Use 'semi' when df_violations has fewer columns than df
    
    Returns:
        tuple: (cleaned_df, updated_quarantined_df)
            - cleaned_df: Main DataFrame with violating rows removed
            - updated_quarantined_df: Accumulated quarantine DataFrame with new rows
    """
    # Identify rows to quarantine
    if join_type == 'semi':
        df_quarantined = df.join(df_violations, join_keys, 'semi').select(df["*"])
    else:
        df_quarantined = df.join(df_violations, join_keys, 'inner').select(df["*"])
    
    # Add quarantine reason
    df_quarantined = df_quarantined.withColumn('Quarantine_Reason', f.lit(quarantine_reason))
    
    # Append to accumulated quarantine DataFrame
    df_quarantined_all = df_quarantined_all.unionByName(
        df_quarantined, 
        allowMissingColumns=True
    )
    
    # Remove quarantined rows from main DataFrame
    df = df.join(df_violations, join_keys, 'left_anti').select(df["*"])
    
    return df, df_quarantined_all


def _handle_corrupt_records(
    df: DataFrame, 
    warnings: list
) -> tuple:
    """Handle corrupt records from schema enforcement."""
    df_quarantined_all = df.limit(0)
    
    if 'delta__corrupt_record' in df.columns:
        identifying_corrupt_records_log_info = "Identifying corrupt records."
        log_and_print(identifying_corrupt_records_log_info)
        df_corrupt_records = df.where(f.col("delta__corrupt_record").isNotNull())
        
        quarantine_message = f"Quarantined due to row data not conforming to schema input in metadata."
        df_quarantined = df_corrupt_records.withColumn('Quarantine_Reason', f.lit(quarantine_message))
        df_quarantined_all = df_quarantined_all.unionByName(
            df_quarantined, 
            allowMissingColumns = True
        )
        
        df = df.join(df_corrupt_records, "delta__idx", 'left_anti').select(df["*"])
        df = df.drop("delta__corrupt_record")

        number_of_corrupt_records = df_corrupt_records.count()
        dq_message = f"There are {number_of_corrupt_records} rows from the input file in the Bronze files did not match the provided schema."
        json_to_append = {
            "outcome": "Warning", 
            "category": "File Corruption", 
            "message": dq_message, 
            "quarantined": "Yes", 
            "rows_quarantined": number_of_corrupt_records
        }
        warnings.append(json_to_append)
    
    return df, df_quarantined_all, warnings

def _validate_primary_keys(
    df: DataFrame, 
    df_quarantined_all: DataFrame, 
    primary_keys: list, 
    if_duplicate_primary_keys: str, 
    columns_to_add_to_pks: list, 
    target_table_name: str, 
    warnings: list, 
    force_failures: list
) -> tuple:
    """Validate primary key uniqueness."""
    if primary_keys:
        validating_primary_keys_log_info = "Validating primary keys."
        log_and_print(validating_primary_keys_log_info)

        data_quality_category = "duplicates_on_pks"

        if columns_to_add_to_pks and target_table_name:
            primary_keys = primary_keys + columns_to_add_to_pks

        # Validate that all primary keys exist
        _validate_columns_exist(
            df = df, 
            columns_to_check = primary_keys, 
            operation_name = "primary_key_validation"
        )

        dupes = df.groupby(primary_keys) \
                    .count() \
                    .filter(f.col('count') > 1) 
        
        number_of_dupes = dupes.count()
        primary_keys_str = ', '.join(primary_keys)
        dq_message = f"There are {number_of_dupes} primary keys with duplicate values. The primary keys are {primary_keys_str}."
        
        # Handle quarantine if needed
        if number_of_dupes > 0 and if_duplicate_primary_keys == 'quarantine':
            quarantine_message = f"Quarantined due to duplicate primary keys. The primary keys are {primary_keys_str}."
            df, df_quarantined_all = _quarantine_rows(
                df=df,
                df_quarantined_all=df_quarantined_all,
                df_violations=dupes,
                quarantine_reason=quarantine_message,
                join_keys=primary_keys
            )
        
        # Record the DQ result
        _record_dq_result(
            violation_count=number_of_dupes,
            dq_category=data_quality_category,
            dq_message=dq_message,
            if_not_compliant=if_duplicate_primary_keys,
            warnings=warnings,
            force_failures=force_failures
        )
    
    return df, df_quarantined_all, warnings, force_failures

def _validate_condition(
    df: DataFrame, 
    df_quarantined_all: DataFrame, 
    data_quality_step: dict, 
    warnings: list, 
    force_failures: list
) -> tuple:
    """Validate data using a SQL condition expression.
    
    Note: The condition should define what VALID data looks like. Records that DON'T match
    this condition will be flagged/quarantined as data quality issues.
    
    Example: condition = "amount > 0 AND status = 'Active'"
    This will flag/quarantine records where amount <= 0 OR status != 'Active'
    """
    data_quality_category = data_quality_step["Category"]
    dq_result_if_not_compliant = data_quality_step.get('if_not_compliant', 'warn').lower()
    condition = data_quality_step["condition"]

    validating_condition_log_info = f"Validating data using SQL condition: {condition}. Records NOT matching this condition will be flagged."
    log_and_print(validating_condition_log_info)
    
    # Find records that DON'T match the valid condition (i.e., the problematic records)
    condition_not_matches = df.filter(f"NOT ({condition})")

    number_of_condition_not_matches = condition_not_matches.count()
    dq_message = f"There are {number_of_condition_not_matches} rows that did NOT match the condition: {condition}."
    
    # Handle quarantine if needed
    if number_of_condition_not_matches > 0 and dq_result_if_not_compliant == 'quarantine':
        quarantine_message = f"Quarantined due to not matching validation condition: {condition}."
        df, df_quarantined_all = _quarantine_rows(
            df=df,
            df_quarantined_all=df_quarantined_all,
            df_violations=condition_not_matches,
            quarantine_reason=quarantine_message,
            join_keys='delta__idx'
        )
    
    # Record the DQ result
    _record_dq_result(
        violation_count=number_of_condition_not_matches,
        dq_category=data_quality_category,
        dq_message=dq_message,
        if_not_compliant=dq_result_if_not_compliant,
        warnings=warnings,
        force_failures=force_failures
    )
    
    return df, df_quarantined_all, warnings, force_failures

def _validate_batch_size(
    df: DataFrame, 
    data_quality_step: dict, 
    warnings: list, 
    force_failures: list
) -> tuple:
    """Validate that the batch meets a minimum row count threshold."""
    data_quality_category = data_quality_step["Category"]
    dq_result_if_not_compliant = data_quality_step.get('if_not_compliant', 'warn').lower()
    min_rows = data_quality_step["min_rows"]
    min_rows_int = int(min_rows)
    records = df.count()

    validating_batch_size_log_info = f"Validating batch size meets minimum threshold of {min_rows} rows"
    log_and_print(validating_batch_size_log_info)

    # Determine violation count (if below threshold, the shortfall is the "violation")
    is_below_threshold = records < min_rows_int
    violation_count = records if is_below_threshold else 0
    
    dq_message = f"Batch contains {records} rows, which is below the minimum threshold of {min_rows}."
    pass_message = f"Batch contains {records} rows, which meets the minimum threshold of {min_rows}."
    
    # Record the DQ result
    _record_dq_result(
        violation_count=violation_count,
        dq_category=data_quality_category,
        dq_message=dq_message,
        if_not_compliant=dq_result_if_not_compliant,
        warnings=warnings,
        force_failures=force_failures,
        pass_message=pass_message
    )
    
    return warnings, force_failures

def _validate_referential_integrity(
    df: DataFrame, 
    df_quarantined_all: DataFrame, 
    data_quality_step: dict, 
    datastore_config: str | list, 
    warnings: list, 
    force_failures: list
) -> tuple:
    """Validate referential integrity by checking values exist in a reference table.
    
    This function validates that values in the current table's column exist in a 
    reference/master table. This is essential for maintaining data integrity in 
    fact-dimension relationships and foreign key validations.
    
    Args:
        df (DataFrame): Input DataFrame to validate
        df_quarantined_all (DataFrame): Accumulated quarantined records
        data_quality_step (dict): Configuration containing:
            - current_table_column_name: Column(s) in current table to validate (comma-separated for multiple)
            - reference_table_name: Reference table name (datastore.schema.table)
            - reference_table_column_name: Corresponding column(s) in reference table (comma-separated)
            - if_not_compliant: Action when values not found ('warn', 'quarantine', 'fail')
        datastore_config (str | list): Datastore configuration from Datastore_Configuration table
        warnings (list): Accumulated warning messages
        force_failures (list): Accumulated failure messages
    
    Returns:
        tuple: (cleaned_df, quarantined_df, warnings, force_failures)
    """
    data_quality_category = data_quality_step["Category"]
    dq_result_if_not_compliant = data_quality_step.get('if_not_compliant', 'warn').lower()
    current_table_column_name = data_quality_step['current_table_column_name']
    reference_table_name = data_quality_step['reference_table_name']
    reference_table_column_name = data_quality_step['reference_table_column_name']

    target_datastore_name = reference_table_name.split('.')[0].lower()
    target_workspace_name = _get_datastore_config(datastore_config, target_datastore_name, 'Workspace_Name')
    full_reference_table_name = f"`{target_workspace_name}`.{reference_table_name}"

    validating_referential_integrity_log_info = f"Validating referential integrity: {current_table_column_name} values exist in {full_reference_table_name}"
    log_and_print(validating_referential_integrity_log_info)

    df_primary_table = df.select(current_table_column_name).distinct()
    df_reference_table = spark.sql(f"SELECT {reference_table_column_name} as {current_table_column_name} FROM {full_reference_table_name}").distinct()

    missing_values = df_primary_table.subtract(df_reference_table)
    number_of_missing_values = missing_values.count()
    dq_message = f"There are {number_of_missing_values} values from column `{current_table_column_name}` that were not found in the reference table column, {full_reference_table_name}.{reference_table_column_name}."
    
    # Handle quarantine if needed
    if number_of_missing_values > 0 and dq_result_if_not_compliant == "quarantine":
        quarantine_message = f"Quarantined because the `{current_table_column_name}` value was not found in the column, `{reference_table_column_name}`, in table {full_reference_table_name}"
        df, df_quarantined_all = _quarantine_rows(
            df=df,
            df_quarantined_all=df_quarantined_all,
            df_violations=missing_values,
            quarantine_reason=quarantine_message,
            join_keys=current_table_column_name
        )
    
    # Record the DQ result
    _record_dq_result(
        violation_count=number_of_missing_values,
        dq_category=data_quality_category,
        dq_message=dq_message,
        if_not_compliant=dq_result_if_not_compliant,
        warnings=warnings,
        force_failures=force_failures
    )
    
    return df, df_quarantined_all, warnings, force_failures


def _validate_not_null(
    df: DataFrame,
    df_quarantined_all: DataFrame,
    data_quality_step: dict,
    warnings: list,
    force_failures: list
) -> tuple:
    """Validate that specified columns do not contain NULL values.
    
    This function checks for NULL values in one or more columns and takes
    appropriate action based on configuration. Each column generates its own
    separate warning/failure message for granular tracking.
    
    Args:
        df (DataFrame): Input DataFrame to validate
        df_quarantined_all (DataFrame): Accumulated quarantined records
        data_quality_step (dict): Configuration containing:
            - column_name: Column(s) to check for NULLs (comma-separated for multiple)
            - if_not_compliant: Action when NULLs found ('warn', 'quarantine', 'fail')
        warnings (list): Accumulated warning messages
        force_failures (list): Accumulated failure messages
    
    Returns:
        tuple: (cleaned_df, quarantined_df, warnings, force_failures)
    """
    data_quality_category = data_quality_step["Category"]
    dq_result_if_not_compliant = data_quality_step.get('if_not_compliant', 'warn').lower()
    column_names_raw = data_quality_step['column_name']
    
    # Parse comma-separated column names
    column_names = [col.strip() for col in column_names_raw.split(',')]
    
    log_and_print(f"Validating NOT NULL constraint on columns: {column_names}")
    
    # Process each column separately for granular tracking
    for col_name in column_names:
        null_condition = f.col(col_name).isNull()
        df_nulls = df.filter(null_condition)
        null_count = df_nulls.count()
        
        dq_message = f"Found {null_count} records with NULL values in column: {col_name}"
        pass_message = f"All records have non-NULL values in column: {col_name}"
        
        # Handle quarantine if needed (must happen before recording result)
        if null_count > 0 and dq_result_if_not_compliant == "quarantine":
            quarantine_message = f"Quarantined due to NULL value in column: {col_name}"
            df_quarantined = df_nulls.withColumn('Quarantine_Reason', f.lit(quarantine_message))
            df_quarantined_all = df_quarantined_all.unionByName(df_quarantined, allowMissingColumns=True)
            df = df.filter(~null_condition)
        
        # Record the DQ result
        _record_dq_result(
            violation_count=null_count,
            dq_category=data_quality_category,
            dq_message=dq_message,
            if_not_compliant=dq_result_if_not_compliant,
            warnings=warnings,
            force_failures=force_failures,
            pass_message=pass_message,
            column_name=col_name
        )
    
    return df, df_quarantined_all, warnings, force_failures


def _validate_unique(
    df: DataFrame,
    df_quarantined_all: DataFrame,
    data_quality_step: dict,
    warnings: list,
    force_failures: list
) -> tuple:
    """Validate that specified columns contain unique values (no duplicates).
    
    This function checks for duplicate values in one or more columns that are
    not part of the primary key validation.
    
    Args:
        df (DataFrame): Input DataFrame to validate
        df_quarantined_all (DataFrame): Accumulated quarantined records
        data_quality_step (dict): Configuration containing:
            - column_name: Column(s) that should be unique (comma-separated for multiple)
            - if_not_compliant: Action when duplicates found ('warn', 'quarantine', 'fail')
        warnings (list): Accumulated warning messages
        force_failures (list): Accumulated failure messages
    
    Returns:
        tuple: (cleaned_df, quarantined_df, warnings, force_failures)
    """
    data_quality_category = data_quality_step["Category"]
    dq_result_if_not_compliant = data_quality_step.get('if_not_compliant', 'warn').lower()
    column_names_raw = data_quality_step['column_name']
    
    # Parse comma-separated column names
    column_names = [col.strip() for col in column_names_raw.split(',')]
    
    log_and_print(f"Validating uniqueness on columns: {column_names}")
    
    # Find duplicates
    window_spec = Window.partitionBy([f.col(c) for c in column_names])
    df_with_count = df.withColumn("_dup_count", f.count("*").over(window_spec))
    df_duplicates = df_with_count.filter(f.col("_dup_count") > 1)
    duplicate_count = df_duplicates.count()
    
    # Get count of unique duplicate values
    unique_dup_values = df_duplicates.select(column_names).distinct().count()
    
    dq_message = f"Found {duplicate_count} records with duplicate values in column(s): {column_names} ({unique_dup_values} unique duplicate value combinations)"
    pass_message = f"All values in column(s) {column_names} are unique"
    
    # Handle quarantine if needed
    if duplicate_count > 0 and dq_result_if_not_compliant == "quarantine":
        quarantine_message = f"Quarantined due to duplicate values in column(s): {column_names}"
        df_quarantined = df_duplicates.drop("_dup_count").withColumn('Quarantine_Reason', f.lit(quarantine_message))
        df_quarantined_all = df_quarantined_all.unionByName(df_quarantined, allowMissingColumns=True)
        df = df_with_count.filter(f.col("_dup_count") == 1).drop("_dup_count")
    else:
        df = df_with_count.drop("_dup_count")
    
    # Record the DQ result
    _record_dq_result(
        violation_count=duplicate_count,
        dq_category=data_quality_category,
        dq_message=dq_message,
        if_not_compliant=dq_result_if_not_compliant,
        warnings=warnings,
        force_failures=force_failures,
        pass_message=pass_message
    )
    
    return df, df_quarantined_all, warnings, force_failures


def _validate_range(
    df: DataFrame,
    df_quarantined_all: DataFrame,
    data_quality_step: dict,
    warnings: list,
    force_failures: list
) -> tuple:
    """Validate that numeric or date column values fall within a specified range.
    
    This function checks if values in the specified column(s) are within the 
    configured minimum and/or maximum bounds. Each column generates its own
    separate warning/failure message for granular tracking.
    
    Args:
        df (DataFrame): Input DataFrame to validate
        df_quarantined_all (DataFrame): Accumulated quarantined records
        data_quality_step (dict): Configuration containing:
            - column_name: Column to validate (supports comma-separated for multiple)
            - min_value: Optional minimum allowed value (inclusive)
            - max_value: Optional maximum allowed value (inclusive)
            - if_not_compliant: Action when out of range ('warn', 'quarantine', 'fail')
        warnings (list): Accumulated warning messages
        force_failures (list): Accumulated failure messages
    
    Returns:
        tuple: (cleaned_df, quarantined_df, warnings, force_failures)
    """
    data_quality_category = data_quality_step["Category"]
    dq_result_if_not_compliant = data_quality_step.get('if_not_compliant', 'warn').lower()
    column_names_raw = data_quality_step['column_name']
    min_value = data_quality_step.get('min_value')
    max_value = data_quality_step.get('max_value')
    
    # Parse comma-separated column names
    column_names = [col.strip() for col in column_names_raw.split(',')]
    
    log_and_print(f"Validating range on columns: {column_names} (min: {min_value}, max: {max_value})")
    
    if min_value is None and max_value is None:
        log_and_print("No min_value or max_value specified, skipping range validation", "warn")
        return df, df_quarantined_all, warnings, force_failures
    
    range_desc = []
    if min_value is not None:
        range_desc.append(f">= {min_value}")
    if max_value is not None:
        range_desc.append(f"<= {max_value}")
    range_str = ' AND '.join(range_desc)
    
    # Process each column separately for granular tracking
    for col_name in column_names:
        # Build out-of-range condition for this column
        col_conditions = []
        if min_value is not None:
            col_conditions.append(f.col(col_name) < f.lit(min_value))
        if max_value is not None:
            col_conditions.append(f.col(col_name) > f.lit(max_value))
        
        # Combine with OR - value is out of range if below min OR above max
        out_of_range_condition = col_conditions[0]
        for cond in col_conditions[1:]:
            out_of_range_condition = out_of_range_condition | cond
        
        df_out_of_range = df.filter(out_of_range_condition)
        out_of_range_count = df_out_of_range.count()
        
        dq_message = f"Found {out_of_range_count} records with values outside range ({range_str}) in column: {col_name}"
        pass_message = f"All values in column {col_name} are within range ({range_str})"
        
        # Handle quarantine if needed (must happen before recording result)
        if out_of_range_count > 0 and dq_result_if_not_compliant == "quarantine":
            quarantine_message = f"Quarantined due to value outside range ({range_str}) in column: {col_name}"
            df_quarantined = df_out_of_range.withColumn('Quarantine_Reason', f.lit(quarantine_message))
            df_quarantined_all = df_quarantined_all.unionByName(df_quarantined, allowMissingColumns=True)
            df = df.filter(~out_of_range_condition)
        
        # Record the DQ result
        _record_dq_result(
            violation_count=out_of_range_count,
            dq_category=data_quality_category,
            dq_message=dq_message,
            if_not_compliant=dq_result_if_not_compliant,
            warnings=warnings,
            force_failures=force_failures,
            pass_message=pass_message,
            column_name=col_name
        )
    
    return df, df_quarantined_all, warnings, force_failures


def _validate_freshness(
    df: DataFrame,
    df_quarantined_all: DataFrame,
    data_quality_step: dict,
    warnings: list,
    force_failures: list
) -> tuple:
    """Validate that data is fresh (within expected time threshold).
    
    This function checks if the maximum timestamp in a date/datetime column
    is within the configured freshness threshold from current time.
    
    Args:
        df (DataFrame): Input DataFrame to validate
        df_quarantined_all (DataFrame): Accumulated quarantined records
        data_quality_step (dict): Configuration containing:
            - column_name: Timestamp/date column to check for freshness
            - max_age: Maximum allowed age value (numeric)
            - unit_of_measure: Unit for max_age - 'minute', 'hour', 'day', 'week', 'month' (default: 'hour')
            - if_not_compliant: Action when data is stale ('warn', 'fail')
        warnings (list): Accumulated warning messages
        force_failures (list): Accumulated failure messages
    
    Returns:
        tuple: (cleaned_df, quarantined_df, warnings, force_failures)
    
    Note:
        This check validates the overall dataset freshness, not individual records.
        It's useful for alerting when data pipelines haven't run recently.
    """
    data_quality_category = data_quality_step["Category"]
    dq_result_if_not_compliant = data_quality_step.get('if_not_compliant', 'warn').lower()
    column_name = data_quality_step['column_name'].strip()
    max_age = data_quality_step.get('max_age')
    unit_of_measure = data_quality_step.get('unit_of_measure', 'hour').lower()
    
    log_and_print(f"Validating data freshness on column: {column_name}")
    
    # Calculate threshold based on unit_of_measure
    unit_to_seconds = {
        'minute': 60,
        'hour': 60 * 60,
        'day': 24 * 60 * 60,
        'week': 7 * 24 * 60 * 60,
        'month': 30 * 24 * 60 * 60  # Approximate 30 days
    }
    
    if max_age is not None:
        if unit_of_measure not in unit_to_seconds:
            raise ValueError(f"Invalid unit_of_measure '{unit_of_measure}'. Must be one of: minute, hour, day, week, month")
        max_age_seconds = float(max_age) * unit_to_seconds[unit_of_measure]
        # Pluralize unit name for display if value > 1
        unit_display = unit_of_measure + ('s' if float(max_age) != 1 else '')
        threshold_desc = f"{max_age} {unit_display}"
    else:
        max_age_seconds = 24 * 60 * 60  # Default 24 hours
        threshold_desc = "24 hours (default)"
    
    # Get the maximum timestamp from the data
    max_timestamp_row = df.agg(f.max(f.col(column_name)).alias("max_ts")).collect()[0]
    max_timestamp = max_timestamp_row["max_ts"]
    
    if max_timestamp is None:
        dq_message = f"No timestamp values found in column '{column_name}' - cannot validate freshness"
        _record_dq_result(
            violation_count=0,
            dq_category=data_quality_category,
            dq_message=dq_message,
            if_not_compliant='warn',
            warnings=warnings,
            force_failures=force_failures
        )
        return df, df_quarantined_all, warnings, force_failures
    
    # Calculate age
    current_time = datetime.now()
    if isinstance(max_timestamp, datetime):
        data_age = current_time - max_timestamp
        age_seconds = data_age.total_seconds()
    else:
        # Handle date type
        from datetime import datetime as dt
        max_datetime = dt.combine(max_timestamp, dt.min.time())
        data_age = current_time - max_datetime
        age_seconds = data_age.total_seconds()
    
    is_stale = age_seconds > max_age_seconds
    age_hours = age_seconds / 3600
    age_days = age_seconds / 86400
    
    if age_days >= 1:
        age_desc = f"{age_days:.1f} days"
    else:
        age_desc = f"{age_hours:.1f} hours"
    
    if is_stale:
        dq_message = f"Data is stale: most recent record in '{column_name}' is {age_desc} old (threshold: {threshold_desc})"
        # For freshness, quarantine doesn't make sense per-record - treat as warn with note
        effective_action = 'warn' if dq_result_if_not_compliant == 'quarantine' else dq_result_if_not_compliant
        if dq_result_if_not_compliant == 'quarantine':
            dq_message += " (entire batch flagged)"
        
        _record_dq_result(
            violation_count=1,  # Freshness is a batch-level metric, use 1 to indicate violation
            dq_category=data_quality_category,
            dq_message=dq_message,
            if_not_compliant=effective_action,
            warnings=warnings,
            force_failures=force_failures
        )
    else:
        pass_message = f"Data is fresh: most recent record in '{column_name}' is {age_desc} old (threshold: {threshold_desc})"
        _record_dq_result(
            violation_count=0,
            dq_category=data_quality_category,
            dq_message=pass_message,
            if_not_compliant=dq_result_if_not_compliant,
            warnings=warnings,
            force_failures=force_failures
        )
    
    return df, df_quarantined_all, warnings, force_failures


def _validate_completeness(
    df: DataFrame,
    df_quarantined_all: DataFrame,
    data_quality_step: dict,
    warnings: list,
    force_failures: list
) -> tuple:
    """Validate that columns meet minimum completeness (non-null percentage) thresholds.
    
    This function calculates the percentage of non-null values in specified columns
    and validates against configured thresholds. Each column generates its own
    separate warning/failure message for granular tracking.
    
    Args:
        df (DataFrame): Input DataFrame to validate
        df_quarantined_all (DataFrame): Accumulated quarantined records
        data_quality_step (dict): Configuration containing:
            - column_name: Column(s) to check completeness (comma-separated for multiple)
            - min_completeness_percent: Minimum required non-null percentage (0-100, default: 95)
            - if_not_compliant: Action when below threshold ('warn', 'fail')
        warnings (list): Accumulated warning messages
        force_failures (list): Accumulated failure messages
    
    Returns:
        tuple: (cleaned_df, quarantined_df, warnings, force_failures)
    
    Note:
        This check validates overall column completeness, not individual records.
        Useful for monitoring data quality trends and SLA compliance.
    """
    data_quality_category = data_quality_step["Category"]
    dq_result_if_not_compliant = data_quality_step.get('if_not_compliant', 'warn').lower()
    column_names_raw = data_quality_step['column_name']
    min_completeness = float(data_quality_step.get('min_completeness_percent', 95))
    
    # Parse comma-separated column names
    column_names = [col.strip() for col in column_names_raw.split(',')]
    
    log_and_print(f"Validating completeness on columns: {column_names} (min: {min_completeness}%)")
    
    total_count = df.count()
    if total_count == 0:
        _record_dq_result(
            violation_count=0,
            dq_category=data_quality_category,
            dq_message="No records to validate completeness",
            if_not_compliant='warn',
            warnings=warnings,
            force_failures=force_failures
        )
        return df, df_quarantined_all, warnings, force_failures
    
    # Process each column separately for granular tracking
    for col_name in column_names:
        non_null_count = df.filter(f.col(col_name).isNotNull()).count()
        completeness_pct = (non_null_count / total_count) * 100
        
        if completeness_pct < min_completeness:
            dq_message = f"Column '{col_name}' completeness {completeness_pct:.1f}% is below threshold {min_completeness}%"
            # For completeness, quarantine doesn't make sense per-record - treat as warn with note
            effective_action = 'warn' if dq_result_if_not_compliant == 'quarantine' else dq_result_if_not_compliant
            if dq_result_if_not_compliant == 'quarantine':
                dq_message += " (completeness is a batch-level metric)"
            
            _record_dq_result(
                violation_count=1,  # Completeness is a batch-level metric, use 1 to indicate violation
                dq_category=data_quality_category,
                dq_message=dq_message,
                if_not_compliant=effective_action,
                warnings=warnings,
                force_failures=force_failures,
                column_name=col_name
            )
        else:
            pass_message = f"Column '{col_name}' completeness {completeness_pct:.1f}% meets threshold {min_completeness}%"
            _record_dq_result(
                violation_count=0,
                dq_category=data_quality_category,
                dq_message=pass_message,
                if_not_compliant=dq_result_if_not_compliant,
                warnings=warnings,
                force_failures=force_failures,
                column_name=col_name
            )
    
    return df, df_quarantined_all, warnings, force_failures


def _resolve_anomaly_baseline_table(
    target_table_name: str,
    datastore_config: str | list
) -> str:
    """Resolve the full target table name with workspace prefix for baseline statistics.
    
    Args:
        target_table_name (str): Three-part table name (lakehouse.schema.table)
        datastore_config (str | list): Datastore configuration from Datastore_Configuration table
    
    Returns:
        str: Fully qualified table name with workspace
    
    Raises:
        KeyError: If the datastore is not found in the configuration
    """
    target_datastore_name = target_table_name.split('.')[0].strip().lower()
    target_workspace_name = _get_datastore_config(datastore_config, target_datastore_name, 'Workspace_Name')
    return f"`{target_workspace_name}`.{target_table_name.lower()}"


def _load_anomaly_baseline_data(
    full_baseline_table_name: str,
    column_name: str,
    minimum_baseline_records: int,
    data_quality_category: str,
    warnings: list
) -> tuple:
    """Load baseline data and validate it has sufficient records.
    
    Args:
        full_baseline_table_name (str): Fully qualified baseline table name
        column_name (str): Column to load for statistics calculation
        minimum_baseline_records (int): Minimum records required
        data_quality_category (str): Category for logging
        warnings (list): Accumulated warnings
    
    Returns:
        tuple: (baseline_df, should_skip, warnings) where should_skip is True if detection should be skipped
    """
    try:
        baseline_df = spark.sql(f"SELECT `{column_name}` FROM {full_baseline_table_name} WHERE `{column_name}` IS NOT NULL")
        baseline_count = baseline_df.count()
    except Exception as e:
        skip_message = f"Skipping anomaly detection: baseline table not accessible or column '{column_name}' not found. Error: {str(e)[:100]}"
        log_and_print(skip_message, "warn")
        json_to_append = {
            "outcome": "Skipped",
            "category": _get_dq_category_display_name(data_quality_category),
            "message": skip_message
        }
        warnings.append(json_to_append)
        return None, True, warnings
    
    if baseline_count < minimum_baseline_records:
        skip_message = f"Skipping anomaly detection: baseline table has {baseline_count} records, minimum required is {minimum_baseline_records}."
        log_and_print(skip_message, "warn")
        json_to_append = {
            "outcome": "Skipped",
            "category": _get_dq_category_display_name(data_quality_category),
            "message": skip_message
        }
        warnings.append(json_to_append)
        return None, True, warnings
    
    return baseline_df, False, warnings


def _calculate_zscore_statistics(
    baseline_df: DataFrame,
    column_name: str,
    data_quality_category: str,
    warnings: list
) -> tuple:
    """Calculate mean and standard deviation for z-score anomaly detection.
    
    Args:
        baseline_df (DataFrame): Baseline data for statistics calculation
        column_name (str): Column to calculate statistics for
        data_quality_category (str): Category for logging
        warnings (list): Accumulated warnings
    
    Returns:
        tuple: (mean_val, stddev_val, should_skip, warnings)
    """
    stats = baseline_df.agg(
        f.mean(column_name).alias("mean_val"),
        f.stddev(column_name).alias("stddev_val")
    ).first()
    
    mean_val = stats["mean_val"]
    stddev_val = stats["stddev_val"]
    
    if stddev_val is None or stddev_val == 0:
        skip_message = f"Skipping anomaly detection: standard deviation is zero or null for column '{column_name}'."
        log_and_print(skip_message, "warn")
        json_to_append = {
            "outcome": "Skipped",
            "category": _get_dq_category_display_name(data_quality_category),
            "message": skip_message
        }
        warnings.append(json_to_append)
        return None, None, True, warnings
    
    stats_log_info = f"Baseline statistics: mean={mean_val:.4f}, stddev={stddev_val:.4f}"
    log_and_print(stats_log_info)
    
    return mean_val, stddev_val, False, warnings


def _calculate_iqr_statistics(
    baseline_df: DataFrame,
    column_name: str,
    threshold: float
) -> tuple:
    """Calculate IQR bounds for anomaly detection.
    
    Args:
        baseline_df (DataFrame): Baseline data for statistics calculation
        column_name (str): Column to calculate statistics for
        threshold (float): IQR multiplier for bounds
    
    Returns:
        tuple: (lower_bound, upper_bound, q1, q3, iqr)
    """
    quantiles = baseline_df.approxQuantile(column_name, [0.25, 0.75], 0.01)
    q1 = quantiles[0]
    q3 = quantiles[1]
    iqr = q3 - q1
    
    lower_bound = q1 - threshold * iqr
    upper_bound = q3 + threshold * iqr
    
    stats_log_info = f"Baseline statistics: Q1={q1:.4f}, Q3={q3:.4f}, IQR={iqr:.4f}, bounds=[{lower_bound:.4f}, {upper_bound:.4f}]"
    log_and_print(stats_log_info)
    
    return lower_bound, upper_bound, q1, q3, iqr


def _find_zscore_anomalies(
    df: DataFrame,
    column_name: str,
    mean_val: float,
    stddev_val: float,
    threshold: float
) -> tuple:
    """Find anomalies using z-score method.
    
    Args:
        df (DataFrame): Input data to check
        column_name (str): Column to analyze
        mean_val (float): Mean from baseline
        stddev_val (float): Standard deviation from baseline
        threshold (float): Z-score threshold
    
    Returns:
        tuple: (anomalies_df, boundary_description)
    """
    anomalies = df.filter(
        (f.col(column_name).isNotNull()) &
        (f.abs(f.col(column_name) - mean_val) / stddev_val > threshold)
    )
    
    boundary_desc = f"z-score > {threshold} (mean: {mean_val:.2f}, stddev: {stddev_val:.2f})"
    
    return anomalies, boundary_desc


def _find_iqr_anomalies(
    df: DataFrame,
    column_name: str,
    lower_bound: float,
    upper_bound: float
) -> tuple:
    """Find anomalies using IQR method.
    
    Args:
        df (DataFrame): Input data to check
        column_name (str): Column to analyze
        lower_bound (float): Lower IQR bound
        upper_bound (float): Upper IQR bound
    
    Returns:
        tuple: (anomalies_df, boundary_description)
    """
    anomalies = df.filter(
        (f.col(column_name).isNotNull()) &
        ((f.col(column_name) < lower_bound) | (f.col(column_name) > upper_bound))
    )
    
    boundary_desc = f"outside IQR bounds [{lower_bound:.2f}, {upper_bound:.2f}]"
    
    return anomalies, boundary_desc


def _handle_anomaly_results(
    df: DataFrame,
    df_quarantined_all: DataFrame,
    anomalies: DataFrame,
    column_name: str,
    boundary_desc: str,
    data_quality_category: str,
    dq_result_if_not_compliant: str,
    warnings: list,
    force_failures: list
) -> tuple:
    """Handle anomaly detection results based on compliance action.
    
    This function handles quarantine logic (using delta__idx join) and delegates
    result recording to _record_dq_result.
    
    Args:
        df (DataFrame): Input data
        df_quarantined_all (DataFrame): Accumulated quarantined records
        anomalies (DataFrame): Detected anomalies
        column_name (str): Column that was analyzed
        boundary_desc (str): Description of boundaries used
        data_quality_category (str): Category for logging
        dq_result_if_not_compliant (str): Action to take ('warn', 'quarantine', 'fail')
        warnings (list): Accumulated warnings
        force_failures (list): Accumulated failures
    
    Returns:
        tuple: (df, df_quarantined_all, warnings, force_failures)
    """
    number_of_anomalies = anomalies.count()
    dq_message = f"Found {number_of_anomalies} anomalies in column '{column_name}' ({boundary_desc})."
    
    # Handle quarantine if needed (must happen before recording result)
    if number_of_anomalies > 0 and dq_result_if_not_compliant == 'quarantine':
        quarantine_message = f"Quarantined because column '{column_name}' value is a statistical anomaly ({boundary_desc})."
        df, df_quarantined_all = _quarantine_rows(
            df=df,
            df_quarantined_all=df_quarantined_all,
            df_violations=anomalies,
            quarantine_reason=quarantine_message,
            join_keys='delta__idx'
        )
    
    # Record the DQ result
    _record_dq_result(
        violation_count=number_of_anomalies,
        dq_category=data_quality_category,
        dq_message=dq_message,
        if_not_compliant=dq_result_if_not_compliant,
        warnings=warnings,
        force_failures=force_failures,
        column_name=column_name
    )
    
    return df, df_quarantined_all, warnings, force_failures


def _validate_anomaly_single_column(
    df: DataFrame,
    df_quarantined_all: DataFrame,
    data_quality_step: dict,
    column_name: str,
    full_baseline_table_name: str,
    warnings: list,
    force_failures: list
) -> tuple:
    """Process anomaly detection for a single column.
    
    Args:
        df (DataFrame): Input data to check for anomalies
        df_quarantined_all (DataFrame): Accumulated quarantined records
        data_quality_step (dict): Configuration containing method, threshold, if_not_compliant, etc.
        column_name (str): Single column name to analyze
        full_baseline_table_name (str): Fully qualified baseline table name
        warnings (list): Accumulated warnings
        force_failures (list): Accumulated failures
    
    Returns:
        tuple: (df, df_quarantined_all, warnings, force_failures)
    """
    # Parse configuration from data_quality_step
    data_quality_category = data_quality_step.get("Category", "validate_anomaly")
    dq_result_if_not_compliant = data_quality_step.get('if_not_compliant', 'warn').lower()
    method = data_quality_step.get('method', 'z_score').lower()
    threshold = float(data_quality_step.get('threshold', '3' if method == 'z_score' else '1.5'))
    minimum_baseline_records = int(data_quality_step.get('minimum_baseline_records', '30'))
    
    detecting_anomaly_log_info = f"Detecting anomalies in column '{column_name}' using {method} method (threshold: {threshold})."
    log_and_print(detecting_anomaly_log_info)
    
    # Load baseline data for this column
    baseline_df, should_skip, warnings = _load_anomaly_baseline_data(
        full_baseline_table_name=full_baseline_table_name,
        column_name=column_name,
        minimum_baseline_records=minimum_baseline_records,
        data_quality_category=data_quality_category,
        warnings=warnings
    )
    
    if should_skip:
        return df, df_quarantined_all, warnings, force_failures
    
    # Calculate statistics and find anomalies based on method
    if method == 'z_score':
        mean_val, stddev_val, should_skip, warnings = _calculate_zscore_statistics(
            baseline_df=baseline_df,
            column_name=column_name,
            data_quality_category=data_quality_category,
            warnings=warnings
        )
        
        if should_skip:
            return df, df_quarantined_all, warnings, force_failures
        
        anomalies, boundary_desc = _find_zscore_anomalies(
            df=df,
            column_name=column_name,
            mean_val=mean_val,
            stddev_val=stddev_val,
            threshold=threshold
        )
        
    elif method == 'iqr':
        lower_bound, upper_bound, q1, q3, iqr = _calculate_iqr_statistics(
            baseline_df=baseline_df,
            column_name=column_name,
            threshold=threshold
        )
        
        anomalies, boundary_desc = _find_iqr_anomalies(
            df=df,
            column_name=column_name,
            lower_bound=lower_bound,
            upper_bound=upper_bound
        )
        
    else:
        raise ValueError(f"Unknown anomaly detection method '{method}'. Supported methods: 'z_score', 'iqr'")
    
    # Handle results
    return _handle_anomaly_results(
        df=df,
        df_quarantined_all=df_quarantined_all,
        anomalies=anomalies,
        column_name=column_name,
        boundary_desc=boundary_desc,
        data_quality_category=data_quality_category,
        dq_result_if_not_compliant=dq_result_if_not_compliant,
        warnings=warnings,
        force_failures=force_failures
    )


def _validate_anomaly(
    df: DataFrame,
    df_quarantined_all: DataFrame,
    data_quality_step: dict,
    target_table_name: str,
    datastore_config: str | list,
    warnings: list,
    force_failures: list
) -> tuple:
    """Detect statistical anomalies in numeric columns using z-score or IQR methods.
    
    This function identifies outliers by comparing incoming data against the target table's
    historical statistical distribution. It supports two detection methods:
    - Z-Score: Flags values more than N standard deviations from the mean
    - IQR (Interquartile Range): Flags values outside Q1 - k*IQR or Q3 + k*IQR
    
    The target table provides baseline data for calculating statistics. If the target table
    doesn't exist (first load) or has insufficient records, anomaly detection is gracefully skipped.
    
    Supports multiple columns via comma-separated values in column_name.
    
    Args:
        df (DataFrame): Input data to check for anomalies
        df_quarantined_all (DataFrame): Accumulated quarantined records
        data_quality_step (dict): Configuration with:
            - column_name: Numeric column(s) to analyze (comma-separated for multiple)
            - method: 'z_score' or 'iqr'
            - threshold: Z-score threshold (default 3) or IQR multiplier (default 1.5)
            - if_not_compliant: 'warn', 'quarantine', or 'fail'
            - minimum_baseline_records: Minimum records needed (default 30)
        target_table_name (str): Target table name used as baseline for statistics
        datastore_config (str | list): Datastore configuration from Datastore_Configuration table
        warnings (list): Accumulated warnings
        force_failures (list): Accumulated failures
    
    Returns:
        tuple: (df, df_quarantined_all, warnings, force_failures)
    
    Detection Methods:
        - z_score: |value - mean| / stddev > threshold → anomaly
        - iqr: value < Q1 - threshold*IQR OR value > Q3 + threshold*IQR → anomaly
    """
    # Extract only the configuration needed at this level
    column_names_raw = data_quality_step['column_name']
    method = data_quality_step.get('method', 'z_score').lower()
    threshold = float(data_quality_step.get('threshold', '3' if method == 'z_score' else '1.5'))
    
    # Parse comma-separated column names
    column_names = [col.strip() for col in column_names_raw.split(',') if col.strip()]
    
    detecting_anomaly_log_info = f"Detecting anomalies in column(s) '{column_names}' using {method} method (threshold: {threshold}). Baseline: {target_table_name}"
    log_and_print(detecting_anomaly_log_info)
    
    # Validate all columns exist in incoming data
    _validate_columns_exist(
        df=df,
        columns_to_check=column_names,
        operation_name="validate_anomaly"
    )
    
    # Resolve target table with workspace for baseline statistics
    full_baseline_table_name = _resolve_anomaly_baseline_table(
        target_table_name=target_table_name,
        datastore_config=datastore_config
    )
    
    # Process each column
    for column_name in column_names:
        df, df_quarantined_all, warnings, force_failures = _validate_anomaly_single_column(
            df=df,
            df_quarantined_all=df_quarantined_all,
            data_quality_step=data_quality_step,
            column_name=column_name,
            full_baseline_table_name=full_baseline_table_name,
            warnings=warnings,
            force_failures=force_failures
        )
    
    return df, df_quarantined_all, warnings, force_failures


def _get_pattern_validation_regex(pattern_type: str) -> str:
    """
    Get regex pattern for common data validation patterns.
    
    Args:
        pattern_type (str): The type of pattern to validate against
    
    Returns:
        str: Regex pattern string for the specified pattern type
    
    Raises:
        ValueError: If pattern_type is not recognized
    
    Notes:
        - All patterns use ^ and $ anchors for full string matching
        - ip_address pattern accepts both IPv4 and IPv6 formats
        - Date patterns validate format but not actual date validity (e.g., allows 02/31/2024)
        - Credit card pattern validates structure only, not actual card validity
        - Email pattern is permissive and follows common format rules
    """
    patterns = {
        # US Phone: Supports (123) 456-7890, 123-456-7890, 123.456.7890, or 1234567890
        'us_phone': r'^\(?([0-9]{3})\)?[-. ]?([0-9]{3})[-. ]?([0-9]{4})$',
        
        # Email: Standard email format (allows letters, numbers, and common special chars)
        'email': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
        
        # US ZIP: 5 digits or 5+4 format (12345 or 12345-6789)
        'us_zip': r'^\d{5}(-\d{4})?$',
        
        # US SSN: Format XXX-XX-XXXX (requires hyphens)
        'us_ssn': r'^\d{3}-\d{2}-\d{4}$',
        
        # Date MM/DD/YYYY: Validates format only, not actual date validity
        'date_mmddyyyy': r'^(0[1-9]|1[0-2])\/(0[1-9]|[12][0-9]|3[01])\/\d{4}$',
        
        # Date YYYY-MM-DD: Validates format only, not actual date validity
        'date_yyyymmdd': r'^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$',
        
        # URL: HTTP/HTTPS URLs with optional www prefix
        'url': r'^https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&/=]*)$',
        
        # Credit Card: 16 digits with optional hyphens or spaces between groups of 4
        'credit_card': r'^\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}$',
        
        # IP Address: Accepts both IPv4 and IPv6 addresses
        # IPv4: 192.168.1.1 (validates octets 0-255)
        # IPv6: 2001:db8::8a2e:370:7334 (full and compressed formats)
        'ip_address': r'^(((25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])\.){3}(25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])|(([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])))$',
        
        # IPv4 Address Only: Validates each octet is 0-255
        'ipv4_address': r'^((25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])\.){3}(25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])$',
        
        # IPv6 Address Only: Full and compressed formats
        'ipv6_address': r'^(([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))$',
        
        # Alpha Only: Only alphabetic characters (uppercase and lowercase)
        'alpha_only': r'^[A-Za-z]+$',
        
        # Alphanumeric Only: Letters and numbers only
        'alphanumeric_only': r'^[A-Za-z0-9]+$',
        
        # Numeric Only: Digits only (no decimals or negative signs)
        'numeric_only': r'^\d+$',
        
        # GUID/UUID: Standard format with hyphens (8-4-4-4-12)
        'guid': r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$',

        # UK Phone: Supports 01612949234, +447895891235 (including any spaces variations after the first 2 digits)
        'uk_phone': r'^(?:0|\+?44\s?)(?:\d\s?){9,10}$',

        # UK Postcode: 6 to 8 characters, with or without central space and case-insensitive (EC1A 1BB, SK12TN, m20 1hs)
        'uk_postcode': r'(?i)^[A-Z]{1,2}[0-9][A-Z0-9]?\s?[0-9][A-Z]{2}$',
    }
    
    if pattern_type not in patterns:
        available_patterns = ', '.join(patterns.keys())
        raise ValueError(f"Unknown pattern type '{pattern_type}'. Available patterns: {available_patterns}")
    
    return patterns[pattern_type]

def _validate_pattern_single_column(
    df: DataFrame,
    df_quarantined_all: DataFrame,
    column_name: str,
    pattern_type: str,
    regex_pattern: str,
    allow_null: bool,
    data_quality_category: str,
    dq_result_if_not_compliant: str,
    warnings: list,
    force_failures: list
) -> tuple:
    """Validate a single column against a pattern.
    
    Args:
        df (DataFrame): Input data to validate
        df_quarantined_all (DataFrame): Accumulated quarantined records
        column_name (str): Single column name to validate
        pattern_type (str): Type of pattern for logging
        regex_pattern (str): Regex pattern to match against
        allow_null (bool): Whether null values are allowed
        data_quality_category (str): Category for logging
        dq_result_if_not_compliant (str): Action to take
        warnings (list): Accumulated warnings
        force_failures (list): Accumulated failures
    
    Returns:
        tuple: (df, df_quarantined_all, warnings, force_failures)
    """
    validating_pattern_log_info = f"Validating column '{column_name}' matches pattern '{pattern_type}'. Allow null: {allow_null}."
    log_and_print(validating_pattern_log_info)
    
    # Find records that DON'T match the pattern
    # Convert column to string and check against regex
    if allow_null:
        # Records that fail: non-null values that don't match the pattern
        pattern_not_matches = df.filter(
            (f.col(column_name).isNotNull()) & 
            (~f.col(column_name).cast("string").rlike(regex_pattern))
        )
    else:
        # Records that fail: null values OR values that don't match the pattern
        pattern_not_matches = df.filter(
            (f.col(column_name).isNull()) | 
            (~f.col(column_name).cast("string").rlike(regex_pattern))
        )
    
    number_of_pattern_not_matches = pattern_not_matches.count()
    dq_message = f"There are {number_of_pattern_not_matches} rows where column '{column_name}' did NOT match the '{pattern_type}' pattern."
    
    # Handle quarantine if needed (must happen before recording result)
    if number_of_pattern_not_matches > 0 and dq_result_if_not_compliant == 'quarantine':
        quarantine_message = f"Quarantined because column '{column_name}' did not match the '{pattern_type}' pattern (regex: {regex_pattern})."
        df, df_quarantined_all = _quarantine_rows(
            df=df,
            df_quarantined_all=df_quarantined_all,
            df_violations=pattern_not_matches,
            quarantine_reason=quarantine_message,
            join_keys='delta__idx'
        )
    
    # Record the DQ result
    _record_dq_result(
        violation_count=number_of_pattern_not_matches,
        dq_category=data_quality_category,
        dq_message=dq_message,
        if_not_compliant=dq_result_if_not_compliant,
        warnings=warnings,
        force_failures=force_failures,
        column_name=column_name
    )
    
    return df, df_quarantined_all, warnings, force_failures


def _validate_pattern(
    df: DataFrame, 
    df_quarantined_all: DataFrame, 
    data_quality_step: dict, 
    warnings: list, 
    force_failures: list
) -> tuple:
    """Validate data using templated pattern matching or custom regex.
    
    This function provides prescriptive validation for common data patterns like
    phone numbers, emails, dates, etc. using predefined regex patterns.
    
    Supports multiple columns via comma-separated values in column_name.
    
    Args:
        df (DataFrame): Input data to validate
        df_quarantined_all (DataFrame): Accumulated quarantined records
        data_quality_step (dict): Configuration with:
            - column_name: Column(s) to validate (comma-separated for multiple)
            - pattern_type: A predefined pattern type (see supported patterns below)
            - if_not_compliant: Action to take ('warn', 'quarantine', 'fail')
            - allow_null: Whether null values are allowed (default: True)
        warnings (list): Accumulated warnings
        force_failures (list): Accumulated failures
    
    Returns:
        tuple: (df, df_quarantined_all, warnings, force_failures)
    
    Supported Pattern Types:
        - us_phone: US phone numbers (formats: 123-456-7890, (123) 456-7890, 1234567890)
        - uk_phone: UK phone numbers (formats: 0161 2949234, +447895891235, 07896789812)
        - email: Email addresses
        - us_zip: US ZIP codes (5 or 9 digit)
        - uk_postcode: UK Postcodes (formats: EC1A 1BB, M201HS)
        - us_ssn: US Social Security Numbers (XXX-XX-XXXX)
        - date_mmddyyyy: Dates in MM/DD/YYYY format
        - date_yyyymmdd: Dates in YYYY-MM-DD format
        - url: HTTP/HTTPS URLs
        - credit_card: Credit card numbers (with or without spaces/hyphens)
        - ip_address: IPv4 or IPv6 addresses (supports both formats)
        - ipv4_address: IPv4 addresses only (e.g., 192.168.1.1)
        - ipv6_address: IPv6 addresses only (e.g., 2001:db8::8a2e:370:7334)
        - alpha_only: Alphabetic characters only
        - alphanumeric_only: Alphanumeric characters only
        - numeric_only: Numeric characters only
        - guid: GUIDs/UUIDs
    """
    data_quality_category = data_quality_step.get("Category", "validate_pattern")
    dq_result_if_not_compliant = data_quality_step.get('if_not_compliant', 'warn').lower()
    column_names_raw = data_quality_step['column_name']
    pattern_type = data_quality_step['pattern_type']
    allow_null = data_quality_step.get('allow_null', True)
    
    # Parse comma-separated column names
    column_names = [col.strip() for col in column_names_raw.split(',') if col.strip()]
    
    validating_pattern_log_info = f"Validating column(s) '{column_names}' match pattern '{pattern_type}'. Allow null: {allow_null}."
    log_and_print(validating_pattern_log_info)
    
    # Validate all columns exist
    _validate_columns_exist(
        df=df,
        columns_to_check=column_names,
        operation_name="pattern_match_validation"
    )
    
    # Get the regex pattern from predefined patterns
    regex_pattern = _get_pattern_validation_regex(pattern_type)
    
    # Process each column
    for column_name in column_names:
        df, df_quarantined_all, warnings, force_failures = _validate_pattern_single_column(
            df=df,
            df_quarantined_all=df_quarantined_all,
            column_name=column_name,
            pattern_type=pattern_type,
            regex_pattern=regex_pattern,
            allow_null=allow_null,
            data_quality_category=data_quality_category,
            dq_result_if_not_compliant=dq_result_if_not_compliant,
            warnings=warnings,
            force_failures=force_failures
        )
    
    return df, df_quarantined_all, warnings, force_failures

def _data_quality_checks(
    df: DataFrame, 
    data_quality_steps: list, 
    primary_keys: list, 
    if_duplicate_primary_keys: str, 
    columns_to_add_to_pks: list, 
    target_table_name: str, 
    datastore_config: str | list
) -> tuple:
    """
    Execute comprehensive data quality validation with configurable actions.

    This function implements a multi-stage data quality framework that:
    1. Validates data against defined business rules
    2. Quarantines or flags problematic records
    3. Generates detailed quality metrics for monitoring
    4. Supports both warning and failure modes

    Args:
        df (DataFrame): Input data to validate
        data_quality_steps (list): List of configured quality check definitions
        primary_keys (list): Primary key columns for uniqueness validation
        if_duplicate_primary_keys (str): Action for duplicates ('warn', 'quarantine', or fail)
        columns_to_add_to_pks (list): Additional columns for composite key validation
        target_table_name (str): target table name. used to determine if the output is a table or file
        datastore_config (str | list): Datastore configuration from Datastore_Configuration table

    Returns:
        tuple: (warnings list, failures list, cleaned DataFrame, quarantined DataFrame)

    Quality Check Types:
        1. Corrupt Records: Identifies records that don't conform to schema
        2. Primary Key Validation: Ensures key uniqueness
        3. validate_condition: Custom SQL-based validation rules
        4. validate_pattern: Templated pattern validation (phone, email, etc.)
        5. validate_batch_size: Validates batch meets minimum row count
        6. validate_referential_integrity: Referential integrity checks (foreign key validation)
        7. validate_anomaly: Statistical anomaly detection (z-score, IQR)
        8. validate_not_null: NULL value validation
        9. validate_unique: Uniqueness validation (outside primary keys)
        10. validate_range: Numeric/date range validation
        11. validate_freshness: Data freshness/SLA validation
        12. validate_completeness: Column completeness percentage validation

    Quarantine Strategy:
        - Each quarantined record includes a detailed reason
        - Records are removed from main dataset when quarantined
        - Quarantine data preserved for analysis and remediation
    """
    df = df.withColumn("delta__idx", f.monotonically_increasing_id())
    
    warnings = []
    force_failures = []

    # Handle corrupt records
    df, df_quarantined_all, warnings = _handle_corrupt_records(
        df = df, 
        warnings = warnings
    )

    # Validate primary keys
    df, df_quarantined_all, warnings, force_failures = _validate_primary_keys(
        df = df, 
        df_quarantined_all = df_quarantined_all, 
        primary_keys = primary_keys, 
        if_duplicate_primary_keys = if_duplicate_primary_keys, 
        columns_to_add_to_pks = columns_to_add_to_pks, 
        target_table_name = target_table_name, 
        warnings = warnings, 
        force_failures = force_failures
    )

    # Process configured data quality steps
    for i, data_quality_step in enumerate(data_quality_steps):
        data_quality_category = data_quality_step["Category"]
        
        if data_quality_category == "validate_condition":
            df, df_quarantined_all, warnings, force_failures = _validate_condition(
                df = df, 
                df_quarantined_all = df_quarantined_all, 
                data_quality_step = data_quality_step, 
                warnings = warnings, 
                force_failures = force_failures
            )
        elif data_quality_category == "validate_pattern":
            df, df_quarantined_all, warnings, force_failures = _validate_pattern(
                df = df, 
                df_quarantined_all = df_quarantined_all, 
                data_quality_step = data_quality_step, 
                warnings = warnings, 
                force_failures = force_failures
            )
        elif data_quality_category == "validate_batch_size":
            warnings, force_failures = _validate_batch_size(
                df = df, 
                data_quality_step = data_quality_step, 
                warnings = warnings, 
                force_failures = force_failures
            )
        elif data_quality_category == "validate_referential_integrity":
            df, df_quarantined_all, warnings, force_failures = _validate_referential_integrity(
                df = df, 
                df_quarantined_all = df_quarantined_all, 
                data_quality_step = data_quality_step, 
                datastore_config = datastore_config, 
                warnings = warnings, 
                force_failures = force_failures
            )
        elif data_quality_category == "validate_anomaly":
            df, df_quarantined_all, warnings, force_failures = _validate_anomaly(
                df = df,
                df_quarantined_all = df_quarantined_all,
                data_quality_step = data_quality_step,
                target_table_name = target_table_name,
                datastore_config = datastore_config,
                warnings = warnings,
                force_failures = force_failures
            )
        elif data_quality_category == "validate_not_null":
            df, df_quarantined_all, warnings, force_failures = _validate_not_null(
                df = df,
                df_quarantined_all = df_quarantined_all,
                data_quality_step = data_quality_step,
                warnings = warnings,
                force_failures = force_failures
            )
        elif data_quality_category == "validate_unique":
            df, df_quarantined_all, warnings, force_failures = _validate_unique(
                df = df,
                df_quarantined_all = df_quarantined_all,
                data_quality_step = data_quality_step,
                warnings = warnings,
                force_failures = force_failures
            )
        elif data_quality_category == "validate_range":
            df, df_quarantined_all, warnings, force_failures = _validate_range(
                df = df,
                df_quarantined_all = df_quarantined_all,
                data_quality_step = data_quality_step,
                warnings = warnings,
                force_failures = force_failures
            )
        elif data_quality_category == "validate_freshness":
            df, df_quarantined_all, warnings, force_failures = _validate_freshness(
                df = df,
                df_quarantined_all = df_quarantined_all,
                data_quality_step = data_quality_step,
                warnings = warnings,
                force_failures = force_failures
            )
        elif data_quality_category == "validate_completeness":
            df, df_quarantined_all, warnings, force_failures = _validate_completeness(
                df = df,
                df_quarantined_all = df_quarantined_all,
                data_quality_step = data_quality_step,
                warnings = warnings,
                force_failures = force_failures
            )
        else:
            raise Exception(f"No logic currently exists to run the data quality check, {data_quality_category}")
    
    df = df.drop("delta__idx")
    df_quarantined_all = df_quarantined_all.drop("delta__idx")

    if warnings:
        data_quality_warnings_log_warn = f"Data Quality Warnings: {warnings}"
        log_and_print(data_quality_warnings_log_warn, "warn")
    
    if force_failures:
        data_quality_failures_log_error = f"Data Quality Failures: {force_failures}"
        log_and_print(data_quality_failures_log_error, "error")

    return warnings, force_failures, df, df_quarantined_all

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def reorder_columns_for_output(
    new_data: DataFrame
) -> DataFrame:
    """
    Reorder DataFrame columns by moving system columns to the end.
    
    This function organizes columns into three groups for improved readability:
    1. Business/data columns (first)
    2. SCD2 tracking columns (middle)
    3. Delta/system metadata columns (last)
    
    Args:
        new_data (DataFrame): Input DataFrame with mixed column types
    
    Returns:
        DataFrame: DataFrame with reordered columns
    """
    # move scd2 and delta columns to back
    data_columns = [col for col in new_data.columns if not col.startswith('delta__') and not col.startswith('scd_')]
    scd_columns = [col for col in new_data.columns if col.startswith('scd_')]
    delta_columns = [col for col in new_data.columns if col.startswith('delta__')]

    # Combine all lists
    all_columns = data_columns + scd_columns + delta_columns

    # Select columns
    new_data = new_data.select(*all_columns)
    return new_data

def execute_data_quality_checks(
    new_data: DataFrame, 
    data_quality_steps: list, 
    if_duplicate_primary_keys: str, 
    primary_keys: list, 
    target_table_name: str, 
    datastore_config: str | list, 
    merge_in_batches_with_columns: List[str]
) -> tuple:
    """
    Execute data quality validation with optional batch-specific primary key handling.
    
    This wrapper function orchestrates data quality checks and adjusts primary key
    validation when data is being written in batches.
    
    Args:
        new_data (DataFrame): Data to validate
        data_quality_steps (list): List of quality check configurations
        if_duplicate_primary_keys (str): Action for duplicates ('warn', 'quarantine', or fail)
        primary_keys (list): Primary key columns
        target_table_name (str): Target table name
        datastore_config (str | list): Datastore configuration from Datastore_Configuration table
        merge_in_batches_with_columns (List[str]): Batch columns to include in PK validation
    
    Returns:
        tuple: (warnings list, failures list, cleaned DataFrame, quarantined DataFrame)
    """
    # If data is being batched by file
    if merge_in_batches_with_columns:
        primary_key_update_log_info = f"Writing data in batches with columns: {merge_in_batches_with_columns}. These columns will be included in primary key validation."
        log_and_print(primary_key_update_log_info)
        columns_to_add_to_pks = merge_in_batches_with_columns
    else:
        columns_to_add_to_pks = []

    # Execute data quality checks and capture results
    dq_warnings, dq_force_failures, new_data, quarantined_data = _data_quality_checks(
        df = new_data,
        data_quality_steps = data_quality_steps,
        if_duplicate_primary_keys = if_duplicate_primary_keys,
        primary_keys = primary_keys,
        columns_to_add_to_pks = columns_to_add_to_pks,
        target_table_name = target_table_name,
        datastore_config = datastore_config
    )
    return dq_warnings, dq_force_failures, new_data, quarantined_data

def analyze_schema_changes(
    new_data: DataFrame, 
    last_schema_id: str, 
    last_schema_details: list
) -> tuple:
    """
    Analyze schema changes between current and previous DataFrame versions.
    
    Compares the current DataFrame schema against the last known schema to detect:
    - Schema hash changes
    - Column additions
    - Column removals
    - Data type modifications
    
    Args:
        new_data (DataFrame): Current DataFrame to analyze
        last_schema_id (str): MD5 hash of the previous schema
        last_schema_details (list): Previous schema definition as list of (column, datatype) tuples
    
    Returns:
        tuple: (new_schema flag, schema details dict, schema hash, change summary, 
                type update summary, changed column types list)
    """
    # Extract and analyze schema information
    new_data_schema = [(field.name, field.dataType.simpleString()) for field in new_data.schema.fields]
    new_data_schema_string = str(new_data_schema)
    new_data_schema_hash = get_md5_of_string(new_data_schema_string)

    # Determine if schema has changed
    if new_data_schema_hash == last_schema_id:
        new_schema = False
    else:
        new_schema = True

    # Calculate schema differences
    schema_changes = get_schema_changes(new_schema = new_data_schema, old_schema = last_schema_details)

    # Prepare schema details for logging
    if new_schema and last_schema_id:
        new_schema_details = {"schema_details": new_data_schema_string, "schema_changes": schema_changes }
        column_types_that_changed = [column_update for column_update in schema_changes if column_update['change_type'] == 'Column Type Changed']
        columns_added = [column_update for column_update in schema_changes if column_update['change_type'] == 'Column Added']
        columns_dropped = [column_update for column_update in schema_changes if column_update['change_type'] == 'Column Dropped']

        schema_change_summary = ""
        schema_type_updates_summary = ""

        if columns_added:
            schema_change_summary += "Columns Added - "
            schema_change_summary += "; ".join(f"{col['column']} ({col['data_type']})" for col in columns_added)
            schema_change_summary += ". "

        if columns_dropped:
            schema_change_summary += "Columns Dropped - "
            schema_change_summary += "; ".join(f"{col['column']} ({col['data_type']})" for col in columns_dropped)
            schema_change_summary += ". "

        if column_types_that_changed:
            schema_type_updates_summary += "Column Types Changed - "
            schema_type_updates_summary += "; ".join(f"{col['column']} → {col['data_type']}" for col in column_types_that_changed)
            schema_type_updates_summary += ". "
            schema_change_summary += schema_type_updates_summary

    else:
        new_schema_details = {"schema_details": new_data_schema_string, "schema_changes": ""}
        schema_change_summary = ""
        schema_type_updates_summary = ""
        column_types_that_changed = ""
        
    return new_schema, new_schema_details, new_data_schema_hash, schema_change_summary, schema_type_updates_summary, column_types_that_changed

def handle_schema_change_failures(
    new_schema: bool, 
    fail_on_new_schema: bool, 
    first_run: bool, 
    last_schema_id: str, 
    schema_change_summary: str, 
    fail_on_column_data_type_change: bool, 
    column_types_that_changed: list, 
    schema_type_updates_summary: str, 
    dq_warnings: list, 
    dq_force_failures: list
) -> tuple:
    """
    Handle schema change validation based on configuration flags.
    
    Determines whether schema changes should result in failures, warnings, or be allowed.
    Supports different strictness levels for schema evolution vs data type changes.
    
    Args:
        new_schema (bool): Whether schema has changed
        fail_on_new_schema (bool): Whether to fail on any schema change
        first_run (bool): Whether this is the initial load
        last_schema_id (str): Previous schema hash
        schema_change_summary (str): Human-readable description of changes
        fail_on_column_data_type_change (bool): Whether to fail specifically on type changes
        column_types_that_changed (list): List of columns with type changes
        schema_type_updates_summary (str): Summary of type changes
        dq_warnings (list): Accumulated warnings list
        dq_force_failures (list): Accumulated failures list
    
    Returns:
        tuple: (updated warnings list, updated failures list)
    """
    # Handle schema change failures
    schema_change_category = "Table Definition Updated"
    if new_schema and fail_on_new_schema and not first_run and last_schema_id:
        dq_schema_force_failure_message = f"Schema has changed. {schema_change_summary}."

        schema_change_failure_log_error = f"Schema change failure: {dq_schema_force_failure_message}"
        log_and_print(schema_change_failure_log_error, "error")

        json_to_append = {"category": schema_change_category, "message": dq_schema_force_failure_message}
        dq_force_failures.append(json_to_append)

    elif new_schema and fail_on_column_data_type_change and last_schema_id and not first_run and column_types_that_changed:
        dq_schema_force_failure_message = f"Column Data Types have changed. {schema_type_updates_summary}."

        column_type_update_failure_log_error = f"Column type update failure: {dq_schema_force_failure_message}"
        log_and_print(column_type_update_failure_log_error, "error")

        json_to_append = {"category": schema_change_category, "message": dq_schema_force_failure_message}
        dq_force_failures.append(json_to_append)

    elif new_schema and not first_run and last_schema_id:
        dq_schema_warn_message = f"Schema has changed. {schema_change_summary}."

        schema_change_warning_log_warn = f"Schema change warning: {dq_schema_warn_message}"
        log_and_print(schema_change_warning_log_warn, "warn")

        json_to_append = {"outcome": "Warning", "category": schema_change_category, "message": dq_schema_warn_message, "quarantined": "No"}
        dq_warnings.append(json_to_append)
    
    return dq_warnings, dq_force_failures

def finalize_processing(
    dq_force_failures: list, 
    source_details: str, 
    dq_warnings: list, 
    new_schema: bool, 
    new_data: DataFrame, 
    target_datastore_medallion_name: str, 
    new_data_schema_hash: str,
    lineage_info: dict
) -> tuple:
    """
    Finalize data processing with failure handling and column reordering.
    
    Performs final checks and formatting before data is written:
    - Exits notebook if critical data quality failures exist
    - Reorders columns to place system metadata at the end
    - Logs warnings and schema changes
    
    Args:
        dq_force_failures (list): List of critical data quality failures
        source_details (str): Source configuration for error reporting
        dq_warnings (list): List of data quality warnings
        new_schema (bool): Whether schema has changed
        new_data (DataFrame): Processed data
        target_datastore_medallion_name (str): Target layer name
        new_data_schema_hash (str): Current schema hash
        lineage_info (dict): Lineage tracking information for logging
    
    Returns:
        tuple: (processed DataFrame, warnings list, schema hash, schema change flag)
    """
    # Exit notebook if critical data quality failures detected
    if dq_force_failures:
        notebookutils.notebook.exit({
            "status": "Failed",
            "source_details": source_details,
            "failure_reason": "One of more data quality checks triggered notebook failure. Please review logging record for details.",
            "data_quality_warnings": json.dumps(dq_warnings),
            "data_quality_failures": json.dumps(dq_force_failures),
            "new_schema": str(new_schema),
            "source_medallion_layer": lineage_info['source_medallion_layer'],
            "source_type": lineage_info['source_type'],
            "target_medallion_layer": lineage_info['target_medallion_layer'],
            "target_type": lineage_info['target_type']
        })

    # Add schema tracking column for Bronze and Silver layers
    if target_datastore_medallion_name in ('bronze','silver'):
        added_schema_tracking_log_info = 'Added schema tracking column, delta__schema_id.'
        log_and_print(added_schema_tracking_log_info)
        
        new_data = new_data.withColumn('delta__schema_id', f.lit(new_data_schema_hash))
    
    return new_data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def _trim_string_data(
    df: DataFrame,
    trim_data_in_string_columns: str
) -> DataFrame:
    """
    Trim leading and trailing whitespace from string columns.
    
    Args:
        df (DataFrame): Input DataFrame
        trim_data_in_string_columns (str): Comma-separated list of columns to trim, or "*" for all string columns
    
    Returns:
        DataFrame: DataFrame with trimmed string values
    """
    if not trim_data_in_string_columns:
        return df
    
    if trim_data_in_string_columns.strip() == "*":
        # Trim all string columns
        trimming_whitespace_log_info = "Trimming leading and trailing whitespace for every value in every string column."
        log_and_print(trimming_whitespace_log_info)

        df = df.select([
                f.trim(f.col(column)).alias(column) if dtype == "string" else f.col(column)
                for column, dtype in df.dtypes
            ])
    else:
        # Trim specific columns
        columns_to_trim = [col.strip().lower() for col in trim_data_in_string_columns.split(",")]
        
        # Only string columns can be trimmed - identify which will actually be processed
        string_cols = [column for column, dtype in df.dtypes if dtype == "string" and column.lower() in columns_to_trim]
        
        # Log which columns are actually being trimmed
        if string_cols:
            trimming_whitespace_log_info = f"Trimming leading and trailing whitespace for string columns: {', '.join(string_cols)}"
            log_and_print(trimming_whitespace_log_info)
        
        # Warn about non-string columns that were requested but skipped
        skipped_cols = [col for col in columns_to_trim if col not in [c.lower() for c in string_cols]]
        if skipped_cols:
            skipped_cols_log_warn = f"Skipping non-string columns for trim (only string columns support this): {', '.join(skipped_cols)}"
            log_and_print(skipped_cols_log_warn, "warn")
        
        df = df.select([
                f.trim(f.col(column)).alias(column) if column.lower() in columns_to_trim and dtype == "string" else f.col(column)
                for column, dtype in df.dtypes
            ])
    
    return df


def _replace_blank_with_null(
    df: DataFrame,
    replace_blank_with_null_in_string_columns: str
) -> DataFrame:
    """
    Replace blank strings with NULL values in specified columns.
    
    Args:
        df (DataFrame): Input DataFrame
        replace_blank_with_null_in_string_columns (str): Comma-separated list of columns, or "*" for all string columns
    
    Returns:
        DataFrame: DataFrame with blank strings replaced by NULL
    """
    if not replace_blank_with_null_in_string_columns:
        return df
    
    if replace_blank_with_null_in_string_columns.strip() == "*":
        # Replace blanks in all string columns
        replacing_blank_strings_log_info = "Replacing blank values in all string columns with null."
        log_and_print(replacing_blank_strings_log_info)
        
        df = df.replace("", None)
    else:
        # Replace blanks in specific columns
        columns_to_replace = [col.strip().lower() for col in replace_blank_with_null_in_string_columns.split(",")]
        
        # Build replacement dict for specific columns - only string columns can have blank values replaced
        string_cols = [column for column, dtype in df.dtypes if dtype == "string" and column.lower() in columns_to_replace]
        
        # Log which columns are actually being processed
        if string_cols:
            replacing_blank_strings_log_info = f"Replacing blank values with null in string columns: {', '.join(string_cols)}"
            log_and_print(replacing_blank_strings_log_info)
        
        # Warn about non-string columns that were requested but skipped
        skipped_cols = [col for col in columns_to_replace if col not in [c.lower() for c in string_cols]]
        if skipped_cols:
            skipped_cols_log_warn = f"Skipping non-string columns for blank replacement (only string columns support this): {', '.join(skipped_cols)}"
            log_and_print(skipped_cols_log_warn, "warn")
        
        for col in string_cols:
            df = df.withColumn(col, f.when(f.col(col) == "", None).otherwise(f.col(col)))
    
    return df


def _convert_timestamp_ntz_to_timestamp(
    df: DataFrame
) -> DataFrame:
    """
    Convert TimestampNTZType fields to TimestampType for Fabric SQL Endpoint compatibility.
    
    Args:
        df (DataFrame): Input DataFrame
    
    Returns:
        DataFrame: DataFrame with TimestampNTZType columns converted to TimestampType
    
    Notes:
        Fabric SQL Endpoint does not support TimestampNTZType as of 6/27/2025.
        See: https://learn.microsoft.com/en-us/fabric/fundamentals/delta-lake-interoperability
    """
    for field in df.schema.fields:
        if isinstance(field.dataType, TimestampNTZType):
            converting_timestamp_type_log_warn = f"Converting {field.name} from TimestampNTZType to TimestampType type. Fabric SQL Endpoint, as of 6/27/2025, does not support TimestampNTZType."
            log_and_print(converting_timestamp_type_log_warn, "warn")

            df = df.withColumn(field.name, f.from_utc_timestamp(df[field.name], "UTC"))
    
    return df


def data_cleansing(
    df: DataFrame, 
    trim_data_in_string_columns: str,
    replace_blank_with_null_in_string_columns: str
) -> DataFrame:
    """
    Apply comprehensive data cleansing operations based on configuration.

    This function orchestrates multiple cleansing strategies to ensure data quality:
    1. String trimming - Removes leading/trailing whitespace from specified columns
    2. Blank to null conversion - Standardizes empty values in specified columns
    3. Convert TimestampNTZType fields to TimestampType

    The cleansing operations are configurable and layer-aware, with different
    defaults for Bronze, Silver, and Gold layers to match typical use cases.

    Args:
        df (DataFrame): Input DataFrame to cleanse
        trim_data_in_string_columns (str): Comma-separated list of columns to trim whitespace, or "*" for all string columns
        replace_blank_with_null_in_string_columns (str): Comma-separated list of columns to replace blanks with null, or "*" for all string columns

    Returns:
        DataFrame: Cleansed DataFrame with applied transformations
    """
    # Step 1: Trim whitespace from specified string columns
    df = _trim_string_data(
        df=df,
        trim_data_in_string_columns=trim_data_in_string_columns
    )

    # Step 2: Replace blank strings with NULL values in specified columns
    df = _replace_blank_with_null(
        df=df,
        replace_blank_with_null_in_string_columns=replace_blank_with_null_in_string_columns
    )

    # Step 3: Convert TimestampNTZType fields to TimestampType
    df = _convert_timestamp_ntz_to_timestamp(df=df)
        
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Transformation Functions
# 
# This section contains functions for applying metadata-driven and custom transformations to your data, including column renaming, derived columns, filtering, and entity resolution. These transformations are orchestrated based on configuration for maximum flexibility.

# CELL ********************

def _validate_columns_exist(
    df: DataFrame, 
    columns_to_check: List[str], 
    operation_name: str
) -> None:
    """Validate that specified columns exist in DataFrame before performing operations.
    
    Args:
        df: DataFrame to check
        columns_to_check: List of column names to validate
        operation_name: Name of the operation for error messaging
        
    Raises:
        Exception: If any columns are missing from the DataFrame
    """
    # spark is not case sensitive so ensure failure doesn't happen on case mismatch
    existing_columns = [col.lower() for col in df.columns]
    missing_columns = [col for col in columns_to_check if col.lower() not in existing_columns]
    
    if missing_columns:
        missing_columns_error = f"Error: The following columns do not exist in DataFrame for {operation_name}: {missing_columns}. Available columns: {existing_columns}"
        log_and_print(missing_columns_error, "error")
        raise Exception(missing_columns_error)

def _rename_columns(
    df: DataFrame, 
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Rename columns for schema alignment or business naming conventions."""
    existing_column_name = data_transformation_step['existing_column_name']
    new_column_name = data_transformation_step['new_column_name']

    existing_columns = [column.strip() for column in existing_column_name.split(',')]
    new_columns = [column.strip() for column in new_column_name.split(',')]
    
    # Validate that all columns to be renamed exist
    _validate_columns_exist(
        df = df, 
        columns_to_check = existing_columns, 
        operation_name = "rename_columns"
    )
    
    column_renames = list(zip(existing_columns, new_columns))
    for column_rename in column_renames:
        existing_column = column_rename[0]
        new_column = column_rename[1]

        renaming_column_log_info = f"Renaming column, {existing_column}, to {new_column}."
        log_and_print(renaming_column_log_info)

        df = df.withColumnRenamed(existing_column, new_column)
    
    return df

def _create_derived_column(
    df: DataFrame, 
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Create new columns using SQL expressions for calculated fields."""
    column_name = data_transformation_step['column_name']
    derived_column_logic = data_transformation_step['expression']

    creating_derived_column_log_info = f"Creating derived column, {column_name}, using logic, {derived_column_logic}."
    log_and_print(creating_derived_column_log_info)
    
    df = df.withColumn(column_name, f.expr(derived_column_logic))
    return df

def _remove_columns(
    df: DataFrame, 
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Remove columns that are not needed in target schema."""
    column_name = data_transformation_step['column_name'] 
    columns = [column.strip() for column in column_name.split(',')]
    
    # Validate that all columns to be removed exist
    _validate_columns_exist(
        df = df, 
        columns_to_check = columns, 
        operation_name = "remove_columns"
    )
    
    for column in columns:
        dropping_column_log_info = f"Dropping column, {column}."
        log_and_print(dropping_column_log_info)

        df = df.drop(column)
    
    return df

def _select_columns(
    df: DataFrame, 
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Select columns that are needed in target schema."""
    column_name = data_transformation_step['column_name'] 
    columns = [column.strip() for column in column_name.split(',')]

    retain_metadata_columns = data_transformation_step.get('retain_metadata_columns', "true").strip().lower() == "true"

    if retain_metadata_columns:
        # Only add metadata columns that aren't already in the user's selection (case-insensitive check)
        existing_columns_lower = [col.lower() for col in columns]
        metadata_columns = [col for col in df.columns if (col.startswith('scd_') or col.startswith('delta__')) and col.lower() not in existing_columns_lower]
        columns = columns + metadata_columns

    # Validate that all columns to be selected exist
    _validate_columns_exist(
        df = df, 
        columns_to_check = columns, 
        operation_name = "select_columns"
    )

    selecting_column_log_info = f"Selecting {columns}."
    log_and_print(selecting_column_log_info)

    df = df.select(*columns)
    
    return df

def _filter_data(
    df: DataFrame, 
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Apply row-level filters to subset data based on business rules."""
    filter_logic = data_transformation_step['filter_logic']

    filtering_data_log_info = f"Filtering data using filter logic, {filter_logic}."
    log_and_print(filtering_data_log_info)

    df = df.filter(filter_logic)
    return df


def _sort_data(
    df: DataFrame,
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Sort DataFrame by one or more columns with configurable sort directions.
    
    This transformation allows ordering data by one or more columns, with 
    independent control over ascending/descending direction for each column.
    Useful for ensuring consistent ordering for downstream processing,
    generating sequence numbers, or preparing data for specific output formats.
    
    Args:
        df (DataFrame): Input DataFrame to sort
        data_transformation_step (dict): Configuration containing:
            - column_name (required): Comma-separated column names to sort by
            - sort_direction (optional): Comma-separated sort directions ('asc'/'desc')
              matching each column. Default is 'asc' for all columns if not specified.
              If fewer directions than columns, remaining columns use 'asc'.
    
    Returns:
        DataFrame: Sorted DataFrame
    
    Attributes:
        - column_name (required): One or more column names to sort by, comma-separated.
          Example: "order_date, customer_id"
        - sort_direction (optional): Sort direction for each column ('asc' or 'desc').
          Comma-separated to match columns. Default: 'asc' for all.
          Example: "desc, asc" (sort order_date descending, customer_id ascending)
    
    Example:
        Configuration:
            column_name: "order_date, customer_id"
            sort_direction: "desc, asc"
        
        Result: Data sorted by order_date descending, then customer_id ascending
    
    Notes:
        - NULL values are sorted according to Spark's default behavior (nulls first for asc)
        - For deterministic results, include enough columns to ensure unique ordering
        - Sort is preserved for subsequent operations in the transformation chain
    """
    # Parse column names
    column_name = data_transformation_step['column_name']
    columns = [col.strip() for col in column_name.split(',')]
    
    # Validate columns exist
    _validate_columns_exist(
        df=df,
        columns_to_check=columns,
        operation_name="sort_data"
    )
    
    # Parse sort directions (default to 'asc' for all columns if not specified)
    sort_direction = data_transformation_step.get('sort_direction', 'asc')
    directions = [d.strip().lower() for d in sort_direction.split(',')]
    
    # Extend directions list if fewer than columns (default remaining to 'asc')
    while len(directions) < len(columns):
        directions.append('asc')
    
    # Validate sort directions
    for direction in directions:
        if direction not in ('asc', 'desc'):
            raise Exception(f"Invalid sort_direction '{direction}'. Must be 'asc' or 'desc'.")
    
    # Build sort specifications
    sort_specs = []
    sort_details = []
    for col, direction in zip(columns, directions):
        if direction == 'desc':
            sort_specs.append(f.desc(col))
        else:
            sort_specs.append(f.asc(col))
        sort_details.append(f"{col} ({direction})")
    
    sort_log_info = f"Sorting data by: {', '.join(sort_details)}."
    log_and_print(sort_log_info)
    
    df = df.orderBy(*sort_specs)
    return df


def _explode_array(
    df: DataFrame,
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Explode array column into multiple rows.
    
    Takes an array column and creates one row for each element in the array,
    duplicating other column values. Useful for normalizing nested data.
    
    Args:
        df (DataFrame): Input DataFrame
        data_transformation_step (dict): Configuration containing:
            - column_name: Name of the array column to explode
            - output_column_name: Optional name for the exploded column (default: same as input)
            - preserve_nulls: If 'true', include rows where array is null/empty (uses explode_outer)
    
    Returns:
        DataFrame: DataFrame with array exploded into rows
    
    Example:
        Input: id=1, tags=['a','b','c']
        Output: id=1, tag='a' | id=1, tag='b' | id=1, tag='c'
    """
    column_name = data_transformation_step['column_name'].strip()
    output_column_name = data_transformation_step.get('output_column_name', column_name).strip()
    preserve_nulls = data_transformation_step.get('preserve_nulls', 'false').strip().lower() == 'true'
    
    _validate_columns_exist(df=df, columns_to_check=[column_name], operation_name="explode_array")
    
    log_and_print(f"Exploding array column '{column_name}' to '{output_column_name}' (preserve_nulls: {preserve_nulls})")
    
    if preserve_nulls:
        df = df.withColumn(output_column_name, f.explode_outer(f.col(column_name)))
    else:
        df = df.withColumn(output_column_name, f.explode(f.col(column_name)))
    
    # Drop original column if output name is different
    if output_column_name != column_name:
        df = df.drop(column_name)
    
    return df


def _flatten_struct(
    df: DataFrame,
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Flatten nested struct columns into individual columns.
    
    Takes a struct column and creates separate columns for each field within
    the struct. Useful for denormalizing nested JSON/struct data.
    
    Args:
        df (DataFrame): Input DataFrame
        data_transformation_step (dict): Configuration containing:
            - column_name: Name of the struct column to flatten
            - prefix: Optional prefix for flattened column names (default: struct column name + '_')
            - fields_to_include: Optional comma-separated list of specific fields to extract (default: all)
            - drop_original: If 'true', drop the original struct column (default: true)
    
    Returns:
        DataFrame: DataFrame with struct fields as individual columns
    
    Example:
        Input: address = {street: '123 Main', city: 'NYC'}
        Output: address_street = '123 Main', address_city = 'NYC'
    """
    column_name = data_transformation_step['column_name'].strip()
    prefix = data_transformation_step.get('prefix', f"{column_name}_").strip()
    fields_to_include = data_transformation_step.get('fields_to_include', '').strip()
    drop_original = data_transformation_step.get('drop_original', 'true').strip().lower() == 'true'
    
    _validate_columns_exist(df=df, columns_to_check=[column_name], operation_name="flatten_struct")
    
    log_and_print(f"Flattening struct column '{column_name}' with prefix '{prefix}'")
    
    # Get struct fields from schema
    struct_field = df.schema[column_name]
    if not hasattr(struct_field.dataType, 'fields'):
        raise ValueError(f"Column '{column_name}' is not a struct type. Use explode_array for array types.")
    
    struct_fields = struct_field.dataType.fields
    
    # Filter fields if specified
    if fields_to_include:
        fields_list = [field.strip() for field in fields_to_include.split(',')]
        struct_fields = [field for field in struct_fields if field.name in fields_list]
    
    # Extract each field
    for field in struct_fields:
        new_col_name = f"{prefix}{field.name}"
        df = df.withColumn(new_col_name, f.col(f"{column_name}.{field.name}"))
        log_and_print(f"  Extracted field '{field.name}' to column '{new_col_name}'")
    
    # Drop original struct column if requested
    if drop_original:
        df = df.drop(column_name)
    
    return df


def _split_column(
    df: DataFrame,
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Split a string column into multiple columns based on a delimiter.
    
    Args:
        df (DataFrame): Input DataFrame
        data_transformation_step (dict): Configuration containing:
            - column_name: Name of the string column to split
            - delimiter: Character(s) to split on (default: ',')
            - output_column_names: Comma-separated names for output columns
            - max_splits: Maximum number of splits to perform (default: -1 for all)
            - drop_original: If 'true', drop the original column (default: false)
    
    Returns:
        DataFrame: DataFrame with split columns added
    
    Example:
        Input: full_name = 'John,Doe,Jr'
        Config: delimiter=',', output_column_names='first_name,last_name,suffix'
        Output: first_name='John', last_name='Doe', suffix='Jr'
    """
    column_name = data_transformation_step['column_name'].strip()
    delimiter = data_transformation_step.get('delimiter', ',')
    output_column_names = data_transformation_step['output_column_names'].strip()
    max_splits = int(data_transformation_step.get('max_splits', -1))
    drop_original = data_transformation_step.get('drop_original', 'false').strip().lower() == 'true'
    
    _validate_columns_exist(df=df, columns_to_check=[column_name], operation_name="split_column")
    
    output_cols = [col.strip() for col in output_column_names.split(',')]
    
    log_and_print(f"Splitting column '{column_name}' by '{delimiter}' into columns: {output_cols}" + 
                  (f" (max_splits: {max_splits})" if max_splits > 0 else ""))
    
    # Split the column into array
    # Spark's split() limit param = max number of resulting strings (not number of splits)
    # So max_splits=2 means we want at most 3 parts, pass limit=max_splits+1
    if max_splits > 0:
        df = df.withColumn("_split_temp", f.split(f.col(column_name), delimiter, max_splits + 1))
    else:
        df = df.withColumn("_split_temp", f.split(f.col(column_name), delimiter))
    
    # Extract each element into named columns
    for idx, col_name in enumerate(output_cols):
        df = df.withColumn(col_name, f.col("_split_temp").getItem(idx))
    
    # Clean up temp column
    df = df.drop("_split_temp")
    
    # Drop original if requested
    if drop_original:
        df = df.drop(column_name)
    
    return df


def _concat_columns(
    df: DataFrame,
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Concatenate multiple columns into a single column.
    
    Args:
        df (DataFrame): Input DataFrame
        data_transformation_step (dict): Configuration containing:
            - column_name: Comma-separated list of columns to concatenate
            - output_column_name: Name for the concatenated output column
            - separator: String to use between values (default: '')
            - null_handling: How to handle nulls - 'skip', 'empty', 'keep' (default: 'skip')
            - drop_original: If 'true', drop the original columns (default: false)
    
    Returns:
        DataFrame: DataFrame with concatenated column added
    
    Example:
        Input: first_name='John', last_name='Doe'
        Config: separator=' ', output_column_name='full_name'
        Output: full_name='John Doe'
    """
    column_names = data_transformation_step['column_name'].strip()
    output_column_name = data_transformation_step['output_column_name'].strip()
    separator = data_transformation_step.get('separator', '')
    null_handling = data_transformation_step.get('null_handling', 'skip').strip().lower()
    drop_original = data_transformation_step.get('drop_original', 'false').strip().lower() == 'true'
    
    columns = [col.strip() for col in column_names.split(',')]
    
    _validate_columns_exist(df=df, columns_to_check=columns, operation_name="concat_columns")
    
    log_and_print(f"Concatenating columns {columns} into '{output_column_name}' with separator '{separator}'")
    
    if null_handling == 'skip':
        # Use concat_ws which skips nulls
        df = df.withColumn(output_column_name, f.concat_ws(separator, *[f.col(c) for c in columns]))
    elif null_handling == 'empty':
        # Replace nulls with empty string then concat
        cols_coalesced = [f.coalesce(f.col(c).cast("string"), f.lit("")) for c in columns]
        if separator:
            df = df.withColumn(output_column_name, f.concat_ws(separator, *cols_coalesced))
        else:
            df = df.withColumn(output_column_name, f.concat(*cols_coalesced))
    else:  # 'keep' - if any null, result is null
        if separator:
            # Build concat with separator manually
            exprs = []
            for i, c in enumerate(columns):
                if i > 0:
                    exprs.append(f.lit(separator))
                exprs.append(f.col(c).cast("string"))
            df = df.withColumn(output_column_name, f.concat(*exprs))
        else:
            df = df.withColumn(output_column_name, f.concat(*[f.col(c).cast("string") for c in columns]))
    
    # Drop original columns if requested
    if drop_original:
        for col in columns:
            df = df.drop(col)
    
    return df


def _conditional_column(
    df: DataFrame,
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Create a column with conditional logic (CASE WHEN equivalent).
    
    Args:
        df (DataFrame): Input DataFrame
        data_transformation_step (dict): Configuration containing:
            - column_name: Name of the output column to create
            - conditions: Comma-separated list of Spark SQL conditions
            - values: Comma-separated list of values (must match conditions count)
            - default_value: Value to use when no conditions match (default: null)
            - values_delimiter: Delimiter for parsing (default: ',')
    
    Returns:
        DataFrame: DataFrame with conditional column added
    
    Example config:
        column_name: 'grade'
        conditions: 'score >= 90, score >= 80, score >= 70'
        values: 'A, B, C'
        values_delimiter: ','  # default
        default_value: 'F'
    """
    column_name = data_transformation_step['column_name'].strip()
    conditions_str = data_transformation_step['conditions']
    values_str = data_transformation_step['values']
    default_value = data_transformation_step.get('default_value')
    values_delimiter = data_transformation_step.get('values_delimiter', ',')
    
    log_and_print(f"Creating conditional column '{column_name}'")
    
    # Parse conditions and values as parallel lists
    conditions_list = [c.strip() for c in conditions_str.split(values_delimiter) if c.strip()]
    values_list = [v.strip() for v in values_str.split(values_delimiter) if v.strip()]
    
    if not conditions_list:
        raise ValueError("conditional_column requires at least one condition")
    
    if len(conditions_list) != len(values_list):
        raise ValueError(f"conditional_column: number of conditions ({len(conditions_list)}) must match number of values ({len(values_list)})")
    
    # Build the CASE WHEN expression
    case_expr = None
    for when_clause, then_value in zip(conditions_list, values_list):
        log_and_print(f"  WHEN {when_clause} THEN {then_value}")
        
        if case_expr is None:
            case_expr = f.when(f.expr(when_clause), f.lit(then_value))
        else:
            case_expr = case_expr.when(f.expr(when_clause), f.lit(then_value))
    
    # Add default value
    if default_value is not None:
        log_and_print(f"  ELSE {default_value}")
        case_expr = case_expr.otherwise(f.lit(default_value))
    else:
        case_expr = case_expr.otherwise(f.lit(None))
    
    df = df.withColumn(column_name, case_expr)
    
    return df


def _string_functions(
    df: DataFrame,
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Apply string transformation functions to columns.
    
    Args:
        df (DataFrame): Input DataFrame
        data_transformation_step (dict): Configuration containing:
            - column_name: Column(s) to transform. Use '*' for all string columns,
              or comma-separated column names for specific columns.
            - function: String function to apply. Supported functions:
                - upper: Convert to uppercase
                - lower: Convert to lowercase
                - trim: Remove leading/trailing whitespace
                - ltrim: Remove leading whitespace
                - rtrim: Remove trailing whitespace
                - reverse: Reverse string
                - length: Get string length
                - initcap: Capitalize first letter of each word
                - soundex: Get soundex code
            - output_column_name: Name for the output column (suffix when using '*')
    
    Returns:
        DataFrame: DataFrame with string transformations applied
    
    Note:
        For advanced string operations (substring, padding, replace, regex),
        use the derived_column transformation with Spark SQL expressions.
    """
    column_names_raw = data_transformation_step['column_name'].strip()
    function_name = data_transformation_step['function'].strip().lower()
    output_column_name = data_transformation_step['output_column_name'].strip()
    
    # Handle * for all string columns
    if column_names_raw == '*':
        columns = [field.name for field in df.schema.fields if field.dataType.simpleString() == 'string']
        if not columns:
            log_and_print("No string columns found for string_functions with '*'", "warn")
            return df
        log_and_print(f"Expanding '*' to string columns: {columns}")
    else:
        columns = [col.strip() for col in column_names_raw.split(',')]
        _validate_columns_exist(df=df, columns_to_check=columns, operation_name="string_functions")
    
    log_and_print(f"Applying string function '{function_name}' to columns: {columns}")
    
    # Supported functions (no extra params needed)
    supported_functions = ['upper', 'lower', 'trim', 'ltrim', 'rtrim', 'reverse', 'length', 'initcap', 'soundex']
    
    if function_name not in supported_functions:
        raise ValueError(f"Unknown string function '{function_name}'. Supported: {', '.join(supported_functions)}. "
                        f"For advanced operations, use derived_column with Spark SQL expressions.")
    
    for col_name in columns:
        # Determine output column name
        if len(columns) == 1:
            out_col = output_column_name
        else:
            out_col = f"{output_column_name}_{col_name}"
        
        # Apply the appropriate string function
        if function_name == 'upper':
            df = df.withColumn(out_col, f.upper(f.col(col_name)))
        elif function_name == 'lower':
            df = df.withColumn(out_col, f.lower(f.col(col_name)))
        elif function_name == 'trim':
            df = df.withColumn(out_col, f.trim(f.col(col_name)))
        elif function_name == 'ltrim':
            df = df.withColumn(out_col, f.ltrim(f.col(col_name)))
        elif function_name == 'rtrim':
            df = df.withColumn(out_col, f.rtrim(f.col(col_name)))
        elif function_name == 'reverse':
            df = df.withColumn(out_col, f.reverse(f.col(col_name)))
        elif function_name == 'length':
            df = df.withColumn(out_col, f.length(f.col(col_name)))
        elif function_name == 'initcap':
            df = df.withColumn(out_col, f.initcap(f.col(col_name)))
        elif function_name == 'soundex':
            df = df.withColumn(out_col, f.soundex(f.col(col_name)))
        
        log_and_print(f"  Applied '{function_name}' to '{col_name}' -> '{out_col}'")
    
    return df


def _normalize_text(
    df: DataFrame,
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Normalize text data for consistency and matching.
    
    Applies multiple text normalization operations in sequence to standardize
    text data for better matching and analysis.
    
    Args:
        df (DataFrame): Input DataFrame
        data_transformation_step (dict): Configuration containing:
            - column_name: Column(s) to normalize. Use '*' for all string columns,
              or comma-separated column names for specific columns.
            - operations: Comma-separated list of operations to apply in order:
                - lowercase: Convert to lowercase
                - uppercase: Convert to uppercase
                - trim: Remove leading/trailing whitespace
                - collapse_whitespace: Replace multiple spaces with single space
                - remove_punctuation: Remove punctuation characters
                - remove_digits: Remove numeric digits
                - remove_special_chars: Remove non-alphanumeric characters (keeps spaces)
                - ascii_only: Remove non-ASCII characters
                - strip_accents: Remove accent marks (é→e, ñ→n)
            - output_column_name: Optional output column name (default: overwrites input)
    
    Returns:
        DataFrame: DataFrame with normalized text
    
    Example:
        Input: '  John   DOE, Jr.  '
        operations: 'trim,lowercase,collapse_whitespace,remove_punctuation'
        Output: 'john doe jr'
    """
    column_names_raw = data_transformation_step['column_name'].strip()
    operations_str = data_transformation_step.get('operations', 'trim,lowercase,collapse_whitespace').strip()
    output_column_name = data_transformation_step.get('output_column_name', '').strip()
    
    # Handle * for all string columns
    if column_names_raw == '*':
        columns = [field.name for field in df.schema.fields if field.dataType.simpleString() == 'string']
        if not columns:
            log_and_print("No string columns found for normalize_text with '*'", "warn")
            return df
        log_and_print(f"Expanding '*' to string columns: {columns}")
    else:
        columns = [col.strip() for col in column_names_raw.split(',')]
        _validate_columns_exist(df=df, columns_to_check=columns, operation_name="normalize_text")
    
    operations = [op.strip().lower() for op in operations_str.split(',')]
    
    log_and_print(f"Normalizing text in columns {columns} with operations: {operations}")
    
    for col_name in columns:
        # Determine output column
        if output_column_name and len(columns) == 1:
            out_col = output_column_name
        else:
            out_col = col_name
        
        # Start with the original column
        result_col = f.col(col_name)
        
        for op in operations:
            if op == 'lowercase':
                result_col = f.lower(result_col)
            elif op == 'uppercase':
                result_col = f.upper(result_col)
            elif op == 'trim':
                result_col = f.trim(result_col)
            elif op == 'collapse_whitespace':
                result_col = f.regexp_replace(result_col, r'\s+', ' ')
            elif op == 'remove_punctuation':
                result_col = f.regexp_replace(result_col, r'[^\w\s]', '')
            elif op == 'remove_digits':
                result_col = f.regexp_replace(result_col, r'\d', '')
            elif op == 'remove_special_chars':
                result_col = f.regexp_replace(result_col, r'[^a-zA-Z0-9\s]', '')
            elif op == 'ascii_only':
                result_col = f.regexp_replace(result_col, r'[^\x00-\x7F]', '')
            elif op == 'strip_accents':
                # Common accent replacements
                result_col = f.regexp_replace(result_col, r'[àáâãäå]', 'a')
                result_col = f.regexp_replace(result_col, r'[èéêë]', 'e')
                result_col = f.regexp_replace(result_col, r'[ìíîï]', 'i')
                result_col = f.regexp_replace(result_col, r'[òóôõö]', 'o')
                result_col = f.regexp_replace(result_col, r'[ùúûü]', 'u')
                result_col = f.regexp_replace(result_col, r'[ýÿ]', 'y')
                result_col = f.regexp_replace(result_col, r'[ñ]', 'n')
                result_col = f.regexp_replace(result_col, r'[ç]', 'c')
                result_col = f.regexp_replace(result_col, r'[ÀÁÂÃÄÅ]', 'A')
                result_col = f.regexp_replace(result_col, r'[ÈÉÊË]', 'E')
                result_col = f.regexp_replace(result_col, r'[ÌÍÎÏ]', 'I')
                result_col = f.regexp_replace(result_col, r'[ÒÓÔÕÖ]', 'O')
                result_col = f.regexp_replace(result_col, r'[ÙÚÛÜ]', 'U')
                result_col = f.regexp_replace(result_col, r'[Ñ]', 'N')
                result_col = f.regexp_replace(result_col, r'[Ç]', 'C')
            else:
                supported_ops = ['lowercase', 'uppercase', 'trim', 'collapse_whitespace', 'remove_punctuation', 
                                'remove_digits', 'remove_special_chars', 'ascii_only', 'strip_accents']
                raise ValueError(f"Unknown normalization operation '{op}'. Supported: {', '.join(supported_ops)}")
        
        df = df.withColumn(out_col, result_col)
        log_and_print(f"  Normalized '{col_name}' -> '{out_col}'")
    
    return df


def _create_surrogate_key(
    df: DataFrame, 
    data_transformation_step: Dict[str, Any],
    target_table_name: str, 
    primary_keys: List[str], 
    first_run: bool, 
    column_to_mark_source_data_deletion: str, 
    merge_type: str
) -> DataFrame:
    """Create surrogate key in target table."""
    creating_surrogate_key_target_log_info = "Creating surrogate key in target table."
    log_and_print(creating_surrogate_key_target_log_info)

    # Extract surrogate key column name from configuration
    dimension_table_key_column_name = data_transformation_step.get('column_name', '').strip()
    if not dimension_table_key_column_name:
        raise Exception("create_surrogate_key requires 'column_name' parameter to specify the surrogate key column name")

    surrogate_key_type = data_transformation_step['type'].strip().lower()
    if surrogate_key_type == "auto_increment":
        df = surr_keys_dim(
            df = df
            , target_table_name = target_table_name
            , primary_keys = primary_keys
            , dimension_table_key_column_name = dimension_table_key_column_name
            , first_run = first_run
        )
    else:
        raise Exception(f"Surrogate key type '{surrogate_key_type}' is not supported. Supported types are: 'auto_increment'")

    # Insert null row with -1 surrogate key for unknown dimension members
    if first_run or merge_type == 'overwrite':
        appending_null_row_log_info = "Appending null row with surrogate key of -1 to dimension table."
        log_and_print(appending_null_row_log_info)

        df = insert_null_row(
            new_data = df,
            column_to_mark_source_data_deletion = column_to_mark_source_data_deletion,
            dimension_table_key_column_name = dimension_table_key_column_name
        ) 
    
    return df

def _execute_custom_transformation_function(
    df: DataFrame, 
    data_transformation_step: Dict[str, Any], 
    all_metadata: Dict[str, Any]
) -> DataFrame:
    """Execute custom transformation functions."""
    # Custom function configurations
    notebooks_to_run = data_transformation_step.get('notebooks_to_run',[])
    if notebooks_to_run:
        notebooks_to_run = notebooks_to_run.split(",")
        notebooks_to_run = [file_name.strip() for file_name in notebooks_to_run]

    custom_transformation_functions = data_transformation_step['functions_to_execute']
    custom_transformation_functions = custom_transformation_functions.split(",")
    custom_transformation_functions = [function_name.strip() for function_name in custom_transformation_functions]

    for notebook in notebooks_to_run:
        instantiate_notebook(notebook_name = notebook)

    # Execute each custom function in order
    for function_name in custom_transformation_functions:
        running_function_log_info = f"Running function: {function_name}"
        log_and_print(running_function_log_info)

        # set advanced config to the key value pairs specific to the transformation
        all_metadata['function_config'] = data_transformation_step
        
        # Prepare standard parameters for custom functions
        function_kwargs = {
            "new_data": df,
            "metadata": all_metadata,
            "spark": spark
        }  

        # Call the function with standard parameters
        df = globals()[function_name](**function_kwargs)
    
    return df

def _validate_transformation_results(
    df: DataFrame, 
    first_run: bool,
    lineage_info: dict
) -> None:
    """Validate that transformations haven't removed all data.
    
    If the DataFrame is empty, this function either raises an exception (on first_run)
    or exits gracefully (subsequent runs).
    
    Args:
        df: DataFrame to validate
        first_run: Whether this is the first run
        lineage_info: Lineage tracking information for logging
    
    Raises:
        Exception: If first_run and no data (configuration error)
        
    Note:
        On subsequent runs with empty data, calls exit_gracefully_no_data() which
        terminates the notebook with a "Processed" status.
    """
    if df.isEmpty():
        exit_gracefully_no_data(
            source_details = "",
            watermark_value = "",
            lineage_info = lineage_info,
            first_run = first_run,
            context_message = "No data returned after applying data transformations."
        )

def _parse_drop_duplicates_columns(
    data_transformation_step: Dict[str, Any]
) -> List[str]:
    """Parse and validate the column_name parameter for drop_duplicates.
    
    Args:
        data_transformation_step: Configuration dictionary containing 'column_name'
        
    Returns:
        List of column names, or None if '*' (all columns)
        
    Raises:
        Exception: If column_name is not provided
    """
    column_name_value = data_transformation_step.get('column_name', '').strip()
    if not column_name_value:
        raise Exception("'column_name' is required for drop_duplicates. Use '*' for all columns or specify column names.")
    
    if column_name_value == '*':
        return None  # None means all columns in dropDuplicates()
    
    return [field.strip() for field in column_name_value.split(",")]


def _parse_order_specifications(
    data_transformation_step: Dict[str, Any]
) -> tuple:
    """Parse and validate order_by and order_direction parameters.
    
    Args:
        data_transformation_step: Configuration dictionary containing 'order_by' and 'order_direction'
        
    Returns:
        Tuple of (order_cols list, order_directions list)
        
    Raises:
        Exception: If order directions count doesn't match order columns count
        Exception: If invalid order direction is provided
    """
    order_by_value = data_transformation_step.get('order_by', '').strip()
    order_direction_value = data_transformation_step.get('order_direction', 'desc').strip().lower()
    
    order_cols = [col.strip() for col in order_by_value.lower().split(",")]
    order_directions = [direction.strip().lower() for direction in order_direction_value.split(',') if direction.strip()]
    
    # If single direction provided, apply to all columns
    if len(order_directions) == 1 and len(order_cols) > 1:
        order_directions = order_directions * len(order_cols)
    elif len(order_directions) != len(order_cols) and len(order_cols) > 0:
        raise Exception(
            f"Number of order directions ({len(order_directions)}) must be 1 or match number of order_by columns ({len(order_cols)})"
        )
    
    # Validate order directions
    for direction in order_directions:
        if direction not in ['asc', 'desc']:
            raise Exception(f"Invalid order direction '{direction}'. Valid options are: 'asc', 'desc'")
    
    return order_cols, order_directions


def _drop_exact_duplicates(
    df: DataFrame,
    subset_fields: List[str]
) -> DataFrame:
    """Drop exact duplicate rows based on specified columns.
    
    Args:
        df: Input DataFrame
        subset_fields: List of columns to check for duplicates, or None for all columns
        
    Returns:
        DataFrame with exact duplicates removed
    """
    if subset_fields:
        log_message = f"Dropping exact duplicates using columns: {subset_fields}. If multiple rows have the exact same values in these columns, only one will be kept."
        log_and_print(log_message)
        return df.dropDuplicates(subset_fields)
    else:
        log_message = "Dropping exact duplicates across all columns. If multiple rows have the exact same values, only one will be kept."
        log_and_print(log_message)
        return df.dropDuplicates()


def _drop_duplicates_with_ordering(
    df: DataFrame,
    subset_fields: List[str],
    order_cols: List[str],
    order_directions: List[str]
) -> DataFrame:
    """Drop duplicates keeping one row per partition based on ordering.
    
    Uses window function with row_number() to keep exactly one row per
    partition (defined by subset_fields) based on the specified ordering.
    
    Args:
        df: Input DataFrame
        subset_fields: List of columns to partition by, or None for all columns
        order_cols: List of columns to order by
        order_directions: List of sort directions ('asc' or 'desc') matching order_cols
        
    Returns:
        DataFrame with duplicates removed based on ordering
    """
    # Determine partition columns
    if subset_fields:
        partition_cols = subset_fields
        partition_log = f"columns: {subset_fields}"
    else:
        partition_cols = df.columns
        partition_log = "all columns"
    
    order_details = list(zip(order_cols, order_directions))
    log_message = f"Dropping duplicates on {partition_log}. Keeping row based on ordering: {order_details}."
    log_and_print(log_message)
    
    # Build order specifications
    order_specs = []
    for col_name, direction in zip(order_cols, order_directions):
        order_specs.append(f.desc(col_name) if direction == 'desc' else f.col(col_name))

    # Use window function to number rows and keep only the first
    window = Window.partitionBy(partition_cols).orderBy(*order_specs)
    return (df.withColumn('row', f.row_number().over(window))
              .filter(f.col('row') == 1)
              .drop('row'))


def _drop_duplicates(
    df: DataFrame, 
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Drop duplicate rows using exact match or ordered deduplication.
    
    Orchestrates the duplicate removal process by delegating to specialized
    helper functions based on the configuration.
    
    Attributes:
    - column_name (required): Columns to deduplicate on. Use '*' for all columns, or comma-separated column names.
    - order_by (optional): If specified, keeps one row per column_name combination based on ordering.
    - order_direction (optional): 'asc' or 'desc' (default: 'desc'). Can be comma-separated to match order_by columns.
    
    Mode is determined by presence of 'order_by':
    - If 'order_by' is NOT specified: Exact duplicate removal on column_name columns
    - If 'order_by' IS specified: Ordered deduplication - keeps one row per column_name combination based on order_by
    """
    # Parse column_name (required)
    subset_fields = _parse_drop_duplicates_columns(data_transformation_step)
    
    # Check if order_by is present to determine mode
    order_by_value = data_transformation_step.get('order_by', '').strip()
    
    if not order_by_value:
        # Exact duplicate removal mode
        return _drop_exact_duplicates(df, subset_fields)
    else:
        # Ordered deduplication mode
        order_cols, order_directions = _parse_order_specifications(data_transformation_step)
        return _drop_duplicates_with_ordering(df, subset_fields, order_cols, order_directions)

def _change_data_types(
    df: DataFrame, 
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Change data types for one or more columns to a single specified PySpark type."""
    
    # Extract configuration parameters
    columns = data_transformation_step['column_name']
    new_type = data_transformation_step['new_type'].strip().lower()
    
    # Parse column names (handle comma-separated)
    column_list = [col.strip() for col in columns.split(',')]
    
    # Validate columns exist
    _validate_columns_exist(
        df = df, 
        columns_to_check = column_list, 
        operation_name = "change_data_types"
    )
    
    # Apply data type changes to all specified columns
    for column_name in column_list:
        changing_data_type_log_info = f"Changing data type of column '{column_name}' to '{new_type}'."
        log_and_print(changing_data_type_log_info)
        
        if new_type in ['string', 'int', 'long', 'float', 'double', 'boolean', 'binary'] or 'decimal' in new_type:
            df = df.withColumn(column_name, f.col(column_name).cast(new_type))
        elif new_type == 'date':
            df = df.withColumn(column_name, f.to_date(f.col(column_name)))
        elif new_type == 'timestamp':
            df = df.withColumn(column_name, f.to_timestamp(f.col(column_name)))
        else:
            raise Exception(f"Invalid data type '{new_type}'. Valid types are: string, int, long, float, double, decimal, boolean, date, timestamp, binary")
    
    return df

def _replace_values(
    df: DataFrame, 
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Replace multiple different values across one or more columns with a specific value."""
    
    # Extract configuration parameters
    columns = data_transformation_step['column_name']
    values_to_replace = data_transformation_step['values_to_replace']
    replacement_value = data_transformation_step['replacement_value']
    # Optional delimiter parameter for parsing values (defaults to comma for backward compatibility)
    values_delimiter = data_transformation_step.get('values_delimiter', ',')
    
    # Parse column names (handle comma-separated)
    column_list = [col.strip() for col in columns.split(',')]
    
    # Parse values to replace (handle comma-separated)
    values_list = [value.strip() for value in values_to_replace.split(values_delimiter) if value.strip()]
    
    # Validate columns exist
    _validate_columns_exist(
        df = df, 
        columns_to_check = column_list, 
        operation_name = "replace_values"
    )
    
    # Validate that values to replace are provided
    if not values_list:
        raise Exception("'values_to_replace' must be provided and cannot be empty")
    
    # Get dtypes once for all columns
    dtypes_dict = dict(df.dtypes)
    
    # Apply value replacement to all specified columns
    for column_name in column_list:
        col_type = dtypes_dict[column_name]
        is_string_column = col_type in ['string']
        
        replacing_values_log_info = f"Replacing values {values_list} with '{replacement_value}' in column '{column_name}'."
        log_and_print(replacing_values_log_info)
        
        if is_string_column:
            # For string columns: use PySpark isin() function for safe comparison
            df = df.withColumn(
                column_name, 
                f.when(f.col(column_name).isin(values_list), f.lit(replacement_value))
                 .otherwise(f.col(column_name))
            )
        else:
            # For numeric columns: convert values and use PySpark isin() function
            converted_values = []
            for value in values_list:
                try:
                    if col_type in ['int', 'long']:
                        converted_values.append(int(value))
                    elif col_type in ['double', 'float']:
                        converted_values.append(float(value))
                    elif col_type.startswith('decimal'):
                        converted_values.append(Decimal(value))
                    else:
                        raise Exception(f"Data type '{col_type}' is not supported for replace_values action.")
                except ValueError:
                    raise Exception(f"Cannot convert value '{value}' to {col_type} type for column '{column_name}'.")
            
            if converted_values:
                # Convert replacement value to appropriate type for numeric columns
                try:
                    if col_type in ['int', 'long']:
                        converted_replacement = int(replacement_value)
                    elif col_type in ['double', 'float']:
                        converted_replacement = float(replacement_value)
                    elif col_type.startswith('decimal'):
                        converted_replacement = Decimal(replacement_value)
                    else:
                        converted_replacement = replacement_value
                    
                    # Use PySpark isin() function for safe numeric comparison
                    df = df.withColumn(
                        column_name, 
                        f.when(f.col(column_name).isin(converted_values), f.lit(converted_replacement))
                         .otherwise(f.col(column_name))
                    )
                except ValueError:
                    raise Exception(f"Cannot convert replacement value '{replacement_value}' to {col_type} type for column '{column_name}'.")
    
    return df

def _mask_sensitive_data(
    df: DataFrame, 
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Mask sensitive data using PySpark mask function."""
    
    # Extract configuration parameters
    column_name = data_transformation_step['column_name']
    upper_char = data_transformation_step.get('upper_char', 'X')
    lower_char = data_transformation_step.get('lower_char', 'X')
    digit_char = data_transformation_step.get('digit_char', 'X')
    other_char = data_transformation_step.get('other_char', None)
    
    # Parse column names (handle comma-separated)
    columns = [col.strip() for col in column_name.split(',')]
    
    # Validate columns exist
    _validate_columns_exist(
        df = df, 
        columns_to_check = columns, 
        operation_name = "mask_sensitive_data"
    )
    
    # Apply masking to all specified columns
    for column_name in columns:
        masking_log_info = f"Masking sensitive data in column '{column_name}' using mask function."
        log_and_print(masking_log_info)
        
        # Build mask function call with parameters
        if other_char is not None:
            masked_column = f.mask(
                f.col(column_name),
                f.lit(upper_char),
                f.lit(lower_char),
                f.lit(digit_char),
                f.lit(other_char)
            )
        else:
            masked_column = f.mask(
                f.col(column_name),
                f.lit(upper_char),
                f.lit(lower_char),
                f.lit(digit_char)
            )

        df = df.withColumn(column_name, masked_column)
    
    return df

def _add_row_hash(
    df: DataFrame, 
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Add hash column for change detection and data lineage with configurable algorithm.
    
    Supports two hashing algorithms via 'hash_algorithm' parameter:
    
    1. xxhash64 (default) - Fast non-cryptographic hash
       - Performance: Excellent (fastest option)
       - Collision risk: Low for typical datasets, increases with scale
       - Birthday paradox: ~50% collision chance at ~4.3 billion records (2^32)
       - At 1 billion records: ~11% collision probability
       - At 100 million records: ~0.11% collision probability
       - Best for: Standard data engineering, change detection, most use cases
    
    2. sha256 - Cryptographic hash (collision-free)
       - Performance: Good (slower than xxhash64 but still efficient)
       - Collision risk: Virtually zero (2^256 possible outputs)
       - Cryptographically secure with no known collision attacks
       - Best for: Large datasets (>100M records), compliance requirements, 
                   critical applications where collisions are unacceptable
    
    Configuration Parameters:
        - column_name: Columns to hash (comma-separated or '*' for all)
        - output_column_name: Name of the hash column (default: 'row_hash')
        - hash_algorithm: 'xxhash64' (default) or 'sha256'
    
    Examples:
        Fast hashing (default):
        {'column_name': 'id,name,date', 'output_column_name': 'row_hash'}
        
        Collision-free hashing:
        {'column_name': '*', 'output_column_name': 'row_hash', 'hash_algorithm': 'sha256'}
    """
    
    # Extract configuration parameters
    column_name = data_transformation_step.get('column_name', '*')
    output_column_name = data_transformation_step.get('output_column_name', 'row_hash')
    hash_algorithm = data_transformation_step.get('hash_algorithm', 'xxhash64').strip().lower()
    
    # Validate hash_algorithm parameter
    valid_algorithms = ['xxhash64', 'sha256']
    if hash_algorithm not in valid_algorithms:
        raise Exception(f"Invalid hash_algorithm '{hash_algorithm}'. Valid options are: {', '.join(valid_algorithms)}")
    
    # Parse column names for hashing
    if column_name == '*':
        columns_to_hash = df.columns
    else:
        columns_to_hash = [col.strip() for col in column_name.split(',')]
        # Validate columns exist
        _validate_columns_exist(
            df = df, 
            columns_to_check = columns_to_hash, 
            operation_name = "add_row_hash"
        )
    
    adding_hash_log_info = f"Adding {hash_algorithm} hash column '{output_column_name}' based on columns: {columns_to_hash}."
    log_and_print(adding_hash_log_info)
    
    # Apply hashing based on selected algorithm
    if hash_algorithm == 'xxhash64':
        # Fast non-cryptographic hash (default)
        column_refs = [f.col(col_name) for col_name in columns_to_hash]
        df = df.withColumn(output_column_name, f.xxhash64(*column_refs))
    
    elif hash_algorithm == 'sha256':
        # Collision-free cryptographic hash
        # SHA-256 requires concatenating columns into a single string first
        # Convert all columns to string and concatenate with separator
        concat_expr = f.concat_ws('|', *[f.coalesce(f.col(col_name).cast('string'), f.lit('NULL')) for col_name in columns_to_hash])
        df = df.withColumn(output_column_name, f.sha2(concat_expr, 256))
    
    return df

def _pivot_data(
    df: DataFrame, 
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Pivot data to convert rows to columns with configurable aggregation.

    Supports multiple value columns with corresponding aggregations.
    Value columns and aggregations must be comma-separated and match in count.
    Output column names follow the default Spark pivot naming convention
    (e.g., `Jan_sum(sales)`), which downstream metadata consumers should
    account for if they need deterministic aliases.
    """

    pivot_config = _prepare_pivot_configuration(
        df=df,
        data_transformation_step=data_transformation_step
    )

    agg_exprs = _build_pivot_aggregation_expressions(
        value_columns=pivot_config['value_columns'],
        aggregations=pivot_config['aggregations']
    )

    pivoting_log_info = (
        f"Pivoting data: grouping by {pivot_config['group_columns']}, pivot column '{pivot_config['pivot_column']}', "
        f"value columns {pivot_config['value_columns']}, aggregations {pivot_config['aggregations']}."
    )
    log_and_print(pivoting_log_info)

    result_df = df.groupBy(*pivot_config['group_columns']).pivot(pivot_config['pivot_column']).agg(*agg_exprs)

    return result_df


def _prepare_pivot_configuration(
    df: DataFrame,
    data_transformation_step: Dict[str, Any]
) -> Dict[str, Any]:
    """Validate and normalize pivot configuration inputs."""

    group_columns = data_transformation_step['group_columns']
    pivot_column = data_transformation_step['pivot_column']
    value_column = data_transformation_step['value_column']
    aggregation = data_transformation_step['aggregation']
    group_col_list = [col.strip() for col in group_columns.split(',')]
    value_col_list = [col.strip() for col in value_column.split(',')]
    agg_list = [agg.strip().lower() for agg in aggregation.split(',')]

    if len(value_col_list) != len(agg_list):
        raise Exception(
            f"Pivot configuration error: Number of value columns ({len(value_col_list)}) must match number of aggregations ({len(agg_list)}). "
            f"You provided value_column='{value_column}' with {len(value_col_list)} column(s) and aggregation='{aggregation}' with {len(agg_list)} aggregation(s). "
            f"Each value column needs a corresponding aggregation function. "
            f"Example: value_column='sales,quantity', aggregation='sum,avg' (2 columns, 2 aggregations)."
        )

    # Validate columns exist before attempting to pivot
    _validate_columns_exist(
        df=df,
        columns_to_check=group_col_list + [pivot_column] + value_col_list,
        operation_name="pivot_data"
    )

    return {
        'group_columns': group_col_list,
        'pivot_column': pivot_column,
        'value_columns': value_col_list,
        'aggregations': agg_list
    }


def _build_pivot_aggregation_expressions(
    value_columns: List[str],
    aggregations: List[str]
) -> List[Any]:
    """Build aggregation expressions for pivot logic."""

    agg_functions = {
        'sum': f.sum,
        'avg': f.avg,
        'min': f.min,
        'max': f.max,
        'count': f.count,
        'first': f.first,
        'last': f.last
    }

    invalid_aggs = [agg for agg in aggregations if agg not in agg_functions]
    if invalid_aggs:
        raise Exception(f"Invalid aggregation(s) '{', '.join(invalid_aggs)}' for pivot_data. Valid options are: {', '.join(agg_functions.keys())}")

    agg_exprs: List[Any] = []

    for value_col, agg in zip(value_columns, aggregations):
        agg_func = agg_functions[agg]
        agg_exprs.append(agg_func(value_col))

    return agg_exprs

def _unpivot_data(
    df: DataFrame, 
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Unpivot data to convert columns to rows (melt operation)."""
    
    # Extract configuration parameters
    id_columns = data_transformation_step['id_columns']
    value_columns = data_transformation_step['value_columns'] 
    variable_name = data_transformation_step.get('variable_name', 'variable')
    value_name = data_transformation_step.get('value_name', 'value')
    
    # Parse column lists
    id_col_list = [col.strip() for col in id_columns.split(',')]
    value_col_list = [col.strip() for col in value_columns.split(',')]
    
    # Validate columns exist
    all_columns_to_check = id_col_list + value_col_list
    _validate_columns_exist(
        df = df, 
        columns_to_check = all_columns_to_check, 
        operation_name = "unpivot_data"
    )
    
    unpivoting_log_info = f"Unpivoting data: ID columns {id_col_list}, value columns {value_col_list} -> '{variable_name}', '{value_name}'."
    log_and_print(unpivoting_log_info)
    
    # Create unpivot using stack function
    stack_expr = f"stack({len(value_col_list)}, " + ", ".join([f"'{col}', {col}" for col in value_col_list]) + f") as ({variable_name}, {value_name})"
    
    # Select ID columns and apply stack
    select_cols = id_col_list + [f.expr(stack_expr)]
    result_df = df.select(*select_cols)
    
    return result_df

def _drop_null_rows(
    df: DataFrame, 
    columns: List[str], 
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Drop rows containing null values."""
    # Get logic parameter (default to 'any' for backward compatibility)
    logic = data_transformation_step.get('logic', 'any').strip().lower()
    
    if logic not in ['any', 'all']:
        raise Exception(f"Invalid logic '{logic}' for drop_rows action. Valid options are: 'any', 'all'")
    
    dropping_null_rows_log_info = f"Dropping rows where {logic} of columns {columns} contain null values."
    log_and_print(dropping_null_rows_log_info)
    
    if logic == 'any':
        # Drop rows where ANY of the specified columns are null (OR logic)
        null_conditions = [f"{column} IS NOT NULL" for column in columns]
        filter_condition = " AND ".join(null_conditions)
        df = df.filter(filter_condition)
    else:  # logic == 'all'
        # Drop rows where ALL of the specified columns are null (AND logic)
        not_null_conditions = [f"{column} IS NOT NULL" for column in columns]
        any_not_null_condition = " OR ".join(not_null_conditions)
        df = df.filter(any_not_null_condition)
    
    return df

def _replace_nulls_with_default(
    df: DataFrame, 
    columns: List[str], 
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Replace null values with type-appropriate default values."""
    default_value = data_transformation_step['default_value'].strip()
    fill_values = {}
    
    # Get dtypes once for all columns with case insensitive mapping
    dtypes_dict = {col.lower(): dtype for col, dtype in df.dtypes}
    
    for column in columns:
        replacing_nulls_log_info = f"Replacing nulls in column '{column}' with default value '{default_value}' converted to appropriate data type."
        log_and_print(replacing_nulls_log_info)
        
        # Get column data type and convert default_value accordingly
        col_type = dtypes_dict[column.lower()]
        try:
            if col_type in ['int', 'long']:
                converted_value = int(default_value)
            elif col_type in ['double', 'float']:
                converted_value = float(default_value)
            elif col_type.startswith('decimal'):
                converted_value = Decimal(str(default_value))
            elif col_type in ['string']:
                converted_value = str(default_value)
            elif col_type in ['boolean']:
                converted_value = str(default_value).lower() == 'true'
            elif col_type in ['date']:
                converted_value = str(default_value)  # Keep as string for date casting
            elif col_type in ['timestamp']:
                converted_value = str(default_value)  # Keep as string for timestamp casting
            else:
                raise Exception(f"Data type '{col_type}' is not supported for replace_with_default action")
        except ValueError as e:
            raise Exception(f"Cannot convert default value '{default_value}' to {col_type} type for column '{column}': {str(e)}")
        except Exception as e:
            # Re-raise any other exceptions (including the custom ones from unsupported data types)
            raise

        fill_values[column] = converted_value
    
    # Single fillna operation for better performance
    df = df.fillna(fill_values)
    return df

def _convert_values_to_null(
    df: DataFrame, 
    columns: List[str], 
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Convert specific values to null."""
    values_to_convert = data_transformation_step.get('values_to_convert', '')
    convert_empty_space = data_transformation_step.get('convert_empty_space', "false").strip().lower() == "true"
    # Optional delimiter parameter for parsing values (defaults to comma for backward compatibility)
    values_delimiter = data_transformation_step.get('values_delimiter', ',')
    
    # Parse comma-separated values to convert (if provided)
    values_list = [value.strip() for value in values_to_convert.split(values_delimiter) if value.strip()]
    
    converting_values_log_info = f"Converting values {values_list} to null in columns {columns}. Convert empty space: {convert_empty_space}. Note: String columns will be trimmed before comparison."
    log_and_print(converting_values_log_info)
    
    # Validate that at least one conversion option is specified
    if not values_list and not convert_empty_space:
        raise Exception("Either 'values_to_convert' must be provided or 'convert_empty_space' must be true")
    
    # Get dtypes once for all columns
    dtypes_dict = dict(df.dtypes)
    
    for column in columns:
        col_type = dtypes_dict[column]
        is_string_column = col_type in ['string']
            
        # Build conditions for the column type
        if is_string_column:
            # Determine values to check based on convert_empty_space setting
            check_values = values_list + [""] if convert_empty_space else values_list
            
            # For string columns: use PySpark trim and isin for safe comparison
            if check_values:
                df = df.withColumn(
                    column, 
                    f.when(f.trim(f.col(column)).isin(check_values), f.lit(None))
                     .otherwise(f.col(column))
                )
        else:
            # For numeric columns: convert values and use PySpark isin function
            converted_values = []
            for value in values_list:
                try:
                    if col_type in ['int', 'long']:
                        converted_values.append(int(value))
                    elif col_type in ['double', 'float']:
                        converted_values.append(float(value))
                    elif col_type.startswith('decimal'):
                        converted_values.append(Decimal(value))
                    else:
                        raise Exception(f"Data type '{col_type}' is not supported for convert_values_to_null action.")
                except ValueError as e:
                    raise Exception(f"Cannot convert default value '{value}' to {col_type} type for column '{column}': {str(e)}")
                except Exception as e:
                    # Re-raise any other exceptions (including the custom ones from unsupported data types)
                    raise
            
            # Use PySpark isin() function for safe numeric comparison
            if converted_values:
                df = df.withColumn(
                    column, 
                    f.when(f.col(column).isin(converted_values), f.lit(None))
                     .otherwise(f.col(column))
                )
    
    return df

def _join_data(
    df: DataFrame, 
    data_transformation_step: Dict[str, Any],
    all_metadata: Dict[str, Any]
) -> DataFrame:
    """Join with another table or dataset using SQL-like join conditions.
    
    Join conditions should reference tables using 'a' for left table and 'b' for right table.
    Examples:
    - Simple equality: "a.customer_id = b.customer_id"
    - Multiple conditions: "a.customer_id = b.customer_id AND a.region = b.region"
    - Range joins: "a.order_date >= b.start_date AND a.order_date <= b.end_date"
    - Complex logic: "a.customer_id = b.customer_id AND (a.amount > b.threshold OR a.tier = 'Premium')"
    
    Column Selection:
    - left_columns: Comma-separated list of columns to select from left table (default: all columns with 'a.' prefix)
    - right_columns: Comma-separated list of columns to select from right table (default: all columns with 'b.' prefix)
    - Use '*' to select all columns from a table
    - Columns are automatically prefixed with table alias (a. or b.) in the result
    
    Note: For semi and anti joins, only left table columns are returned (right table is used only for filtering).
          The right_columns parameter is ignored for these join types.
    """
    
    # Extract configuration parameters
    join_type = data_transformation_step.get('join_type', 'inner').strip().lower()
    right_table_name = data_transformation_step['right_table_name']
    broadcast_hint = data_transformation_step.get('broadcast_hint', 'false').strip().lower() == 'true'
    join_condition = data_transformation_step['join_condition']  # Required parameter
    left_columns = data_transformation_step.get('left_columns', '*').strip()
    right_columns = data_transformation_step.get('right_columns', '*').strip()
    
    # Validate join condition is provided
    if not join_condition or join_condition.strip() == '':
        raise Exception("'join_condition' must be provided and cannot be empty. Use 'a' for left table and 'b' for right table in your condition.")
    
    # Validate join type
    valid_join_types = ['inner', 'left', 'outer', 'left_outer', 'full_outer', 'semi', 'anti']
    if join_type not in valid_join_types:
        raise Exception(f"Invalid join type '{join_type}'. Valid options are: {valid_join_types}")
    
    # Determine workspace name from metadata
    right_table_datastore_name = right_table_name.split('.')[0].strip()
    workspace_name = _get_datastore_config(all_metadata['datastore_config'], right_table_datastore_name, 'Workspace_Name')
    right_full_table_name = f"`{workspace_name}`.{right_table_name}"
    
    joining_log_info = f"Joining with table {right_full_table_name} using {join_type} join with condition: {join_condition}. Broadcast hint: {broadcast_hint}. Left columns: {left_columns}, Right columns: {right_columns}."
    log_and_print(joining_log_info)
    
    # Read the right table
    right_df = spark.read.table(right_full_table_name)
    
    # Apply broadcast hint if specified
    if broadcast_hint:
        right_df = f.broadcast(right_df)
        join_log_broadcast = "Applied broadcast hint to right table for performance optimization."
        log_and_print(join_log_broadcast)
    
    # Alias DataFrames for join condition referencing
    # User writes conditions like: "a.customer_id = b.customer_id AND a.date >= b.start_date"
    left_aliased = df.alias("a")
    right_aliased = right_df.alias("b")
    
    # Perform the join with aliased DataFrames
    joined_df = left_aliased.join(right_aliased, f.expr(join_condition), join_type)
    
    # For semi and anti joins, right table columns are not available in the result
    # These join types only use the right table for filtering
    is_semi_or_anti_join = join_type in ['semi', 'anti']
    
    # Build column selection list
    select_columns = []
    
    # Process left table columns
    if left_columns == '*':
        # Select all columns from left table with 'a.' prefix
        select_columns.extend([f.col(f"a.{col}") for col in df.columns])
    else:
        # Parse comma-separated list and validate
        left_col_list = [col.strip() for col in left_columns.split(',') if col.strip()]
        _validate_columns_exist(df, left_col_list, "join_data (left_columns)")
        select_columns.extend([f.col(f"a.{col}") for col in left_col_list])
    
    # Process right table columns (only for non-semi/anti joins)
    if not is_semi_or_anti_join:
        if right_columns == '*':
            # Select all columns from right table with 'b.' prefix
            select_columns.extend([f.col(f"b.{col}") for col in right_df.columns])
        else:
            # Parse comma-separated list and validate
            right_col_list = [col.strip() for col in right_columns.split(',') if col.strip()]
            _validate_columns_exist(right_df, right_col_list, "join_data (right_columns)")
            select_columns.extend([f.col(f"b.{col}") for col in right_col_list])
    elif right_columns != '*':
        # Warn if user specified right columns for semi/anti join
        warning_msg = f"Note: right_columns parameter ignored for {join_type} join. Semi and anti joins only return left table columns."
        log_and_print(warning_msg)
    
    # Apply column selection to joined DataFrame
    joined_df = joined_df.select(*select_columns)
    
    return joined_df

_WINDOW_FUNCTIONS = ['row_number', 'rank', 'dense_rank', 'first_value', 'last_value', 'sum', 'avg', 'count', 'min', 'max']
_WINDOW_AGGREGATION_FUNCTIONS = ['sum', 'avg', 'count', 'min', 'max']
_WINDOW_VALUE_FUNCTIONS = ['first_value', 'last_value']
_VALID_AGGREGATION_FUNCTIONS = ['sum', 'avg', 'count', 'min', 'max', 'first', 'last', 'count_distinct']

_DATETIME_OPERATIONS_REQUIRING_COLUMN = {
    'year', 'month', 'day', 'dayofmonth', 'dayofweek', 'dayofyear',
    'hour', 'minute', 'second', 'quarter', 'weekofyear',
    'add_days', 'subtract_days', 'add_months', 'date_diff',
    'format_date', 'to_timestamp'
}

_VALID_DATETIME_OPERATIONS = [
    'year', 'month', 'day', 'dayofmonth', 'dayofweek', 'dayofyear',
    'hour', 'minute', 'second', 'quarter', 'weekofyear', 'add_days',
    'subtract_days', 'add_months', 'date_diff', 'format_date',
    'to_timestamp', 'current_date', 'current_timestamp'
]


def _parse_window_function_config(
    data_transformation_step: Dict[str, Any]
) -> Dict[str, Any]:
    """Parse and normalize configuration for window functions."""

    column_name = data_transformation_step.get('column_name', '').strip()
    output_column_name = data_transformation_step.get('output_column_name', '').strip()
    if not output_column_name:
        raise Exception("'output_column_name' must be provided for add_window_function transformations")

    window_function = data_transformation_step['window_function'].strip().lower()
    partition_by = data_transformation_step.get('partition_by', '')
    order_by = data_transformation_step.get('order_by', '')
    order_direction_value = data_transformation_step.get('order_direction', 'asc').strip().lower()

    partition_cols = [col.strip() for col in partition_by.split(',') if col.strip()] if partition_by else []
    order_cols = [col.strip() for col in order_by.split(',') if col.strip()] if order_by else []
    order_directions = [direction.strip().lower() for direction in order_direction_value.split(',') if direction.strip()]

    if len(order_directions) == 1 and len(order_cols) > 1:
        order_directions = order_directions * len(order_cols)

    return {
        'column_name': column_name,
        'output_column_name': output_column_name,
        'window_function': window_function,
        'partition_cols': partition_cols,
        'order_cols': order_cols,
        'order_directions': order_directions
    }


def _validate_window_function_config(
    df: DataFrame,
    config: Dict[str, Any]
) -> None:
    """Validate window function configuration and ensure required columns exist."""

    window_function = config['window_function']
    if window_function not in _WINDOW_FUNCTIONS:
        raise Exception(f"Invalid window function '{window_function}'. Valid options are: {_WINDOW_FUNCTIONS}")

    if window_function in _WINDOW_AGGREGATION_FUNCTIONS + _WINDOW_VALUE_FUNCTIONS:
        if not config['column_name']:
            raise Exception(f"'column_name' must be provided for window function '{window_function}'")

    order_cols = config['order_cols']
    order_directions = config['order_directions']
    if len(order_directions) == 1 and len(order_cols) > 1:
        config['order_directions'] = order_directions * len(order_cols)
        order_directions = config['order_directions']
    elif len(order_directions) != len(order_cols) and len(order_cols) > 0:
        raise Exception(
            f"Number of order directions ({len(order_directions)}) must be 1 or match number of order_by columns ({len(order_cols)})"
        )

    for direction in order_directions:
        if direction not in ['asc', 'desc']:
            raise Exception(f"Invalid order direction '{direction}'. Valid options are: 'asc', 'desc'")

    columns_to_check = config['partition_cols'] + config['order_cols']
    if config['column_name']:
        columns_to_check.append(config['column_name'])
    if columns_to_check:
        _validate_columns_exist(
            df=df,
            columns_to_check=columns_to_check,
            operation_name="add_window_function"
        )


def _log_window_function_configuration(config: Dict[str, Any]) -> None:
    """Generate consistent logging for window function transformations."""

    source_col_info = f"source column: '{config['column_name']}'" if config['column_name'] else "no source column (ranking function)"
    order_details = list(zip(config['order_cols'], config['order_directions'])) if config['order_cols'] else []
    window_log_info = (
        f"Adding window function '{config['window_function']}' as column '{config['output_column_name']}' "
        f"({source_col_info}). Partition by: {config['partition_cols']}, Order by: {order_details}."
    )
    log_and_print(window_log_info)


def _build_window_spec(
    partition_cols: List[str],
    order_cols: List[str],
    order_directions: List[str]
) -> WindowSpec:
    """Create a WindowSpec based on partition and order configuration."""

    window_spec = Window.partitionBy(*partition_cols) if partition_cols else Window.partitionBy()

    if order_cols:
        order_specs = []
        for col_name, direction in zip(order_cols, order_directions):
            order_specs.append(f.desc(col_name) if direction == 'desc' else f.col(col_name))
        window_spec = window_spec.orderBy(*order_specs)

    return window_spec


def _apply_window_function(
    df: DataFrame,
    config: Dict[str, Any],
    window_spec: WindowSpec
) -> DataFrame:
    """Apply the requested window function to the DataFrame."""

    output_column_name = config['output_column_name']
    column_name = config['column_name']
    window_function = config['window_function']
    order_cols = config['order_cols']

    if window_function == 'row_number':
        return df.withColumn(output_column_name, f.row_number().over(window_spec))
    if window_function == 'rank':
        return df.withColumn(output_column_name, f.rank().over(window_spec))
    if window_function == 'dense_rank':
        return df.withColumn(output_column_name, f.dense_rank().over(window_spec))
    if window_function == 'first_value':
        return df.withColumn(output_column_name, f.first(column_name).over(window_spec))
    if window_function == 'last_value':
        return df.withColumn(output_column_name, f.last(column_name).over(window_spec))
    if window_function in _WINDOW_AGGREGATION_FUNCTIONS:
        agg_col = column_name if column_name else (order_cols[0] if order_cols else '*')
        if window_function == 'sum':
            return df.withColumn(output_column_name, f.sum(agg_col).over(window_spec))
        if window_function == 'avg':
            return df.withColumn(output_column_name, f.avg(agg_col).over(window_spec))
        if window_function == 'count':
            return df.withColumn(output_column_name, f.count(agg_col).over(window_spec))
        if window_function == 'min':
            return df.withColumn(output_column_name, f.min(agg_col).over(window_spec))
        if window_function == 'max':
            return df.withColumn(output_column_name, f.max(agg_col).over(window_spec))

    raise Exception(f"Invalid window function '{window_function}'. Valid options are: {_WINDOW_FUNCTIONS}")


def _add_window_function(
    df: DataFrame,
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Orchestrate window function calculations while preserving public API."""

    config = _parse_window_function_config(data_transformation_step=data_transformation_step)
    _validate_window_function_config(df=df, config=config)
    _log_window_function_configuration(config=config)
    window_spec = _build_window_spec(
        partition_cols=config['partition_cols'],
        order_cols=config['order_cols'],
        order_directions=config['order_directions']
    )
    return _apply_window_function(df=df, config=config, window_spec=window_spec)


def _parse_aggregate_config(
    data_transformation_step: Dict[str, Any]
) -> Dict[str, Any]:
    """Parse and normalize aggregate configuration inputs."""

    if 'column_name' not in data_transformation_step or not data_transformation_step['column_name'].strip():
        raise Exception("'column_name' must be provided for aggregate_data transformations")
    if 'aggregation' not in data_transformation_step or not data_transformation_step['aggregation'].strip():
        raise Exception("'aggregation' must be provided for aggregate_data transformations")
    if 'output_column_name' not in data_transformation_step or not data_transformation_step['output_column_name'].strip():
        raise Exception("'output_column_name' must be provided for aggregate_data transformations")

    group_by_columns = data_transformation_step.get('group_by_columns', '')
    group_cols = [col.strip() for col in group_by_columns.split(',') if col.strip()] if group_by_columns else []
    agg_columns = [col.strip() for col in data_transformation_step['column_name'].split(',') if col.strip()]
    agg_functions = [func.strip().lower() for func in data_transformation_step['aggregation'].split(',') if func.strip()]
    output_column_names = [col.strip() for col in data_transformation_step['output_column_name'].split(',') if col.strip()]

    return {
        'group_cols': group_cols,
        'agg_columns': agg_columns,
        'agg_functions': agg_functions,
        'output_column_names': output_column_names
    }


def _validate_aggregate_config(
    df: DataFrame,
    config: Dict[str, Any]
) -> None:
    """Validate aggregate configuration values and column existence."""

    agg_columns = config['agg_columns']
    agg_functions = config['agg_functions']
    output_column_names = config['output_column_names']
    group_cols = config['group_cols']

    if len(agg_columns) != len(agg_functions):
        raise Exception(
            f"Number of columns ({len(agg_columns)}) must match number of aggregation functions ({len(agg_functions)})"
        )
    if len(output_column_names) != len(agg_columns):
        raise Exception(
            f"Number of output column names ({len(output_column_names)}) must match number of columns to aggregate ({len(agg_columns)})"
        )

    invalid_functions = [func for func in agg_functions if func not in _VALID_AGGREGATION_FUNCTIONS]
    if invalid_functions:
        raise Exception(
            f"Invalid aggregation function(s) {invalid_functions}. Valid options are: {_VALID_AGGREGATION_FUNCTIONS}"
        )

    columns_to_check = group_cols + agg_columns
    if columns_to_check:
        _validate_columns_exist(
            df=df,
            columns_to_check=columns_to_check,
            operation_name="aggregate_data"
        )


def _log_aggregate_config(config: Dict[str, Any]) -> None:
    """Log aggregate configuration for observability."""

    agg_pairs = list(zip(config['agg_columns'], config['agg_functions']))
    aggregating_log_info = (
        f"Aggregating data. Group by: {config['group_cols']}, Column-Function pairs: {agg_pairs}, "
        f"Output names: {config['output_column_names']}."
    )
    log_and_print(aggregating_log_info)


def _build_grouped_dataframe(
    df: DataFrame,
    group_cols: List[str]
):
    """Group DataFrame by provided columns or aggregate entire dataset."""

    if group_cols:
        return df.groupBy(*group_cols)
    return df.groupBy()


def _build_aggregate_expressions(
    agg_columns: List[str],
    agg_functions: List[str],
    output_column_names: List[str]
) -> List[Any]:
    """Create Spark aggregation expressions."""

    agg_expressions: List[Any] = []
    for column_name, agg_func, output_col in zip(agg_columns, agg_functions, output_column_names):
        if agg_func == 'sum':
            agg_expressions.append(f.sum(column_name).alias(output_col))
        elif agg_func == 'avg':
            agg_expressions.append(f.avg(column_name).alias(output_col))
        elif agg_func == 'count':
            agg_expressions.append(f.count(column_name).alias(output_col))
        elif agg_func == 'min':
            agg_expressions.append(f.min(column_name).alias(output_col))
        elif agg_func == 'max':
            agg_expressions.append(f.max(column_name).alias(output_col))
        elif agg_func == 'first':
            agg_expressions.append(f.first(column_name).alias(output_col))
        elif agg_func == 'last':
            agg_expressions.append(f.last(column_name).alias(output_col))
        elif agg_func == 'count_distinct':
            agg_expressions.append(f.countDistinct(column_name).alias(output_col))
        else:
            raise Exception(
                f"Invalid aggregation function '{agg_func}' for column '{column_name}'. Valid options are: {_VALID_AGGREGATION_FUNCTIONS}"
            )
    return agg_expressions


def _aggregate_data(
    df: DataFrame, 
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Orchestrate aggregate transformations with helper utilities."""

    config = _parse_aggregate_config(data_transformation_step=data_transformation_step)
    _validate_aggregate_config(df=df, config=config)
    _log_aggregate_config(config=config)
    grouped_df = _build_grouped_dataframe(df=df, group_cols=config['group_cols'])
    agg_expressions = _build_aggregate_expressions(
        agg_columns=config['agg_columns'],
        agg_functions=config['agg_functions'],
        output_column_names=config['output_column_names']
    )
    return grouped_df.agg(*agg_expressions)


def _parse_transform_datetime_config(
    data_transformation_step: Dict[str, Any]
) -> Dict[str, Any]:
    """Parse and normalize configuration for datetime transformations."""

    operation_value = data_transformation_step.get('operation', '').strip()
    if not operation_value:
        raise Exception("'operation' must be provided for transform_datetime transformations")
    operation = operation_value.lower()

    if operation not in _VALID_DATETIME_OPERATIONS:
        raise Exception(
            f"Invalid datetime operation '{operation}'. Valid options are: {_VALID_DATETIME_OPERATIONS}"
        )

    output_column_name = data_transformation_step.get('output_column_name', '').strip()
    if not output_column_name:
        raise Exception("'output_column_name' must be provided for transform_datetime transformations")

    column_name = data_transformation_step.get('column_name', '').strip()

    return {
        'operation': operation,
        'column_name': column_name,
        'output_column_name': output_column_name,
        'days': None,
        'months': None,
        'date_format': '',
        'timestamp_format': None,
        'end_date_column': ''
    }


def _validate_transform_datetime_config(
    df: DataFrame,
    data_transformation_step: Dict[str, Any],
    config: Dict[str, Any]
) -> None:
    """Validate inputs and populate derived values for datetime transformations."""

    operation = config['operation']
    column_name = config['column_name']
    columns_to_check: List[str] = []

    if operation in _DATETIME_OPERATIONS_REQUIRING_COLUMN:
        if not column_name:
            raise Exception(f"'column_name' must be provided for transform_datetime operation '{operation}'")
        columns_to_check.append(column_name)
    elif column_name:
        columns_to_check.append(column_name)

    if operation in {'add_days', 'subtract_days'}:
        days_value = data_transformation_step.get('days', '').strip()
        if days_value == '':
            raise Exception(f"'days' must be provided for transform_datetime operation '{operation}'")
        config['days'] = int(days_value)
    elif operation == 'add_months':
        months_value = data_transformation_step.get('months', '').strip()
        if months_value == '':
            raise Exception("'months' must be provided for transform_datetime operation 'add_months'")
        config['months'] = int(months_value)
    elif operation == 'format_date':
        date_format = data_transformation_step.get('date_format', '').strip()
        if not date_format:
            raise Exception("'date_format' must be provided for transform_datetime operation 'format_date'")
        config['date_format'] = date_format
    elif operation == 'to_timestamp':
        timestamp_format = data_transformation_step.get('timestamp_format', None)
        if timestamp_format is not None:
            config['timestamp_format'] = timestamp_format

    if operation == 'date_diff':
        end_date_column = data_transformation_step.get('end_date_column', '').strip()
        if not end_date_column:
            raise Exception("'end_date_column' must be provided for transform_datetime date_diff operations")
        config['end_date_column'] = end_date_column
        columns_to_check.append(end_date_column)

    if columns_to_check:
        _validate_columns_exist(
            df = df,
            columns_to_check = columns_to_check,
            operation_name = "transform_datetime"
        )


def _log_transform_datetime_config(config: Dict[str, Any]) -> None:
    """Log details about the datetime transformation for observability."""

    column_name = config['column_name']
    operation = config['operation']
    output_column_name = config['output_column_name']

    if column_name:
        datetime_transform_log_info = (
            f"Applying datetime transformation '{operation}' to column '{column_name}' -> '{output_column_name}'."
        )
    else:
        datetime_transform_log_info = (
            f"Applying datetime transformation '{operation}' -> '{output_column_name}'."
        )
    log_and_print(datetime_transform_log_info)


def _apply_transform_datetime_operation(
    df: DataFrame,
    config: Dict[str, Any]
) -> DataFrame:
    """Execute the requested datetime transformation using explicit keyword arguments."""

    operation = config.get('operation')
    output_column_name = config.get('output_column_name')
    column_name = config.get('column_name', '')

    if not operation or not output_column_name:
        raise Exception("'operation' and 'output_column_name' must be provided for transform_datetime transformations")

    if operation == 'year':
        return df.withColumn(output_column_name, f.year(f.col(column_name)))
    if operation == 'month':
        return df.withColumn(output_column_name, f.month(f.col(column_name)))
    if operation in {'day', 'dayofmonth'}:
        return df.withColumn(output_column_name, f.dayofmonth(f.col(column_name)))
    if operation == 'dayofweek':
        return df.withColumn(output_column_name, f.dayofweek(f.col(column_name)))
    if operation == 'dayofyear':
        return df.withColumn(output_column_name, f.dayofyear(f.col(column_name)))
    if operation == 'hour':
        return df.withColumn(output_column_name, f.hour(f.col(column_name)))
    if operation == 'minute':
        return df.withColumn(output_column_name, f.minute(f.col(column_name)))
    if operation == 'second':
        return df.withColumn(output_column_name, f.second(f.col(column_name)))
    if operation == 'quarter':
        return df.withColumn(output_column_name, f.quarter(f.col(column_name)))
    if operation == 'weekofyear':
        return df.withColumn(output_column_name, f.weekofyear(f.col(column_name)))
    if operation == 'add_days':
        days_to_add = config['days']
        return df.withColumn(output_column_name, f.date_add(f.col(column_name), days_to_add))
    if operation == 'subtract_days':
        days_to_subtract = config['days']
        return df.withColumn(output_column_name, f.date_sub(f.col(column_name), days_to_subtract))
    if operation == 'add_months':
        months_to_add = config['months']
        return df.withColumn(output_column_name, f.add_months(f.col(column_name), months_to_add))
    if operation == 'date_diff':
        end_date_column = config['end_date_column']
        return df.withColumn(output_column_name, f.datediff(f.col(end_date_column), f.col(column_name)))
    if operation == 'format_date':
        date_format = config['date_format']
        return df.withColumn(output_column_name, f.date_format(f.col(column_name), date_format))
    if operation == 'to_timestamp':
        timestamp_format = config['timestamp_format']
        if timestamp_format:
            return df.withColumn(output_column_name, f.to_timestamp(f.col(column_name), timestamp_format))
        return df.withColumn(output_column_name, f.to_timestamp(f.col(column_name)))
    if operation == 'current_date':
        return df.withColumn(output_column_name, f.current_date())
    if operation == 'current_timestamp':
        return df.withColumn(output_column_name, f.current_timestamp())

    raise Exception(
        f"Invalid datetime operation '{operation}'. Valid options are: {_VALID_DATETIME_OPERATIONS}"
    )

def _transform_datetime(
    df: DataFrame, 
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Apply date/time transformations and extractions."""
    config = _parse_transform_datetime_config(data_transformation_step)
    _validate_transform_datetime_config(
        df=df,
        data_transformation_step=data_transformation_step,
        config=config
    )
    _log_transform_datetime_config(config=config)
    return _apply_transform_datetime_operation(df=df, config=config)

def _union_data(
    df: DataFrame, 
    data_transformation_step: Dict[str, Any], 
    all_metadata: Dict[str, Any]
) -> DataFrame:
    """Union with other datasets vertically."""
    
    # Extract configuration parameters
    union_type = data_transformation_step.get('union_type', 'by_name').strip().lower()
    # Comma-separated list of datastore.schema.table_name
    union_tables = data_transformation_step['union_tables']  
    allow_missing_columns = data_transformation_step.get('allow_missing_columns', 'true').strip().lower() == 'true'
    deduplicate = data_transformation_step.get('deduplicate', 'false').strip().lower() == 'true'
    
    # Validate union type
    valid_union_types = ['by_name', 'by_position']
    if union_type not in valid_union_types:
        raise Exception(f"Invalid union type '{union_type}'. Valid options are: {valid_union_types}")
    
    # Parse union tables
    table_list = [table.strip() for table in union_tables.split(',')]
    
    union_log_info = f"Union operation: type='{union_type}', tables={table_list}, allow_missing_columns={allow_missing_columns}, deduplicate={deduplicate}."
    log_and_print(union_log_info)

    if union_type == "by_position" and allow_missing_columns:
        union_warn_log_info = f'If `union_type` = `by_position`, the `allow_missing_columns` parameter is ignored. The function will always fail on schema mismatch.'
        log_and_print(union_warn_log_info, "warn")
    
    # Start with the original DataFrame
    result_df = df
    
    # Union with each specified table
    for table_name in table_list:
        datastore_name = table_name.split('.')[0].strip()
        workspace_name = _get_datastore_config(all_metadata['datastore_config'], datastore_name, 'Workspace_Name')
        full_table_name = f"`{workspace_name}`.{table_name}"
        
        union_table_log = f"Reading and union with table: {full_table_name}"
        log_and_print(union_table_log)
        
        # Read the table to union
        union_df = spark.read.table(full_table_name)
        
        # Perform union based on type
        if union_type == 'by_name':
            result_df = result_df.unionByName(union_df, allowMissingColumns = allow_missing_columns)
        else:  # by_position
            result_df = result_df.unionAll(union_df)
    
    # Apply deduplication if requested
    if deduplicate:
        dedup_log = "Applying deduplication to union result."
        log_and_print(dedup_log)
        result_df = result_df.dropDuplicates()
    
    return result_df

def _apply_null_handling(
    df: DataFrame, 
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Handle null values by filling, dropping, or replacing."""
    
    column_name = data_transformation_step['column_name']
    action = data_transformation_step['action'].strip().lower()
    
    # Handle multiple columns if comma-separated
    columns = [col.strip() for col in column_name.split(',')]
    
    # Validate columns exist
    _validate_columns_exist(
        df = df, 
        columns_to_check = columns, 
        operation_name = "apply_null_handling"
    )
    
    # Validate action type
    valid_actions = ['drop_rows', 'replace_with_default', 'convert_values_to_null']

    if action == 'drop_rows':
        df = _drop_null_rows(df, columns, data_transformation_step)
    elif action == 'replace_with_default':
        df = _replace_nulls_with_default(df, columns, data_transformation_step)
    elif action == 'convert_values_to_null':
        df = _convert_values_to_null(df, columns, data_transformation_step)
    else:
        raise Exception(f"Invalid null handling action '{action}'. Valid options are: {valid_actions}")
    
    return df

def transformation_functions(
    df: DataFrame,
    data_transformation_steps: List[Dict[str, Any]],
    first_run: bool,
    target_table_name: str,
    primary_keys: List[str],
    dimension_config: dict,
    watermark_config: dict,
    custom_paths: dict,
    merge_type: str,
    all_metadata: Dict[str, Any],
    lineage_info: dict
) -> DataFrame:
    """
    Apply metadata-driven transformations to DataFrames based on configuration.

    This function serves as a transformation orchestrator that executes various
    data transformation operations in sequence. It supports both standard
    transformations (column operations, filtering) and advanced features like
    entity resolution for matching records across datasets.

    The function processes transformations in the order they appear in the
    metadata, ensuring predictable and repeatable results.

    Args:

    Returns:
        DataFrame: Transformed DataFrame with all operations applied

    Transformation Categories:
        1. columns_to_rename: Rename columns to match target schema
        2. derived_column: Create new columns using Spark SQL expressions
        3. remove_columns: Drop unnecessary columns
        4. select_columns: Select only specified columns (with optional metadata retention)
        5. filter_data: Apply row-level filters to subset data
        6. sort_data: Sort data by one or more columns with configurable direction
        7. create_surrogate_key: Generate surrogate keys for dimension tables
        8. attach_dimension_surrogate_key: Insert surrogate keys from reference tables
        9. drop_duplicates: Remove duplicate rows using exact or primary key logic
        10. change_data_types: Change data types for one or more columns
        11. apply_null_handling: Handle null values (fill/drop/replace/convert values to null)
        12. replace_values: Replace multiple different values across one or more columns with a specific value
        13. mask_sensitive_data: Mask sensitive data for PII protection and anonymization
        14. add_row_hash: Add hash column for change detection and data lineage
        15. pivot_data: Pivot data to convert rows to columns
        16. unpivot_data: Unpivot data to convert columns to rows (melt operation)
        17. join_data: Join with another table or dataset using various join types
        18. add_window_function: Add window function calculations (rank, row_number, lag, lead, etc.)
        19. aggregate_data: Group by and aggregate data with multiple aggregation functions
        20. transform_datetime: Apply date/time transformations and extractions
        21. union_data: Union with other datasets vertically
        22. entity_resolution: Match and merge records from multiple sources
        23. custom_transformation_function: Execute user-defined transformation functions
        24. explode_array: Explode array column into multiple rows
        25. flatten_struct: Flatten nested struct columns into individual columns
        26. split_column: Split a string column into multiple columns by delimiter
        27. concat_columns: Concatenate multiple columns into a single column
        28. conditional_column: Create column with conditional logic (CASE WHEN)
        29. string_functions: Apply string transformation functions (upper, lower, trim, etc.)
        30. normalize_text: Normalize text data for consistency and matching

    Raises:
        Exception: If an unknown transformation category is specified
        Exception: If filter removes all data on first run (data quality check)

    Notes:
        - Transformations are applied sequentially in metadata order
        - Filter operations that remove all data trigger different behaviors
          based on whether it's a first run (error) or incremental (graceful exit)
        - Entity resolution is triggered by presence of specific config keys
        - All column validation is case-insensitive to handle Spark behavior
    """
    # Process each transformation step from metadata
    for data_transformation_step in data_transformation_steps:
        data_transformation_category = data_transformation_step["Category"]

        if data_transformation_category == "columns_to_rename":
            df = _rename_columns(
                df = df, 
                data_transformation_step = data_transformation_step
            )

        elif data_transformation_category == "derived_column":
            df = _create_derived_column(
                df = df, 
                data_transformation_step = data_transformation_step
            )

        elif data_transformation_category == "remove_columns":
            df = _remove_columns(
                df = df, 
                data_transformation_step = data_transformation_step
            )

        elif data_transformation_category == "select_columns":
            df = _select_columns(
                df = df, 
                data_transformation_step = data_transformation_step
            )

        elif data_transformation_category == "filter_data":
            df = _filter_data(
                df = df, 
                data_transformation_step = data_transformation_step
            )

        elif data_transformation_category == "sort_data":
            df = _sort_data(
                df = df,
                data_transformation_step = data_transformation_step
            )

        elif data_transformation_category == "create_surrogate_key":
            df = _create_surrogate_key(
                df = df, 
                data_transformation_step = data_transformation_step, 
                target_table_name = target_table_name, 
                primary_keys = primary_keys, 
                first_run = first_run, 
                column_to_mark_source_data_deletion = watermark_config['column_to_mark_source_data_deletion'], 
                merge_type = merge_type
            )

        elif data_transformation_category == "attach_dimension_surrogate_key":
            df = _attach_dimension_surrogate_key(
                df = df, 
                surrogate_key_logic = data_transformation_step,
                all_metadata = all_metadata, 
                dimension_table_key_column_name = dimension_config['dimension_table_key_column_name']
            )

        elif data_transformation_category == "entity_resolution":
            df = entity_resolution(
                primary_dataset_df = df,
                data_transformation_config = data_transformation_step
            )

        elif data_transformation_category == "drop_duplicates":
            df = _drop_duplicates(
                df = df, 
                data_transformation_step = data_transformation_step
            )

        elif data_transformation_category == "change_data_types":
            df = _change_data_types(
                df = df, 
                data_transformation_step = data_transformation_step
            )

        elif data_transformation_category == "apply_null_handling":
            df = _apply_null_handling(
                df = df, 
                data_transformation_step = data_transformation_step
            )

        elif data_transformation_category == "replace_values":
            df = _replace_values(
                df = df, 
                data_transformation_step = data_transformation_step
            )

        elif data_transformation_category == "mask_sensitive_data":
            df = _mask_sensitive_data(
                df = df, 
                data_transformation_step = data_transformation_step
            )

        elif data_transformation_category == "add_row_hash":
            df = _add_row_hash(
                df = df, 
                data_transformation_step = data_transformation_step
            )

        elif data_transformation_category == "pivot_data":
            df = _pivot_data(
                df = df, 
                data_transformation_step = data_transformation_step
            )

        elif data_transformation_category == "unpivot_data":
            df = _unpivot_data(
                df = df, 
                data_transformation_step = data_transformation_step
            )

        elif data_transformation_category == "join_data":
            df = _join_data(
                df = df, 
                data_transformation_step = data_transformation_step,
                all_metadata = all_metadata
            )

        elif data_transformation_category == "add_window_function":
            df = _add_window_function(
                df = df, 
                data_transformation_step = data_transformation_step
            )

        elif data_transformation_category == "aggregate_data":
            df = _aggregate_data(
                df = df, 
                data_transformation_step = data_transformation_step
            )

        elif data_transformation_category == "transform_datetime":
            df = _transform_datetime(
                df = df, 
                data_transformation_step = data_transformation_step
            )

        elif data_transformation_category == "union_data":
            df = _union_data(
                df = df, 
                data_transformation_step = data_transformation_step,
                all_metadata = all_metadata
            )

        elif data_transformation_category == "custom_transformation_function":
            df = _execute_custom_transformation_function(
                df = df, 
                data_transformation_step = data_transformation_step, 
                all_metadata = all_metadata
            )

        elif data_transformation_category == "explode_array":
            df = _explode_array(
                df = df,
                data_transformation_step = data_transformation_step
            )

        elif data_transformation_category == "flatten_struct":
            df = _flatten_struct(
                df = df,
                data_transformation_step = data_transformation_step
            )

        elif data_transformation_category == "split_column":
            df = _split_column(
                df = df,
                data_transformation_step = data_transformation_step
            )

        elif data_transformation_category == "concat_columns":
            df = _concat_columns(
                df = df,
                data_transformation_step = data_transformation_step
            )

        elif data_transformation_category == "conditional_column":
            df = _conditional_column(
                df = df,
                data_transformation_step = data_transformation_step
            )

        elif data_transformation_category == "string_functions":
            df = _string_functions(
                df = df,
                data_transformation_step = data_transformation_step
            )

        elif data_transformation_category == "normalize_text":
            df = _normalize_text(
                df = df,
                data_transformation_step = data_transformation_step
            )

        else:
            # Fail fast on unknown transformation types
            raise Exception(f"No logic currently exists to run the {data_transformation_category} data transformation.")

    # Validate that transformations haven't removed all data
    # Exits gracefully if empty (subsequent runs) or raises exception (first run)
    _validate_transformation_results(df, first_run, lineage_info)
    
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Merge and Delta Table Management Functions
# 
# This section contains advanced merge patterns and Delta table management utilities that handle various data integration scenarios. These functions are critical for maintaining data consistency and implementing different merge strategies based on business requirements.
# 
# Key capabilities include:
# - **Merge Condition Generation**: Dynamic SQL condition building for complex joins
# - **Delta Table Creation**: DDL generation with liquid clustering and external location support
# - **Merge Strategies**: Multiple merge patterns including standard upserts, SCD2, selective overwrites, and deletion tracking
# - **Performance Optimization**: Leverages Delta Lake features for efficient data operations

# CELL ********************

def get_merge_condition(merge_keys: list, right_alias: str, left_alias: str):
    """
    Generate a null-safe merge condition string for DataFrame merge operations.

    This function constructs SQL join conditions that properly handle NULL values
    using Spark's null-safe equality operator (<=>). This is crucial for ensuring
    that NULL values in join keys are handled correctly during merge operations.

    Args:
        merge_keys (list): List of column names to use as join keys
        right_alias (str): Alias for the source/right DataFrame in the merge
        left_alias (str): Alias for the target/left DataFrame in the merge

    Returns:
        str: SQL merge condition string with null-safe comparisons

    Example:
        >>> get_merge_condition(['id', 'date'], 'source', 'target')
        'target.`id` <=> source.`id` AND target.`date` <=> source.`date`'

    Notes:
        - Uses backticks around column names to handle special characters
        - The <=> operator returns true when both operands are NULL
        - Essential for SCD2 implementations where NULL end dates are common
    """
    merge_condition = ""

    if not merge_keys:
        raise Exception('To enable data merging into the table, ensure that Primary Keys are specified in the Orchestration metadata table. Alternatively, update the merge_type in the primary configuration to options such as Overwrite or Append if merging is not required.')
    for pk in merge_keys:
        if merge_condition != "":
            merge_condition += " AND "
        # <=> is null safe comparison operator - handles NULL = NULL as true
        merge_condition += f"{left_alias}.`{pk}` <=> {right_alias}.`{pk}`"
    return merge_condition

def _prepare_merge_config(
    data_to_merge: DataFrame,
    primary_keys: list,
    source_alias: str,
    target_alias: str
) -> tuple[str, dict, dict]:
    """
    Prepare merge condition and update columns for Delta table merge operations.
    
    This function generates the merge condition and update column mapping needed for
    Delta Lake merge operations, automatically excluding system columns that should
    not be updated (delta__created_datetime and Change Data Feed columns).
    
    Args:
        data_to_merge (DataFrame): Source DataFrame to merge
        primary_keys (list): List of primary key column names for merge condition
        source_alias (str): Alias for source DataFrame in merge condition
        target_alias (str): Alias for target DataFrame in merge condition
    
    Returns:
        tuple[str, dict, dict]: A tuple containing:
            - merge_condition (str): SQL condition string for merge operation
            - update_columns (dict): Dictionary mapping target columns to source column references
            - insert_columns (dict): Dictionary mapping column names for inserts (excludes CDF helpers)
    
    Excluded Columns:
        - delta__created_datetime: Preserved from initial insert
        - Primary key columns: Used for matching, not updating
        - _change_type: Change Data Feed system column
        - _commit_version: Change Data Feed system column
        - _commit_timestamp: Change Data Feed system column
    
    Example:
        >>> merge_condition, update_columns, insert_columns = _prepare_merge_config(
        ...     data_to_merge=df,
        ...     primary_keys=['customer_id'],
        ...     source_alias='source',
        ...     target_alias='target'
        ... )
    """
    # Change Data Feed system columns that should be excluded from updates
    cdf_system_columns = {'_change_type', '_commit_version', '_commit_timestamp'}
    
    merge_condition = get_merge_condition(
        merge_keys=primary_keys,
        right_alias=source_alias,
        left_alias=target_alias
    )
    
    insert_columns = {
        column: f"{source_alias}.`{column}`"
        for column in data_to_merge.columns
        if column not in cdf_system_columns
    }

    update_columns = {
        column: f"{source_alias}.`{column}`"
        for column in data_to_merge.columns
        if column != "delta__created_datetime" 
           and column not in primary_keys
           and column not in cdf_system_columns
    }
    
    return merge_condition, update_columns, insert_columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_delta_table(
    df: DataFrame,
    liquid_clustering_columns: list,
    target_table_name: str,
    output_external_location: str,
    lakehouse_table_output: bool,
    target_table_exists: bool
):
    """
    Dynamically create a Delta table with optimal configuration based on input DataFrame schema.

    This function generates and executes DDL statements to create Delta tables with:
    - Schema inference from the input DataFrame
    - Liquid clustering for optimized query performance
    - Optional external storage location support
    - Column-level comments and constraints preservation

    Args:
        df (DataFrame): Source DataFrame whose schema will be used for table creation
        liquid_clustering_columns (list): Columns to use for liquid clustering optimization
        target_table_name (str): Fully qualified table name (schema.table)
        output_external_location (str): Optional ADLS Gen2 path for external table storage
        lakehouse_table_output (bool): target is lakehouse table
        target_table_exists (bool): whether table already exists

    Implementation Details:
        1. Creates temporary view to extract schema metadata
        2. Iterates through columns to build DDL with proper data types
        3. Preserves column descriptions and nullable constraints
        4. Applies liquid clustering if specified
        5. Supports external Delta tables for cross-platform scenarios

    Notes:
        - Temporary view is automatically cleaned up after use
        - Supports all Spark SQL data types
        - External tables enable OneLake shortcut creation
    """

    if not lakehouse_table_output or target_table_exists:
        return

    log_and_print("Creating table.")

    column_details = [{
        "name": field.name,
        "type": field.dataType.simpleString(),
        "nullable": field.nullable
    } for field in df.schema.fields]


    # Begin constructing CREATE TABLE statement
    spark_ddl = f"""CREATE TABLE {target_table_name} (
        """
    
    # Build column definitions with proper formatting
    cdf_system_columns = {"_change_type", "_commit_version", "_commit_timestamp"}
    column_written = False
    for column_detail in column_details:
        col_name = column_detail['name']
        data_type = column_detail['type']
        nullable = column_detail['nullable']

        # Skip Change Data Feed helper columns that should never exist in target schemas
        if col_name in cdf_system_columns:
            continue

        # Add NOT NULL constraint if column is non-nullable
        if not nullable:
            nullable_text = " NOT NULL"
        else:
            nullable_text = ""

        # Format column definition with proper comma placement
        if not column_written:
            column_definition = f"`{col_name}` {data_type} {nullable_text} \n"
            column_written = True
        else:
            column_definition = f",`{col_name}` {data_type} {nullable_text} \n"

        spark_ddl += column_definition
    
    # Add liquid clustering for query optimization
    if liquid_clustering_columns:
        liquid_clustering_columns_str = ', '.join(liquid_clustering_columns)
        clusterBy = f"CLUSTER BY ({liquid_clustering_columns_str})"
    else:
        clusterBy = ""

    # Add external location for ADLS Gen2 storage
    if output_external_location:
        external_table_path = f"LOCATION '{output_external_location}'"
    else:
        external_table_path = ""

    # Complete DDL with table properties
    spark_ddl += f""")
        USING delta
        {clusterBy}
        {external_table_path}
    """

    # Execute the CREATE TABLE statement
    spark.sql(spark_ddl)

# Data types that Delta does not support for data skipping statistics
_DATA_SKIPPING_UNSUPPORTED_TYPES = (BooleanType, ArrayType, MapType, StructType, BinaryType, NullType)

def _filter_unsupported_stats_columns(
    df: DataFrame,
    columns: List[str]
) -> List[str]:
    """
    Filter out columns with data types unsupported by Delta data skipping.
    
    Delta Lake's data skipping feature does not support statistics collection
    on BooleanType, ArrayType, MapType, StructType, BinaryType, or NullType columns.
    Attempting to configure statistics on these types raises an IllegalArgumentException.
    
    Args:
        df (DataFrame): Source DataFrame to check column types against
        columns (List[str]): Column names to filter
    
    Returns:
        List[str]: Columns with supported types for data skipping
    """
    schema_lookup = {field.name.lower(): field.dataType for field in df.schema.fields}
    
    eligible = [col for col in columns if not isinstance(schema_lookup.get(col.lower()), _DATA_SKIPPING_UNSUPPORTED_TYPES)]
    skipped = [col for col in columns if isinstance(schema_lookup.get(col.lower()), _DATA_SKIPPING_UNSUPPORTED_TYPES)]
    
    if skipped:
        skipped_details = [(col, str(schema_lookup.get(col.lower()))) for col in skipped]
        log_and_print(f"Excluded {len(skipped)} column(s) with unsupported types for data skipping: {skipped_details}")
    
    return eligible

def set_column_statistics(
    df: DataFrame,
    compute_statistics_on_columns: str,
    compute_statistics_on_first_n_columns: str,
    lakehouse_table_output: bool
) -> None:
    """
    Configure Delta table column statistics for query optimization.
    
    This function enables column-level statistics collection on Delta tables to improve
    query performance through better query planning and optimization. Statistics help
    the Spark catalyst optimizer make informed decisions about join ordering, filter
    pushdown, and partition pruning.
    
    Args:
        df (DataFrame): Source DataFrame to analyze for column statistics
        compute_statistics_on_columns (str): Comma-separated list of specific columns to collect stats on
        compute_statistics_on_first_n_columns (str): Number of first columns to collect stats on (alternative to specific columns)
        lakehouse_table_output (bool): target is lakehouse table
    Configuration Logic:
        1. If compute_statistics_on_columns is specified: Use those exact columns (validates existence)
        2. If compute_statistics_on_first_n_columns is specified: Use first N columns + delta timestamp columns
        3. If neither specified: Defaults to first 32 columns + delta timestamp columns
    
    Automatic Delta Column Inclusion:
        - Always includes 'delta__created_datetime' if present in DataFrame
        - Always includes 'delta__modified_datetime' if present in DataFrame
        - These columns are critical for query optimization and incremental processing
        - Only added if not already included in the first N columns
    
    Error Handling:
        - Raises ValueError if specified columns don't exist in the DataFrame
        - Provides detailed error message with available columns for debugging
        - Ensures data integrity by failing fast on invalid configurations
    
    Notes:
        - Column statistics improve query performance for filtering and joining
        - Recommended for frequently queried columns like keys, dates, and status fields
        - Statistics collection has minimal overhead during write operations
        - Cannot combine both column list and first N columns approaches
        - Delta timestamp columns are automatically prioritized for optimal performance
        - Columns with unsupported types (Boolean, Array, Map, Struct, Binary, Null) are
          automatically excluded to prevent IllegalArgumentException
    
    Raises:
        ValueError: When compute_statistics_on_columns contains non-existent columns
    """

    if not lakehouse_table_output:
        return
    
    # Default to first 32 columns if no statistics configuration is provided
    # this is the default behavior of Fabric: https://learn.microsoft.com/en-us/fabric/data-engineering/automated-table-statistics
    if not compute_statistics_on_columns and not compute_statistics_on_first_n_columns:
        compute_statistics_on_first_n_columns = '32'
    
    # Determine which columns to collect statistics on
    stats_columns = []
    
    if compute_statistics_on_columns:
        # Use specific columns provided by user
        stats_columns = [col.strip() for col in compute_statistics_on_columns.split(',')]

        log_and_print(f"Configuring statistics for columns: {stats_columns}.")
        
        # Validate all specified columns exist in DataFrame - fail fast if any are missing (case-insensitive)
        existing_columns_lower = [col.lower() for col in df.columns]
        missing_columns = [col for col in stats_columns if col.lower() not in existing_columns_lower]
        if missing_columns:
            error_msg = f"Cannot configure statistics for non-existent columns: {missing_columns}. Available columns: {df.columns}"
            raise ValueError(error_msg)
        
        # Filter out unsupported types even for explicitly specified columns
        stats_columns = _filter_unsupported_stats_columns(df, stats_columns)

    else:
        # Use first N columns approach with automatic delta column inclusion
        n_columns = int(compute_statistics_on_first_n_columns)
        log_and_print(f"Configuring statistics for first {n_columns} eligible columns.")
        
        # Filter out unsupported types BEFORE selecting first N so we always get N eligible columns
        eligible_columns = _filter_unsupported_stats_columns(df, df.columns)
        
        stats_columns = eligible_columns[:n_columns]
        
        # Identify delta timestamp columns that exist and are eligible
        delta_datetime_columns = []
        if "delta__created_datetime" in eligible_columns:
            delta_datetime_columns.append("delta__created_datetime")
        if "delta__modified_datetime" in eligible_columns:
            delta_datetime_columns.append("delta__modified_datetime")
        
        # Automatically add delta timestamp columns for optimal query performance
        added_delta_columns = []
        for delta_col in delta_datetime_columns:
            if delta_col not in stats_columns:
                stats_columns.append(delta_col)
                added_delta_columns.append(delta_col)
        
        # Log automatic inclusion of delta timestamp columns for transparency
        if added_delta_columns:
            delta_log_msg = f"Automatically including delta timestamp columns in statistics: {added_delta_columns}"
            log_and_print(delta_log_msg)
    
    if not stats_columns:
        log_and_print("No columns eligible for data skipping statistics after filtering unsupported types. Skipping statistics configuration.")
        return
    
    # Configure Spark with the final list of columns for statistics collection
    stats_config_with_backticks = [f"`{col}`" for col in stats_columns]
    stats_config_comma_separated = ",".join(stats_config_with_backticks)
    # Apply the statistics configuration to Spark session
    spark.conf.set("spark.databricks.delta.properties.defaults.dataSkippingStatsColumns", stats_config_comma_separated)
    log_and_print(f"Spark Config: spark.databricks.delta.properties.defaults.dataSkippingStatsColumns = {stats_config_comma_separated}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


def _apply_datetime_substitutions(path: str) -> str:
    """
    Apply datetime placeholder substitutions to a file path.
    
    Imports datetime locally to avoid conflicts with custom functions that may
    have modified the datetime module in the global namespace.
    
    Supported placeholders:
        %Y - Four-digit year (e.g., 2026)
        %m - Two-digit month (01-12)
        %d - Two-digit day of month (01-31)
        %B - Full month name (e.g., January)
        %b - Abbreviated month name (e.g., Jan)
        %H - Two-digit hour in 24-hour format (00-23)
        %M - Two-digit minute (00-59)
        %S - Two-digit second (00-59)
        %f - Microseconds (000000-999999)
        %j - Day of year (001-366)
        %W - Week number of year (00-53)
        %A - Full weekday name (e.g., Monday)
        %a - Abbreviated weekday name (e.g., Mon)
    
    Args:
        path: File path potentially containing datetime placeholders
        
    Returns:
        Path with all datetime placeholders replaced with current values
    """
    from datetime import datetime as dt_module
    dt_now = dt_module.now()
    
    # Define all supported datetime placeholders
    datetime_placeholders = [
        '%Y',  # Four-digit year
        '%m',  # Two-digit month
        '%d',  # Two-digit day
        '%B',  # Full month name
        '%b',  # Abbreviated month name
        '%H',  # Hour (24-hour)
        '%M',  # Minute
        '%S',  # Second
        '%f',  # Microseconds
        '%j',  # Day of year
        '%W',  # Week number
        '%A',  # Full weekday name
        '%a',  # Abbreviated weekday name
    ]
    
    result_path = path
    for placeholder in datetime_placeholders:
        if placeholder in result_path:
            result_path = result_path.replace(placeholder, dt_now.strftime(placeholder))
    
    return result_path


def _write_spark_file_with_rename(
    data_to_merge: DataFrame,
    output_path: str,
    file_format: str,
    delimiter: str = ','
) -> None:
    """
    Write DataFrame using Spark to avoid OOM errors, then rename to desired filename.
    
    Spark writes files to directories with part-xxxx naming which is not consumer-friendly.
    This function writes to a temp directory, then uses notebookutils to rename the file
    to the exact path specified by the user.
    
    Args:
        data_to_merge: Spark DataFrame to write
        output_path: Desired output file path (abfss://...)
        file_format: File format ('csv', 'txt', 'parquet', 'json', 'jsonl', or 'ndjson')
        delimiter: Field delimiter for CSV/TXT files (default: ',')
    """
    import uuid
    
    # Generate unique temp directory path adjacent to target file
    path_without_filename = output_path.rsplit('/', 1)[0]
    temp_dir_name = f"_temp_spark_output_{uuid.uuid4().hex[:8]}"
    temp_dir_path = f"{path_without_filename}/{temp_dir_name}"
    
    write_spark_file_log_info = f"Writing {file_format} file using Spark to avoid memory issues. Temp path: {temp_dir_path}"
    log_and_print(write_spark_file_log_info)
    
    try:
        # Write using Spark with coalesce(1) to produce single file
        # This avoids pandas OOM while producing a single output file
        if file_format in ('csv', 'txt'):
            data_to_merge.coalesce(1).write.mode("overwrite").option("header", "true").option("delimiter", delimiter).csv(temp_dir_path)
        elif file_format == 'parquet':
            data_to_merge.coalesce(1).write.mode("overwrite").parquet(temp_dir_path)
        elif file_format in ('json', 'jsonl', 'ndjson'):
            # JSON Lines format - one JSON object per line (Spark's native JSON output)
            # Note: All JSON formats produce the same output; .json extension is kept for 
            # compatibility but consumers should expect JSON Lines, not a JSON array
            data_to_merge.coalesce(1).write.mode("overwrite").json(temp_dir_path)
        
        # Find the actual output file (part-00000-xxx.format)
        files = notebookutils.fs.ls(temp_dir_path)
        data_file = None
        for file_info in files:
            file_name = file_info.name
            if file_name.startswith("part-") and not file_name.endswith(".crc"):
                data_file = file_name
                break
        
        if data_file is None:
            raise Exception(f"Could not find output data file in temp directory: {temp_dir_path}")
        
        temp_file_path = f"{temp_dir_path}/{data_file}"
        
        # Move/rename to the desired output path
        rename_log_info = f"Renaming temp file to final destination: {output_path}"
        log_and_print(rename_log_info)
        notebookutils.fs.mv(temp_file_path, output_path, overwrite=True)
        
    finally:
        # Clean up temp directory (non-critical - main write already succeeded if we got here)
        try:
            notebookutils.fs.rm(temp_dir_path, recurse=True)
        except Exception as cleanup_error:
            # Log but don't raise - cleanup failure shouldn't mask a successful write operation
            cleanup_warning_msg = f"Warning: Could not clean up temp directory {temp_dir_path}: {cleanup_error}. The file was written successfully but orphaned temp files may need manual cleanup."
            log_and_print(cleanup_warning_msg, level="WARNING")


def _write_excel_file(
    data_to_merge: DataFrame, 
    output_path: str, 
    sheet_name: str
) -> None:
    """
    Write DataFrame to Excel file using pandas.
    
    Excel requires pandas as there is no Spark Excel writer in Fabric runtime.
    Includes row count validation to prevent exceeding Excel limits or causing
    out-of-memory errors during Spark-to-pandas conversion.
    
    Limits:
        - Hard limit: 1,048,576 rows (Excel maximum)
        - Warning at: 500,000 rows (pandas conversion may cause OOM)
    
    Args:
        data_to_merge: Spark DataFrame to write
        output_path: Desired output file path (abfss://...)
        sheet_name: Name of the Excel sheet to write to
        
    Raises:
        Exception: If row count exceeds Excel's maximum limit
    """
    row_count = data_to_merge.count()
    
    # Excel has a hard limit of ~1M rows, and pandas conversion becomes risky above ~500K
    excel_row_limit = 1048576  # Excel max rows
    pandas_safe_limit = 500000  # Recommended limit for pandas conversion
    
    if row_count > excel_row_limit:
        error_msg = f"Error: Dataset has {row_count:,} rows which exceeds Excel's maximum of {excel_row_limit:,} rows. Use CSV or Parquet format instead."
        log_and_print(error_msg, level="ERROR")
        raise Exception(error_msg)
    
    if row_count > pandas_safe_limit:
        warning_msg = f"Warning: Dataset has {row_count:,} rows. Large Excel files may cause memory issues. Consider using CSV or Parquet format for datasets over {pandas_safe_limit:,} rows."
        log_and_print(warning_msg, level="WARNING")
    
    # Arrow optimization is enabled by default in Fabric Runtime 1.3+ (Spark 3.5)
    # which provides more efficient Spark to Pandas conversion
    excel_log_info = f"Writing Excel file: {output_path} (sheet: {sheet_name}, rows: {row_count:,})"
    log_and_print(excel_log_info)
    
    data_to_merge_pandas = data_to_merge.toPandas()
    data_to_merge_pandas.to_excel(output_path, sheet_name=sheet_name, index=False)


def _write_output_file(
    data_to_merge: DataFrame,
    target_abfss_path: str,
    target_config: dict
) -> None:
    """
    Route file output to format-specific handlers with datetime substitution.
    
    This is the main dispatcher for file output operations. It applies datetime
    placeholders to the output path, determines the file format from the extension,
    and routes to the appropriate format-specific handler.
    
    For large datasets, Spark-based handlers (CSV, TXT, Parquet, JSON, JSONL) avoid
    out-of-memory errors. Excel uses pandas as there is no Spark alternative.
    
    Dynamic datetime placeholders supported in file path:
        %Y, %m, %d, %B, %b, %H, %M, %S, %f, %j, %W, %A, %a
        
    Example paths:
        - reports_%Y-%m-%d.csv -> reports_2026-01-22.csv
        - data_%Y%m%d_%H%M%S.parquet -> data_20260122_143025.parquet
        - monthly/%B_%Y/export.xlsx -> monthly/January_2026/export.xlsx
        - events_%Y%m%d.jsonl -> events_20260122.jsonl
        
    Supported formats:
        - csv: Comma-separated values with header row
        - txt: Text file (same as CSV format)
        - parquet: Columnar binary format (efficient for analytics)
        - json: JSON Lines format (one JSON object per line)
        - jsonl: JSON Lines format (explicit extension)
        - ndjson: Newline-delimited JSON (alias for JSON Lines)
        - xls/xlsx: Excel workbook (requires pandas, limited to ~1M rows)
    
    Args:
        data_to_merge: Spark DataFrame to write
        target_abfss_path: Target file path with optional datetime placeholders
        target_config: Configuration dict containing format-specific options
                       (e.g., target_excel_sheet_name for Excel files,
                        target_output_delimiter for CSV/TXT files)
    
    Raises:
        Exception: If file extension is not supported
    """
    # Apply datetime substitutions (imports datetime locally to avoid conflicts)
    output_path = _apply_datetime_substitutions(target_abfss_path)

    # Determine file format from extension
    target_file_extension = output_path.split('.')[-1].lower()
    
    # Get custom delimiter if configured (empty string means use format default)
    custom_delimiter = target_config['target_output_delimiter']
    
    writing_file_log_info = f"Writing output file: {output_path} (format: {target_file_extension})"
    log_and_print(writing_file_log_info)
    
    # Route to appropriate handler
    if target_file_extension == 'csv':
        delimiter = custom_delimiter if custom_delimiter else ','
        _write_spark_file_with_rename(data_to_merge, output_path, 'csv', delimiter)
        
    elif target_file_extension == 'txt':
        delimiter = custom_delimiter if custom_delimiter else '\t'
        _write_spark_file_with_rename(data_to_merge, output_path, 'txt', delimiter)
        
    elif target_file_extension == 'parquet':
        _write_spark_file_with_rename(data_to_merge, output_path, 'parquet')
        
    elif target_file_extension in ('json', 'jsonl', 'ndjson'):
        _write_spark_file_with_rename(data_to_merge, output_path, 'json')
        
    elif target_file_extension in ('xls', 'xlsx'):
        sheet_name = target_config.get('target_excel_sheet_name', 'Sheet1')
        _write_excel_file(data_to_merge, output_path, sheet_name)
        
    else:
        allowed_extensions = "'csv', 'txt', 'parquet', 'json', 'jsonl', 'ndjson', 'xls', 'xlsx'"
        raise Exception(
            f"Invalid file extension '{target_file_extension}'. "
            f"Allowed extensions are {allowed_extensions}."
        )


def _write_warehouse_spark_connector(
    data_to_merge: DataFrame,
    target_datastore_workspace_id: str,
    target_table_name: str,
    warehouse_write_mode: str
) -> None:
    """Write data to Fabric Warehouse table using spark connector."""
    writing_warehouse_table_log_info = f"Writing data to Fabric Warehouse table using {warehouse_write_mode} logic."
    log_and_print(writing_warehouse_table_log_info)

    target_table_name_without_ws = target_table_name.split('.', 1)[1]

    data_to_merge.write.mode(warehouse_write_mode).option(Constants.WorkspaceId, target_datastore_workspace_id) \
                   .synapsesql(target_table_name_without_ws)

def _handle_first_run(
    data_to_merge: DataFrame,
    target_abfss_path: str
) -> bool:
    """Handle first run write operation."""
    first_run_log_info = "This is the first run. Appending data to table."
    log_and_print(first_run_log_info)

    # Drop Change Data Feed helper columns so they are never written to user tables
    cdf_system_columns = [col for col in ("_change_type", "_commit_version", "_commit_timestamp") if col in data_to_merge.columns]
    if cdf_system_columns:
        log_and_print(f"Dropping Change Data Feed helper columns on first run before insert: {cdf_system_columns}.")
        cleaned_data = data_to_merge.drop(*cdf_system_columns)
    else:
        cleaned_data = data_to_merge

    cleaned_data.write.mode("append").save(target_abfss_path)
    return False

def _handle_append(
    data_to_merge: DataFrame,
    target_abfss_path: str
) -> None:
    """Handle simple append operation."""
    appending_data_log_info = "Appending data to existing table"
    log_and_print(appending_data_log_info)

    data_to_merge.write.mode("append").save(target_abfss_path)

def _handle_overwrite(
    data_to_merge: DataFrame,
    target_abfss_path: str
) -> None:
    """Handle full table replacement with schema evolution."""
    overwriting_data_log_info = "Overwriting data in existing table"
    log_and_print(overwriting_data_log_info)

    data_to_merge.write.mode("overwrite").option("overwriteSchema", "true").save(target_abfss_path)

def _handle_merge(
    target_table: DeltaTable,
    data_to_merge: DataFrame,
    primary_keys: list,
    target_alias: str,
    source_alias: str
) -> None:
    """Handle standard upsert operation."""
    merging_data_log_info = "Merging data to existing table"
    log_and_print(merging_data_log_info)
    
    merge_condition, update_columns, insert_columns = _prepare_merge_config(
        data_to_merge=data_to_merge,
        primary_keys=primary_keys,
        source_alias=source_alias,
        target_alias=target_alias
    )
    
    target_table.alias(target_alias).merge(source = data_to_merge.alias(source_alias)
                    ,condition = merge_condition
                ).whenMatchedUpdate(set = update_columns
                ).whenNotMatchedInsert(values = insert_columns
                ).execute()

def _handle_merge_and_delete(
    target_table: DeltaTable,
    data_to_merge: DataFrame,
    primary_keys: list,
    target_alias: str,
    source_alias: str,
    delete_rows_with_value: str,
    column_to_mark_source_data_deletion: str
) -> None:
    """Handle merge with deletion of marked records."""
    deleting_marked_data_log_info = f"Deleting data marked as deleted in source. All rows with a value of {delete_rows_with_value} in column, {column_to_mark_source_data_deletion}, are deleted."
    log_and_print(deleting_marked_data_log_info)
            
    merge_condition, update_columns, insert_columns = _prepare_merge_config(
        data_to_merge=data_to_merge,
        primary_keys=primary_keys,
        source_alias=source_alias,
        target_alias=target_alias
    )

    if not delete_rows_with_value.isdigit():
        delete_rows_with_value = f"'{delete_rows_with_value}'"

    target_table.alias(target_alias).merge(source = data_to_merge.alias(source_alias)
                , condition = merge_condition
            ).whenMatchedUpdate(
                    condition = f"{source_alias}.`{column_to_mark_source_data_deletion}` != {delete_rows_with_value} OR {source_alias}.`{column_to_mark_source_data_deletion}` IS NULL"
                    ,set = update_columns
            ).whenMatchedDelete(
                condition = f"{source_alias}.`{column_to_mark_source_data_deletion}` = {delete_rows_with_value}"
            ).whenNotMatchedInsert(values = insert_columns).execute()

def _handle_merge_mark_unmatched_deleted(
    target_table: DeltaTable,
    data_to_merge: DataFrame,
    primary_keys: list,
    target_alias: str,
    source_alias: str,
    delete_rows_with_value: str,
    column_to_mark_source_data_deletion: str
) -> None:
    """Handle merge with deletion tracking for unmatched target records."""
    merging_mark_unmatched_log_info = "Merging data to existing table and marking target records not found in source dataset as deleted."
    log_and_print(merging_mark_unmatched_log_info)
    
    merge_condition, update_columns, insert_columns = _prepare_merge_config(
        data_to_merge=data_to_merge,
        primary_keys=primary_keys,
        source_alias=source_alias,
        target_alias=target_alias
    )

    if delete_rows_with_value.isdigit():
        delete_rows_with_value = int(delete_rows_with_value)

    data_to_merge = data_to_merge.withColumn(column_to_mark_source_data_deletion, f.lit(None))

    target_table.alias(target_alias) \
                .merge(source = data_to_merge.alias(source_alias)
                    ,condition = merge_condition
                ).whenMatchedUpdate(set = update_columns) \
                .whenNotMatchedInsert(values = insert_columns) \
                .whenNotMatchedBySourceUpdate(set={column_to_mark_source_data_deletion: f.lit(delete_rows_with_value)}) \
                .execute()

def _handle_merge_mark_all_deleted(
    target_table: DeltaTable,
    data_to_merge: DataFrame,
    primary_keys: list,
    target_alias: str,
    source_alias: str,
    delete_rows_with_value: str,
    column_to_mark_source_data_deletion: str
) -> None:
    """Handle merge marking all matched records as deleted (soft delete)."""
    merging_mark_all_deleted_log_info = "Merging data to existing table and marking all matched records as deleted."
    log_and_print(merging_mark_all_deleted_log_info)
    
    if delete_rows_with_value.isdigit():
        delete_rows_with_value = int(delete_rows_with_value)
    
    data_to_merge = data_to_merge.withColumn(column_to_mark_source_data_deletion, f.lit(delete_rows_with_value))

    merge_condition, update_columns, insert_columns = _prepare_merge_config(
        data_to_merge=data_to_merge,
        primary_keys=primary_keys,
        source_alias=source_alias,
        target_alias=target_alias
    )

    target_table.alias(target_alias) \
                .merge(source = data_to_merge.alias(source_alias)
                    ,condition = merge_condition
                ).whenMatchedUpdate(set = update_columns) \
                .whenNotMatchedInsert(values = insert_columns) \
                .execute()

def _handle_replace_where(
    data_to_merge: DataFrame,
    target_abfss_path: str,
    replace_where_column: str
) -> None:
    """Handle selective partition overwrite based on column values."""
    merging_replace_where_log_info = "Merging data to existing table using replaceWhere Spark logic."
    log_and_print(merging_replace_where_log_info)

    # Get column data type
    replace_dtype = [
        dtype for name, dtype in data_to_merge.dtypes 
        if name == replace_where_column
    ][0].lower()

    # Build replace condition based on data type
    if 'timestamp' in replace_dtype or 'date' in replace_dtype:
        # Handle date/timestamp columns
        bounds = data_to_merge.agg(
            f.min(f.col(replace_where_column)).alias("min_v"),
            f.max(f.col(replace_where_column)).alias("max_v")
        ).first()
        min_v, max_v = bounds.min_v, bounds.max_v
        replace_where_expression = (
            f"{replace_where_column} >= '{min_v}' AND {replace_where_column} <= '{max_v}'"
        )
        
    elif any(t in replace_dtype for t in ('int', 'bigint', 'double', 'float', 'decimal')):
        # Handle numeric columns
        bounds = data_to_merge.agg(
            f.min(f.col(replace_where_column)).alias("min_v"),
            f.max(f.col(replace_where_column)).alias("max_v")
        ).first()
        replace_where_expression = (
            f"{replace_where_column} >= {bounds.min_v} AND {replace_where_column} <= {bounds.max_v}"
        )
        
    else:
        # Handle string and other columns
        distinct_values = [
            row[0] for row in data_to_merge.select(replace_where_column).distinct().collect()
        ]
        values_str = ", ".join([f"'{v}'" for v in distinct_values])
        replace_where_expression = f"{replace_where_column} IN ({values_str})"

    data_to_merge.write \
       .mode("overwrite") \
       .option("replaceWhere", replace_where_expression) \
       .save(target_abfss_path)

def get_records_written_for_table(target_abfss_path: str):
    """
    This function gets the number of records written for the last write operation in a delta table.

    Args:
        target_abfss_path (str): abfss path for table

    Returns:
        records_written (int): Records written

    """
    history_df = spark.sql(f"DESCRIBE HISTORY delta.`{target_abfss_path}`") \
                     .filter("operation in ('MERGE', 'UPDATE', 'WRITE', 'CREATE OR REPLACE TABLE AS SELECT', " +
                            "'CREATE TABLE AS SELECT', 'REPLACE TABLE AS SELECT', 'COPY INTO')")

    history_rows = history_df.orderBy(f.desc("timestamp")).select("operationMetrics").collect()
    
    if not history_rows:
        # No matching write operations found in history - this can happen if:
        # 1. Table was just created but no data written yet
        # 2. Custom function performed a non-standard write operation
        # 3. Delta history hasn't been updated yet (rare timing issue)
        log_and_print(
            f"⚠️ Warning: No write operations found in Delta history for {target_abfss_path}. "
            "Returning 0 records. If data was written, verify the write operation completed successfully.",
            "warn"
        )
        return 0
    
    operation_metrics = history_rows[0][0]
    
    if operation_metrics is None:
        log_and_print(
            f"⚠️ Warning: Operation metrics not available for {target_abfss_path}. Returning 0 records.",
            "warn"
        )
        return 0
        
    records_written = int(operation_metrics.get('numOutputRows', 0))

    return records_written

def _get_records_processed(
    merge_type: str,
    target_abfss_path: str,
    data_to_merge: DataFrame
) -> int:
    """Get count of records processed."""
    if merge_type not in ("output_file", "warehouse_spark_connector"):   
        records_processed = get_records_written_for_table(
            target_abfss_path = target_abfss_path
        )
    else:
        records_processed = data_to_merge.count()
    
    return records_processed

# Extracted function to build dynamic filter expressions
def _build_filter_expression_for_batch(
    ingestion_row: object,
    columns: list
) -> str:
    """Build dynamic filter expression for the specified columns and row values."""
    filter_conditions = []
    for col in columns:
        col_value = getattr(ingestion_row, col)
        if isinstance(col_value, str):
            # Handle string columns
            filter_conditions.append(f"{col} = '{col_value}'")
        elif col_value is None:
            # Handle null values
            filter_conditions.append(f"{col} IS NULL")
        elif isinstance(col_value, datetime):
            # Handle datetime columns
            filter_conditions.append(f"{col} = to_timestamp('{str(col_value)}', 'yyyy-MM-dd HH:mm:ss.SSSSSSS')")
        elif isinstance(col_value, date):
            # Handle date columns
            filter_conditions.append(f"{col} = to_date('{str(col_value)}', 'yyyy-MM-dd')")
        else:
            # Handle numeric, boolean, and other types
            filter_conditions.append(f"{col} = {col_value}")
    
    return " AND ".join(filter_conditions)

def merge_data(
    data_to_merge: DataFrame,
    first_run: bool,
    target_config: dict,
    watermark_config: str,
    primary_keys: list,
    dimension_config: dict
) -> tuple[bool, int]:
    """
    Execute data merge operations with multiple strategies for different business scenarios.

    This function is the central hub for all data write operations, supporting various
    merge patterns from simple appends to complex SCD2 implementations. It intelligently
    routes to the appropriate merge strategy based on configuration.

    Args:

    Returns:
        tuple[bool, int]: Updated first_run flag and records processed count

    Merge Types:
        - output_file: Write to file formats (CSV, Parquet, Excel)
        - append: Simple append to existing table
        - overwrite: Full table replacement
        - overwrite_with_all_paths: Overwrite with multi-file support
        - merge: Upsert based on primary keys
        - merge_mark_unmatched_deleted: Upsert + mark missing as deleted
        - merge_mark_all_deleted: Mark all matched records as deleted
        - replace_where: Selective partition overwrite

    Performance Considerations:
        - Uses Delta Lake merge for efficient updates
        - Leverages partition pruning for replace_where
        - Supports batch processing for large datasets
        - Maintains statistics through operation metrics
    """
    merge_type = watermark_config['merge_type']
    target_abfss_path = target_config['target_abfss_path']
    delete_rows_with_value = watermark_config['delete_rows_with_value']
    column_to_mark_source_data_deletion = watermark_config['column_to_mark_source_data_deletion']
    
    # Log target path for debugging and tracking
    target_path_log_info = f"Writing data to path: {target_abfss_path}"
    log_and_print(target_path_log_info)
    
    target_alias = "target_df"
    source_alias = "source_df"    

    # Note: This is safe on first_run because create_delta_table() in NB_Batch_Processing
    # always runs before write_data_orchestrator(), so the table already exists by this point.
    if merge_type not in ("output_file", "warehouse_spark_connector"):
        target_table = DeltaTable.forPath(spark, target_abfss_path)

    if merge_type == "output_file":
        _write_output_file(
            data_to_merge = data_to_merge,
            target_abfss_path = target_abfss_path,
            target_config = target_config
        )

    elif merge_type == "warehouse_spark_connector":
        _write_warehouse_spark_connector(
            data_to_merge = data_to_merge,
            target_datastore_workspace_id = target_config['target_datastore_workspace_id'],
            target_table_name = target_config['target_table_name'],
            warehouse_write_mode = target_config['warehouse_write_mode']
        )
                       
    elif first_run:
        first_run = _handle_first_run(
            data_to_merge = data_to_merge,
            target_abfss_path = target_abfss_path
        )

    elif dimension_config['enable_scd2_dimension']:
        scd_dimensions(
            new_data = data_to_merge,
            target_abfss_path = target_abfss_path,
            primary_keys = primary_keys,
            source_timestamp_column_name = dimension_config['source_timestamp_column_name'],
            column_to_mark_source_data_deletion = column_to_mark_source_data_deletion,
            delete_rows_with_value = delete_rows_with_value,
            dimension_table_key_column_name = dimension_config['dimension_table_key_column_name'],
            watermark_column_name = watermark_config['watermark_column_name']
        )

    elif merge_type == "append":
        _handle_append(
            data_to_merge = data_to_merge,
            target_abfss_path = target_abfss_path
        )

    elif merge_type == "overwrite":
        _handle_overwrite(
            data_to_merge = data_to_merge,
            target_abfss_path = target_abfss_path
        )
        
    elif merge_type == "merge":
        _handle_merge(
            target_table = target_table,
            data_to_merge = data_to_merge,
            primary_keys = primary_keys,
            target_alias = target_alias,
            source_alias = source_alias
        )

    elif merge_type == "merge_and_delete":
        _handle_merge_and_delete(
            target_table = target_table,
            data_to_merge = data_to_merge,
            primary_keys = primary_keys,
            target_alias = target_alias,
            source_alias = source_alias,
            delete_rows_with_value = delete_rows_with_value,
            column_to_mark_source_data_deletion = column_to_mark_source_data_deletion
        )

    elif merge_type == "merge_mark_unmatched_deleted":
        _handle_merge_mark_unmatched_deleted(
            target_table = target_table,
            data_to_merge = data_to_merge,
            primary_keys = primary_keys,
            target_alias = target_alias,
            source_alias = source_alias,
            delete_rows_with_value = delete_rows_with_value,
            column_to_mark_source_data_deletion = column_to_mark_source_data_deletion
        )

    elif merge_type == "merge_mark_all_deleted":
        _handle_merge_mark_all_deleted(
            target_table = target_table,
            data_to_merge = data_to_merge,
            primary_keys = primary_keys,
            target_alias = target_alias,
            source_alias = source_alias,
            delete_rows_with_value = delete_rows_with_value,
            column_to_mark_source_data_deletion = column_to_mark_source_data_deletion
        )

    elif merge_type == "replace_where":
        _handle_replace_where(
            data_to_merge = data_to_merge,
            target_abfss_path = target_abfss_path,
            replace_where_column = target_config['replace_where_column']
        )
            
    else:
        raise Exception(f"No logic currently exists to execute the {merge_type} merge type.")

    records_processed = _get_records_processed(
        merge_type = merge_type,
        target_abfss_path = target_abfss_path,
        data_to_merge = data_to_merge
    )

    return first_run, records_processed

def get_unique_batch_metadata(df, batch_columns: list):
    """
    Extract unique batch values from DataFrame for batch processing.
    
    This function identifies all unique combinations of values in the specified
    batch columns, which are used to partition the data for sequential processing.
    This is helpful when recreating tables from prior data loads or when processing
    large datasets in smaller, manageable chunks.
    
    Args:
        df: Spark DataFrame containing the data to process
        batch_columns (list): List of column names to use for batching
    
    Returns:
        list: List of Row objects containing unique batch value combinations,
              sorted in ascending order by the batch columns
    
    Example:
        >>> batch_metadata = get_unique_batch_metadata(
        ...     df = new_data,
        ...     batch_columns = ['delta__raw_folderpath', 'source_file_name']
        ... )
        >>> print(f"Processing {len(batch_metadata)} batches")
    """
    ingestion_metadata = df.dropDuplicates(batch_columns) \
                          .select(batch_columns) \
                          .sort(*[f.asc(col) for col in batch_columns]) \
                          .collect()
    
    return ingestion_metadata


def process_single_batch(
    df: DataFrame,
    batch_row: Row,
    merge_in_batches_with_columns: list,
    first_run: bool,
    target_config: dict,
    watermark_config: dict,
    primary_keys: list,
    dimension_config: dict
) -> tuple:
    """
    Process a single batch of data from the source DataFrame.
    
    This function filters the source data for a specific batch, logs the operation,
    and merges the filtered data to the target location. Each batch is processed
    sequentially to maintain data lineage and enable recovery from specific batches.
    
    Args:
        df: Spark DataFrame containing the data to process
        batch_row: Row object containing the batch column values
        merge_in_batches_with_columns (list): List of column names used for batching
        first_run (bool): Whether this is the first load for the table
        target_config (dict): Target configuration dictionary
        watermark_config (dict): Watermark configuration dictionary
        primary_keys (list): List of primary key column names
        dimension_config (dict): Dimension configuration dictionary

    Returns:
        tuple: (updated_first_run, records_processed)
            - updated_first_run (bool): Updated first_run flag after merge
            - records_processed (int): Number of records processed in this batch
    
    """
    # Build filter expression for current batch
    filter_expression = _build_filter_expression_for_batch(
        ingestion_row = batch_row,
        columns = merge_in_batches_with_columns
    )
    
    log_and_print(f"Writing batch with filter: {filter_expression}")
    
    # Filter data for current batch
    data_to_merge = df.filter(filter_expression)
    
    # Merge current batch to target
    updated_first_run, records_processed = merge_data(
        data_to_merge = data_to_merge,
        first_run = first_run,
        target_config = target_config,
        watermark_config = watermark_config,
        primary_keys = primary_keys,
        dimension_config = dimension_config
    )
    
    return updated_first_run, records_processed

def write_data_orchestrator(
    df: DataFrame,
    first_run: bool,
    target_config: dict,
    watermark_config: dict,
    primary_keys: list,
    dimension_config: dict
) -> int:
    """
    Process all data in a single merge operation.
    
    This is the standard processing mode where all source data is merged to the
    target in one operation. This provides optimal performance for most use cases.
    
    Args:
    
    Returns:
        int: Number of records processed

    """
    merge_in_batches_with_columns = watermark_config['merge_in_batches_with_columns']

    if merge_in_batches_with_columns:
        log_and_print(f"Writing data in batches, grouped by distinct values in columns: {merge_in_batches_with_columns}.")
        
        # Get unique batch combinations
        batch_metadata = get_unique_batch_metadata(
            df = df,
            batch_columns = merge_in_batches_with_columns
        )
        
        total_records_processed = 0
        current_first_run = first_run
        
        # Process each batch sequentially
        for batch_row in batch_metadata:
            current_first_run, batch_records = process_single_batch(
                df = df,
                merge_in_batches_with_columns = merge_in_batches_with_columns,
                batch_row = batch_row,
                first_run = current_first_run,
                target_config = target_config,
                watermark_config = watermark_config,
                primary_keys = primary_keys,
                dimension_config = dimension_config
            )
            total_records_processed += batch_records
    else:
        first_run, total_records_processed = merge_data(
            data_to_merge = df,
            first_run = first_run,
            target_config = target_config,
            watermark_config = watermark_config,
            primary_keys = primary_keys,
            dimension_config = dimension_config
        )
    
    return total_records_processed

def write_quarantined_data(
    quarantined_df,
    target_quarantined_abfss_path: str
) -> int:
    """
    Write quarantined data to the quarantine table and return count of records.
    
    This function handles the complete quarantine workflow: checking for data,
    writing to the quarantine table (append mode), and retrieving the count
    of records written from table history.
    
    Args:
        quarantined_df: Spark DataFrame containing quarantined records
        target_quarantined_abfss_path (str): abfss path for quarantined table
    
    Returns:
        int: Number of records written to quarantine table (0 if no data)
    
    Example:
        >>> quarantined_count = write_quarantine_data(
        ...     quarantined_df = failed_quality_checks,
        ...     target_quarantined_abfss_path = ''
        ... )
        >>> print(f"Quarantined {quarantined_count} records")
    
    Note:
        - Always uses append mode to preserve historical quarantine records
        - Quarantine tables store records that failed validation including:
          * Primary key duplicates
          * Data quality rule violations
          * Schema validation failures
          * Reference data mismatches
    """
    if not quarantined_df.isEmpty():
        log_and_print("Writing quarantined data to table.")
        
        # Create or append to quarantine table
        quarantined_df.write.mode("append") \
                      .save(target_quarantined_abfss_path)
        
        # Get quarantine metrics from table history
        quarantined_records = get_records_written_for_table(
            target_abfss_path = target_quarantined_abfss_path
        )
    else:
        quarantined_records = 0
    
    return quarantined_records

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Slowly Changing Dimensions (SCD) Type 2 Implementation
# 
# This section implements Type 2 Slowly Changing Dimensions (SCD2), a critical pattern for tracking historical changes in dimension data. SCD2 preserves the complete history of changes by creating new records for each change while maintaining the old records with validity date ranges.
# 
# Key features:
# - **Historical Tracking**: Maintains full audit trail of all dimension changes
# - **Temporal Queries**: Enables point-in-time analysis with start/end dates
# - **Performance Optimization**: Uses hash-based change detection for efficiency
# - **Data Integrity**: Ensures no gaps or overlaps in temporal coverage

# CELL ********************

def _scd2_prepare_existing_data(target_abfss_path: str) -> DataFrame:
    """Load existing dimension data and filter for active records."""
    existing_df = spark.read.format("delta").load(target_abfss_path)
    existing_df_active = existing_df.filter(f.col("scd_active") == 1).alias('existing_df_active')
    return existing_df_active


def _prepare_comparison_columns(
    new_data: DataFrame, 
    existing_df_active: DataFrame, 
    dimension_table_key_column_name: str, 
    column_to_mark_source_data_deletion: str,
    source_timestamp_column_name: str,
    watermark_column_name: str
) -> Tuple[List[str], List[str]]:
    """Identify columns for comparison (exclude system columns, scd2 timestamp, and watermark column)."""
    
    # Build set of columns to exclude from comparison
    columns_to_exclude = {
        dimension_table_key_column_name,
        source_timestamp_column_name,
        watermark_column_name
    }
    
    # Filter new data columns (exclude specified columns from comparison)
    new_data_non_delta_columns = sorted([
        col
        for col in new_data.columns
        if (col not in columns_to_exclude) and 
           ("DELTA__" not in col.upper() or col.upper() == column_to_mark_source_data_deletion.upper()) and 
           ("SCD_" not in col.upper())
    ])

    # Filter existing table columns (exclude specified columns from comparison)
    existing_table_non_delta_columns = sorted([
        col
        for col in existing_df_active.columns
        if (col not in columns_to_exclude) and 
           ("DELTA__" not in col.upper() or col.upper() == column_to_mark_source_data_deletion.upper()) and 
           ("SCD" not in col.upper())
    ])
    
    return new_data_non_delta_columns, existing_table_non_delta_columns


def _calculate_hash_comparison(
    new_data: DataFrame, 
    existing_df_active: DataFrame, 
    primary_keys: List[str], 
    new_data_non_delta_columns: List[str], 
    existing_table_non_delta_columns: List[str]
) -> Tuple[DataFrame, DataFrame]:
    """Calculate hash values for efficient comparison."""
    
    # Calculate hash for new data
    new_data_hash = new_data.select(
        *primary_keys, 
        f.hash(*new_data_non_delta_columns).alias("new_hash")
    )
    
    # Calculate hash for existing data
    existing_df_hash = existing_df_active.select(
        *primary_keys, 
        f.hash(*existing_table_non_delta_columns).alias("existing_hash")
    )

    # Join and compare hashes
    hash_comparison = new_data_hash.join(
        existing_df_hash, primary_keys, how="left_outer"
    ).select(new_data_hash["*"], existing_df_hash["existing_hash"])
    
    # Identify changed records
    changed_dimension_hash = hash_comparison.filter(
        f.col("new_hash") != f.col("existing_hash")
    )
    
    # Identify new records
    new_dimension_hash = hash_comparison.filter(f.col("existing_hash").isNull())
    
    return changed_dimension_hash, new_dimension_hash


def _get_changed_dimensions_values(
    new_data: DataFrame, 
    existing_df_active: DataFrame, 
    changed_dimension_hash: DataFrame, 
    primary_keys: List[str]
) -> DataFrame:
    """Get full data for changed records."""
    
    # Get distinct primary keys for changed records
    changed_dimension_hashes_distinct_pks = changed_dimension_hash.select(
        *primary_keys
    ).distinct()
    
    # Join to get full new data for changed records
    changed_dimensions_values = new_data.join(
        changed_dimension_hashes_distinct_pks, primary_keys, how="inner"
    ).select(new_data["*"])
    
    # Add existing start date
    changed_dimensions_values = changed_dimensions_values.join(
        existing_df_active, primary_keys, how="left_outer"
    ).select(changed_dimensions_values["*"], "scd_start_date")
    
    print("changed_dimensions_values:")
    display(changed_dimensions_values)
    
    return changed_dimensions_values


def _get_new_dimensions_values(
    new_data: DataFrame, 
    new_dimension_hash: DataFrame, 
    primary_keys: List[str]
) -> DataFrame:
    """Get full data for new records."""
    
    # Get distinct primary keys for new records
    new_dimension_hashes_distinct_pks = new_dimension_hash.select(
        *primary_keys
    ).distinct()
    
    # Join to get full new data for new records
    new_dimensions_values = new_data.join(
        new_dimension_hashes_distinct_pks, primary_keys, how="inner"
    ).select(new_data["*"])
    
    print("new_dimensions_values")
    display(new_dimensions_values)
    
    return new_dimensions_values

def _process_deleted_records(
    target_table: DeltaTable, 
    new_dimensions_values: DataFrame, 
    changed_dimensions_values: DataFrame, 
    column_to_mark_source_data_deletion: Optional[str], 
    delete_rows_with_value: str, 
    source_timestamp_column_name: str, 
    primary_keys: List[str]
) -> None:
    """Process deleted records."""
    
    # Create primary key join condition
    primary_key_join = " AND ".join(
        [f"current.`{col}` <=> new.`{col}`" for col in primary_keys]
    )
    
    if column_to_mark_source_data_deletion:
        # Filter new deleted records
        new_deleted_values = new_dimensions_values.where(
            f"`{column_to_mark_source_data_deletion}` = {delete_rows_with_value}"
        )

        new_deleted_values = new_deleted_values.withColumn(
            "scd_start_date", f.col(source_timestamp_column_name)
        )
        
        # Log deletion operation
        marking_records_inactive_log_info = (
            f"Marking all records as inactive where "
            f"`{column_to_mark_source_data_deletion}` = {delete_rows_with_value}"
        )
        log_and_print(marking_records_inactive_log_info)
        
        # Filter changed deleted records
        deleted_dimension_values = changed_dimensions_values.where(
            f"`{column_to_mark_source_data_deletion}` = {delete_rows_with_value}"
        )
        
        # Combine all deleted records
        deleted_dimension_values = new_deleted_values.unionByName(
            deleted_dimension_values, allowMissingColumns=True
        )
        
        # Mark as inactive with end date
        deleted_dimension_values = deleted_dimension_values.withColumn(
            "scd_end_date", f.col(source_timestamp_column_name)
        ).withColumn(
            "scd_active", f.lit(0)
        )
        
        print("deleted_dimension_values:")
        display(deleted_dimension_values)

        # Create join condition for active records
        primary_key_join_deleted = primary_key_join + " AND current.scd_active = 1"

        # Execute merge for deleted records
        target_table.alias("current").merge(
            deleted_dimension_values.alias("new"), primary_key_join_deleted
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

def _process_modified_records(
    changed_dimensions_values: DataFrame, 
    existing_df_active: DataFrame, 
    column_to_mark_source_data_deletion: Optional[str], 
    delete_rows_with_value: str, 
    source_timestamp_column_name: str, 
    primary_keys: List[str]
) -> DataFrame:
    """Process non-deleted changed records."""
    
    # Filter out deleted records from changed records
    if column_to_mark_source_data_deletion:
        modified_dimension_values = changed_dimensions_values.where(
            f"{column_to_mark_source_data_deletion} != {delete_rows_with_value} OR "
            f"{column_to_mark_source_data_deletion} IS NULL"
        )
    else:
        modified_dimension_values = changed_dimensions_values

    # Join with existing data to get current record details
    modified_dimension_values = (
        modified_dimension_values.alias("A")
        .join(
            existing_df_active.alias("B")
            .drop(source_timestamp_column_name)
            .drop("scd_end_date"),
            primary_keys,
            how="inner",
        )
        .select(f.col("B.*"), f.col(f"A.{source_timestamp_column_name}"))
    )
    
    # Mark existing records as inactive
    new_dimensions_values_set_inactive = modified_dimension_values.withColumn(
        "scd_end_date", f.col(source_timestamp_column_name)
    ).withColumn(
        "scd_active", f.lit(0)
    )
    
    print("new_dimensions_values_set_inactive:")
    display(new_dimensions_values_set_inactive)
    
    return new_dimensions_values_set_inactive

def _create_new_versions(
    changed_dimensions_values: DataFrame, 
    column_to_mark_source_data_deletion: Optional[str], 
    delete_rows_with_value: str, 
    source_timestamp_column_name: str
) -> DataFrame:
    """Insert new versions for modified records."""
    
    # Filter out deleted records from changed records
    if column_to_mark_source_data_deletion:
        modified_dimension_values = changed_dimensions_values.where(
            f"{column_to_mark_source_data_deletion} != {delete_rows_with_value} OR "
            f"{column_to_mark_source_data_deletion} IS NULL"
        )
    else:
        modified_dimension_values = changed_dimensions_values
    
    # Create new active versions with current timestamp
    new_dimensions_values_insert = modified_dimension_values.withColumn(
        "scd_start_date", f.col(source_timestamp_column_name)
    ).withColumn(
        "scd_end_date", f.lit(None)
    ).withColumn(
        "scd_active", f.lit(1)
    )
    
    print("new_dimensions_values_insert")
    display(new_dimensions_values_insert)
    
    return new_dimensions_values_insert

def _prepare_new_records(
    new_dimensions_values: DataFrame, 
    column_to_mark_source_data_deletion: Optional[str], 
    delete_rows_with_value: str, 
    source_timestamp_column_name: str
) -> DataFrame:
    """Insert completely new records."""
    
    # Filter out deleted records from new records
    if column_to_mark_source_data_deletion:
        new_dimensions_values = new_dimensions_values.where(
            f"{column_to_mark_source_data_deletion} != {delete_rows_with_value} OR "
            f"{column_to_mark_source_data_deletion} IS NULL"
        )

    # Set SCD columns for new records
    new_dimensions_values = new_dimensions_values.withColumn(
        "scd_start_date", f.col(source_timestamp_column_name)
    ).withColumn(
        "scd_end_date", f.lit(None)
    ).withColumn(
        "scd_active", f.lit(1)
    )
    
    print("new_dimensions_values")
    display(new_dimensions_values)
    
    return new_dimensions_values

def _execute_final_merge(
    target_table: DeltaTable, 
    new_dimensions_values_set_inactive: DataFrame, 
    new_dimensions_values_insert: DataFrame, 
    new_dimensions_values: DataFrame, 
    primary_keys: List[str]
) -> None:
    """Combine all updates and execute final merge."""
    
    # Combine all dimension updates
    all_updated_dimension_values = new_dimensions_values_set_inactive.unionByName(
        new_dimensions_values_insert, allowMissingColumns=True
    ).unionByName(
        new_dimensions_values, allowMissingColumns=True
    )
    
    print("all:")
    display(all_updated_dimension_values)
    
    # Create join conditions
    primary_key_join = " AND ".join(
        [f"current.`{col}` <=> new.`{col}`" for col in primary_keys]
    )
    primary_key_join_updates = (
        primary_key_join + " AND current.scd_active = 1 AND new.scd_active = 0"
    )

    # Execute final merge operation
    target_table.alias("current").merge(
        all_updated_dimension_values.alias("new"), primary_key_join_updates
    ).whenMatchedUpdate(
        set={"scd_active": "new.scd_active", "scd_end_date": "new.scd_end_date"}
    ).whenNotMatchedInsertAll().execute()


def scd_dimensions(
    new_data: DataFrame,
    target_abfss_path: str,
    primary_keys: List[str],
    source_timestamp_column_name: str,
    column_to_mark_source_data_deletion: Optional[str],
    delete_rows_with_value: str,
    dimension_table_key_column_name: str,
    watermark_column_name: str
) -> None:
    """
    Implement Type 2 Slowly Changing Dimensions with comprehensive change tracking.

    This function manages the complete SCD2 lifecycle including:
    - Detection of new, changed, and deleted dimension members
    - Creation of new versions for changed records
    - Closure of previous versions with appropriate end dates
    - Preservation of surrogate keys across versions

    The implementation uses hash-based comparison for efficient change detection
    and maintains referential integrity for fact table relationships.

    Args:
        new_data (DataFrame): Incoming dimension data with potential changes
        target_abfss_path (str): ABFSS Path of target table
        primary_keys (List[str]): Natural/business key columns
        source_timestamp_column_name (str): Timestamp column for SCD2 versioning (scd_start_date/scd_end_date)
        column_to_mark_source_data_deletion (Optional[str]): Column indicating source deletions
        delete_rows_with_value (str): value that indicates a record should be deleted
        dimension_table_key_column_name (str): Surrogate key column name
        watermark_column_name (str): Watermark column to exclude from hash comparison (can be empty string if not configured)

    Processing Steps:
        1. Load existing active dimension records
        2. Calculate hash values for change detection
        3. Identify new, changed, and deleted records
        4. Close out changed/deleted records (set end date)
        5. Insert new versions for changed records
        6. Insert completely new records

    Performance Optimizations:
        - Hash-based comparison reduces column-by-column checks
        - Processes only active records for comparison
        - Bulk operations for all record types
        - Minimizes table scans with targeted filters

    Data Integrity:
        - Ensures no gaps in temporal coverage
        - Maintains consistent surrogate keys
        - Handles edge cases (deletes, resurrections)
    """

    # Log the SCD2 operation
    merging_scd2_dimensions_log_info = "Merging dimension data to existing table with slowly changing type 2 format."
    log_and_print(merging_scd2_dimensions_log_info)

    # Initialize target table
    target_table = DeltaTable.forPath(spark, target_abfss_path)
    
    # Step 1: Load existing active dimension records
    existing_df_active = _scd2_prepare_existing_data(
        target_abfss_path=target_abfss_path
    )

    # Format delete value if needed
    if column_to_mark_source_data_deletion:
        if not delete_rows_with_value.isdigit():
            delete_rows_with_value = f"'{delete_rows_with_value}'"

    # Step 2: Prepare columns for comparison
    new_data_non_delta_columns, existing_table_non_delta_columns = _prepare_comparison_columns(
        new_data=new_data,
        existing_df_active=existing_df_active,
        dimension_table_key_column_name=dimension_table_key_column_name,
        column_to_mark_source_data_deletion=column_to_mark_source_data_deletion,
        source_timestamp_column_name=source_timestamp_column_name,
        watermark_column_name=watermark_column_name
    )
    
    # Step 3: Calculate hash comparison to identify changes
    changed_dimension_hash, new_dimension_hash = _calculate_hash_comparison(
        new_data=new_data,
        existing_df_active=existing_df_active,
        primary_keys=primary_keys,
        new_data_non_delta_columns=new_data_non_delta_columns,
        existing_table_non_delta_columns=existing_table_non_delta_columns
    )
    
    # Step 4: Get full data for changed records
    changed_dimensions_values = _get_changed_dimensions_values(
        new_data=new_data,
        existing_df_active=existing_df_active,
        changed_dimension_hash=changed_dimension_hash,
        primary_keys=primary_keys
    )
    
    # Step 5: Get full data for new records
    new_dimensions_values = _get_new_dimensions_values(
        new_data=new_data,
        new_dimension_hash=new_dimension_hash,
        primary_keys=primary_keys
    )

    # Step 6: Process deleted records
    _process_deleted_records(
        target_table=target_table,
        new_dimensions_values=new_dimensions_values,
        changed_dimensions_values=changed_dimensions_values,
        column_to_mark_source_data_deletion=column_to_mark_source_data_deletion,
        delete_rows_with_value=delete_rows_with_value,
        source_timestamp_column_name=source_timestamp_column_name,
        primary_keys=primary_keys
    )

    # Step 7: Process modified records (mark existing as inactive)
    new_dimensions_values_set_inactive = _process_modified_records(
        changed_dimensions_values=changed_dimensions_values,
        existing_df_active=existing_df_active,
        column_to_mark_source_data_deletion=column_to_mark_source_data_deletion,
        delete_rows_with_value=delete_rows_with_value,
        source_timestamp_column_name=source_timestamp_column_name,
        primary_keys=primary_keys
    )

    # Step 8: Create new versions for changed records
    new_dimensions_values_insert = _create_new_versions(
        changed_dimensions_values=changed_dimensions_values,
        column_to_mark_source_data_deletion=column_to_mark_source_data_deletion,
        delete_rows_with_value=delete_rows_with_value,
        source_timestamp_column_name=source_timestamp_column_name
    )

    # Step 9: Prepare completely new records
    new_dimensions_values = _prepare_new_records(
        new_dimensions_values=new_dimensions_values,
        column_to_mark_source_data_deletion=column_to_mark_source_data_deletion,
        delete_rows_with_value=delete_rows_with_value,
        source_timestamp_column_name=source_timestamp_column_name
    )

    # Step 10: Execute final merge operation
    _execute_final_merge(
        target_table=target_table,
        new_dimensions_values_set_inactive=new_dimensions_values_set_inactive,
        new_dimensions_values_insert=new_dimensions_values_insert,
        new_dimensions_values=new_dimensions_values,
        primary_keys=primary_keys
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5b. Schema Contract Validation for Delta Tables
# 
# This section provides functions for validating Delta table schemas against expected schema contracts.
# When a schema is defined in `source_details.schema`, the framework validates that the source table
# matches the expected columns and types before processing continues.
# 
# Key capabilities:
# - **Schema Contract Enforcement**: Ensures source tables have required columns with correct types
# - **Forward Compatibility**: Extra columns in source are allowed by default
# - **Clear Error Messages**: Provides detailed information about schema mismatches

# CELL ********************

def _parse_expected_schema(expected_schema_str: str) -> dict:
    """
    Parse an expected schema string into a dictionary of column names and types.
    
    Handles complex types with commas inside parentheses like DECIMAL(10,2).
    
    Args:
        expected_schema_str: Schema definition string like 'name STRING, age INT, amount DECIMAL(10,2)'
    
    Returns:
        dict: Dictionary mapping column names (lowercase) to data types (lowercase)
        
    Example:
        >>> _parse_expected_schema('name STRING, age INT')
        {'name': 'string', 'age': 'int'}
        >>> _parse_expected_schema('id INT, amount DECIMAL(10,2), name STRING')
        {'id': 'int', 'amount': 'decimal(10,2)', 'name': 'string'}
    """
    if not expected_schema_str:
        return {}
    
    # Match: column_name TYPE or column_name TYPE(params)
    # Pattern captures: (column_name) (TYPE_NAME with optional parentheses)
    pattern = r'(\w+)\s+(\w+(?:\([^)]*\))?)'
    matches = re.findall(pattern, expected_schema_str)
    
    return {name.lower(): type_def.lower() for name, type_def in matches}


def _normalize_spark_type(spark_type: str) -> str:
    """
    Normalize Spark data type names to standard lowercase forms.
    
    Handles variations like 'StringType()', 'string', 'STRING' -> 'string'
    
    Args:
        spark_type: Spark data type as string
        
    Returns:
        str: Normalized type name in lowercase
    """
    type_str = str(spark_type).lower()
    
    # Remove 'type()' suffix if present (e.g., 'StringType()' -> 'string')
    if 'type' in type_str:
        type_str = type_str.replace('type()', '').replace('type', '')
    
    # Handle common type aliases
    type_mappings = {
        'bigint': 'long',
        'int': 'integer',
        'short': 'smallint',
        'byte': 'tinyint',
        'real': 'float',
        'text': 'string',
        'bool': 'boolean',
    }
    
    return type_mappings.get(type_str, type_str)


def validate_table_schema_contract(
    df: "DataFrame",
    expected_schema: str,
    source_description: str
) -> None:
    """
    Validate that a DataFrame's schema matches the expected schema contract.
    
    This function enforces schema contracts for Delta table sources. It verifies that:
    1. All expected columns exist in the source
    2. Column data types match the expected types
    
    Extra columns in the source are allowed (forward compatible).
    
    Args:
        df: The DataFrame to validate
        expected_schema: Expected schema string like 'name STRING, age INT, city STRING'
        source_description: Description of the source for error messages
        
    Raises:
        Exception: If schema validation fails with detailed mismatch information
        
    Example:
        >>> validate_table_schema_contract(
        ...     df=spark.sql("SELECT * FROM bronze.dbo.customers"),
        ...     expected_schema='customer_id INT, name STRING, email STRING',
        ...     source_description='bronze.dbo.customers'
        ... )
    """
    if not expected_schema:
        return  # No schema contract defined, skip validation
    
    # Parse expected schema
    expected_cols = _parse_expected_schema(expected_schema)
    
    if not expected_cols:
        return  # Empty schema, skip validation
    
    # Get actual schema from DataFrame
    actual_schema = {field.name.lower(): _normalize_spark_type(str(field.dataType)) 
                     for field in df.schema.fields}
    
    # Track validation errors
    missing_columns = []
    type_mismatches = []
    
    for expected_col, expected_type in expected_cols.items():
        normalized_expected_type = _normalize_spark_type(expected_type)
        
        if expected_col not in actual_schema:
            missing_columns.append(expected_col)
        elif actual_schema[expected_col] != normalized_expected_type:
            type_mismatches.append({
                'column': expected_col,
                'expected': normalized_expected_type,
                'actual': actual_schema[expected_col]
            })
    
    # Build error message if validation failed
    if missing_columns or type_mismatches:
        error_parts = [f"Schema contract validation failed for source: {source_description}"]
        
        if missing_columns:
            error_parts.append(f"\nMissing columns: {', '.join(missing_columns)}")
        
        if type_mismatches:
            mismatch_details = [f"{m['column']} (expected: {m['expected']}, actual: {m['actual']})" 
                               for m in type_mismatches]
            error_parts.append(f"\nType mismatches: {'; '.join(mismatch_details)}")
        
        error_parts.append(f"\nExpected schema: {expected_schema}")
        error_parts.append(f"\nActual columns: {', '.join(sorted(actual_schema.keys()))}")
        
        raise Exception(''.join(error_parts))
    
    log_and_print(f"Schema contract validated successfully for {source_description}. "
                  f"Verified {len(expected_cols)} expected columns.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Ingestion from Fabric Table Sources
# 
# This section provides functions for ingesting data from existing Delta tables within the Fabric environment. These functions support incremental loading patterns through watermark-based filtering and handle various data types and query complexities.
# 
# Key capabilities:
# - **Incremental Data Loading**: Efficiently processes only new or changed records using watermark columns
# - **Flexibility**: Handles complex ingestion functions including joins, CTEs, and aggregations
# - **Type-Safe Watermarking**: Supports both datetime and numeric watermark columns with proper filtering
# - **Empty Result Handling**: Gracefully exits when no new data is available

# CELL ********************

def route_to_ingestion_method(
    source_config: dict,
    watermark_config: dict,
    custom_paths: dict,
    file_config: dict,
    all_metadata: dict,
    folder_path_from_trigger: str,
    first_run: bool,
    lineage_info: dict
) -> tuple:
    """
    Route to appropriate ingestion method based on source configuration.
    
    This function determines whether to ingest data from files or from a Fabric table
    and calls the appropriate ingestion function accordingly.
    
    Args:
        source_config (dict): Source configuration settings
        watermark_config (dict): Watermark tracking configuration
        custom_paths (dict): Custom path configurations
        file_config (dict): Configs for file data
        all_metadata (dict): Complete metadata dictionary
        folder_path_from_trigger (str, optional): Folder path provided by trigger
        first_run (bool, optional): Flag indicating first run
        lineage_info (dict): Lineage tracking information for logging
    
    Returns:
        tuple: (new_data, new_watermark_value, source_details)
            - new_data (DataFrame): Ingested data
            - new_watermark_value: New watermark value after ingestion
            - source_details (str): Description of the data source
    
    Notes:
        - File ingestion uses ingest_raw_files() for Bronze_Files or shortcuts
        - Table ingestion uses ingest_from_fabric_table() for Fabric tables
        - Source details format differs between methods
    """
    if source_config['using_source_folder_path']:
        # Ingest from files (Bronze_Files or shortcuts)
        source_details = f"{custom_paths['source_files_lakehouse_path']}/{source_config['source_path']}" 
        new_data, new_watermark_value = ingest_raw_files(
            source_config = source_config,
            watermark_config = watermark_config,
            file_config = file_config,
            custom_paths = custom_paths,
            all_metadata = all_metadata,
            folder_path_from_trigger = folder_path_from_trigger,
            first_run = first_run,
            lineage_info = lineage_info
        )
    else:
        # Ingest from Fabric table
        new_data, new_watermark_value, source_details = ingest_from_fabric_table(
            source_config = source_config,
            watermark_config = watermark_config,
            all_metadata = all_metadata,
            first_run = first_run,
            lineage_info = lineage_info
        )
    
        # Validate schema contract if expected schema is defined
        expected_schema = source_config.get('expected_schema', '')
        if expected_schema:
            validate_table_schema_contract(
                df = new_data,
                expected_schema = expected_schema,
                source_description = source_details
            )
        # Note: exit_gracefully_no_data() is called inside ingest_from_fabric_table() when no data found
        
    return new_data, new_watermark_value, source_details

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def _extract_new_watermark_value(
    watermark_table_name: str, 
    watermark_column_name: str
) -> str:
    """Extract new watermark value from table, supporting multiple watermark columns.
    
    Args:
        watermark_table_name (str): The table name to extract watermark from
        watermark_column_name (str): Single column name or comma-separated column names
        
    Returns:
        str: The maximum watermark value across all specified columns
    """    
    # Handle multiple watermark columns separated by commas
    if ',' in watermark_column_name:
        # Split and clean column names
        watermark_columns = [col.strip() for col in watermark_column_name.split(',')]
        
        # Use greatest function to find overall maximum across all columns
        overall_max = (
            spark.sql(f"SELECT * FROM {watermark_table_name}")
                 .agg(f.greatest(*[f.max(c) for c in watermark_columns]).alias("overall_max"))
                 .first()["overall_max"]
        )
        return str(overall_max)
    else:
        # Single column - use existing logic
        return str(spark.sql(f"SELECT MAX(`{watermark_column_name}`) as new_watermark_value FROM {watermark_table_name}").first()['new_watermark_value'])

def _construct_watermark_filter(
    watermark_column_data_type: str, 
    watermark_column_name: str, 
    watermark_value: str
) -> str:
    """Construct watermark filter based on data type and value."""
    if not watermark_value:
        # No watermark = initial/full load
        return ""
    
    # Handle multiple watermark columns separated by commas
    if ',' in watermark_column_name:
        watermark_columns = [col.strip() for col in watermark_column_name.split(',')]
        
        if watermark_column_data_type == 'datetime':
            # Create filter for each column and combine with AND
            column_filters = [f"{col} > TIMESTAMP '{watermark_value}'" for col in watermark_columns]
            return f"({' OR '.join(column_filters)})"
        elif watermark_column_data_type == 'numeric':
            # Create filter for each column and combine with AND
            column_filters = [f"{col} > {watermark_value}" for col in watermark_columns]
            return f"({' OR '.join(column_filters)})"
        else:
            # Fail fast on unsupported data types
            raise Exception(f"No logic currently exists for the {watermark_column_data_type} watermark column type. " +
                        "Supported types are: 'datetime', 'numeric'")
    else:
        # Single column - use existing logic
        if watermark_column_data_type == 'datetime':
            # datetime comparison using TIMESTAMP literal
            return f"{watermark_column_name} > TIMESTAMP '{watermark_value}'"
        elif watermark_column_data_type == 'numeric':
            # numeric comparison without quotes
            return f"{watermark_column_name} > {watermark_value}"
        else:
            # Fail fast on unsupported data types
            raise Exception(f"No logic currently exists for the {watermark_column_data_type} watermark column type. " +
                        "Supported types are: 'datetime', 'numeric'")

def _extract_and_validate_watermark(
    watermark_table_name: str, 
    watermark_column_name: str
) -> str:
    """Extract and validate new watermark value from table."""
    retrieving_watermark_log_info = f"Retrieving watermark value for next run from table, {watermark_table_name}."
    log_and_print(retrieving_watermark_log_info)

    columns = spark.sql(f"SELECT * FROM {watermark_table_name}").columns
    
    # Check if all watermark columns exist in the source table (case-insensitive, matching Spark behavior)
    watermark_columns = [col.strip() for col in watermark_column_name.split(',')]
    columns_lower = [c.lower() for c in columns]
    missing_columns = [col for col in watermark_columns if col.lower() not in columns_lower]
    if missing_columns:
        raise Exception(f"The following watermark columns do not exist in the source table: {missing_columns}. Available columns are: {columns}. To resolve, update your metadata by inputting valid watermark column names in watermark_details_column_name or set watermark_details_use_watermark_column to false.")
    
    new_watermark_value = _extract_new_watermark_value(
        watermark_table_name = watermark_table_name, 
        watermark_column_name = watermark_column_name
    )

    new_watermark_value_log_info = f"Watermark value for next run: {new_watermark_value}."
    log_and_print(new_watermark_value_log_info)
    
    return new_watermark_value

def _ingest_with_custom_table_function(
    custom_table_ingestion_function: str, 
    custom_table_ingestion_function_notebook: str, 
    all_metadata: dict, 
    watermark_table_name: str,
    watermark_column_name: str,
    watermark_column_data_type: str, 
    watermark_value: str
):
    """Handle ingestion using custom table/SQL function."""

    # Extract watermark value from the watermark table for next run
    if watermark_value:
        new_watermark_value = _extract_and_validate_watermark(
            watermark_table_name = watermark_table_name, 
            watermark_column_name = watermark_column_name
        )

        watermark_filter = _construct_watermark_filter(
            watermark_column_data_type = watermark_column_data_type, 
            watermark_column_name = watermark_column_name, 
            watermark_value = watermark_value
        )
    else:
        new_watermark_value = ""
        watermark_filter = ""

    ingesting_custom_table_ingestion_function_log_info = f"Ingesting data with custom table function, {custom_table_ingestion_function}."
    log_and_print(ingesting_custom_table_ingestion_function_log_info)

    source_details = f"Custom table function, {custom_table_ingestion_function}"

    if custom_table_ingestion_function_notebook:
        source_details += f", in {custom_table_ingestion_function_notebook}"
        instantiate_notebook(notebook_name = custom_table_ingestion_function_notebook)

    all_metadata['watermark_column_name'] = watermark_column_name
    all_metadata['watermark_value'] = watermark_value
    all_metadata['watermark_filter'] = watermark_filter

    function_kwargs = {
        "metadata": all_metadata,
        "spark": spark
    }

    new_data = globals()[custom_table_ingestion_function](**function_kwargs)

    return new_data, new_watermark_value, source_details

def _read_full_table_with_cdf_columns(
    extracted_table_name: str
) -> tuple:
    """Read full table for initial load and add CDF metadata columns for downstream compatibility.
    
    Args:
        extracted_table_name: Name of the Delta table to read from
    
    Returns:
        tuple: (new_data DataFrame, new_watermark_value, source_details string)
    """
    log_and_print(f"Reading initial full table snapshot (no CDF): {extracted_table_name}")

    # Get table version BEFORE reading to ensure consistency
    table_version = DeltaTable.forName(spark, extracted_table_name).history(1).select("version").first()["version"]
    
    # Read full table
    new_data = spark.read.format("delta").table(extracted_table_name)
    
    # Add CDF metadata columns for downstream compatibility
    new_data = new_data \
        .withColumn("_commit_version", f.lit(table_version)) \
        .withColumn("_commit_timestamp", f.current_timestamp()) \
        .withColumn("_change_type", f.lit("insert"))
    
    # New watermark is current version + 1
    new_watermark_value = str(table_version + 1)
    log_and_print(f"New watermark (version): {new_watermark_value} (initial load from version: {table_version})")

    source_details = f"Full table read from {extracted_table_name} (initial load, version: {table_version})"
    
    return new_data, new_watermark_value, source_details


def _read_cdf_incremental(
    extracted_table_name: str,
    watermark_value: str
) -> tuple:
    """Read incremental changes from Delta table using Change Data Feed.
    
    Args:
        extracted_table_name: Name of the Delta table to read from
        watermark_value: Previous watermark value (Delta version number) for incremental loading
    
    Returns:
        tuple: (new_data DataFrame, new_watermark_value, source_details string)
    """
    log_and_print(f"Ingesting incremental data using Change Data Feed: {extracted_table_name}")
    log_and_print(f"Reading CDF changes from version: {watermark_value}")
    
    # Check if the requested version exists in the table history
    current_version = DeltaTable.forName(spark, extracted_table_name).history(1).select("version").first()["version"]
    requested_version = int(watermark_value)
    
    if requested_version > current_version:
        # Requested version doesn't exist yet - no new changes
        log_and_print(f"No new changes: requested version {requested_version} > current version {current_version}")
        log_and_print("Returning empty DataFrame - no data to process")
        
        # Create empty DataFrame with CDF schema for consistency
        base_df = spark.read.format("delta").table(extracted_table_name).limit(0)
        new_data = base_df \
            .withColumn("_commit_version", f.lit(None).cast("long")) \
            .withColumn("_commit_timestamp", f.lit(None).cast("timestamp")) \
            .withColumn("_change_type", f.lit(None).cast("string"))
        
        # Keep same watermark since no new data
        new_watermark_value = str(requested_version)
        log_and_print(f"No new watermark - keeping version: {new_watermark_value}")
        
        source_details = f"Change Data Feed from {extracted_table_name} (no changes: version {requested_version} not yet available)"
    else:
        # Version exists - read CDF changes
        new_data = spark.read.format("delta") \
            .option("readChangeFeed", "true") \
            .option("startingVersion", watermark_value) \
            .table(extracted_table_name)
        
        # Filter to only relevant change types (exclude update_preimage)
        log_and_print("Filtering CDF data to include only insert, update_postimage, and delete change types")
        new_data = new_data.filter(
            f.col("_change_type").isin("insert", "update_postimage", "delete")
        )
        
        # Set watermark based on whether data exists after filtering
        if new_data.isEmpty():
            # Data was filtered out completely - keep current version
            new_watermark_value = str(requested_version)
            log_and_print(f"No data after filtering - keeping watermark at version: {new_watermark_value}")
        else:
            # Data exists - advance watermark to current table version + 1
            new_watermark_value = str(current_version + 1)
            log_and_print(f"New watermark (version): {new_watermark_value} (current table version: {current_version})")
        
        source_details = f"Change Data Feed from {extracted_table_name} (startingVersion: {watermark_value})"
    
    return new_data, new_watermark_value, source_details


def _ingest_from_table_with_cdf(
    extracted_table_name: str,
    watermark_value: str,
    first_run: bool
) -> tuple:
    """Handle ingestion from Fabric tables using Change Data Feed.
    
    Routes to appropriate ingestion method based on whether this is an initial or incremental load.
    For incremental loads, it reads CDF changes and filters to relevant change types.
    For initial loads, it reads the full table and adds CDF metadata columns for consistency.
    
    Important Notes:
        - Watermark tracks the Delta table version number (integer)
        - New watermark is set to current table version + 1 to avoid re-reading same version
        - Automatically filters out 'update_preimage' change type (only 'insert', 
          'update_postimage', and 'delete' are returned)
        - If a single record has multiple updates/deletes between watermark checks,
          CDF will return multiple rows for that primary key
        - Recommended: Add drop_duplicates transformation on primary keys with 
          order_by='_commit_version' to keep only the most recent change
    
    Args:
        extracted_table_name: Name of the Delta table to read from
        watermark_value: Previous watermark value (Delta version number as string) for incremental loading
        first_run: Whether this is the first run (full load vs incremental)
    
    Returns:
        tuple: (new_data DataFrame, new_watermark_value, source_details string)
    
    Example Configuration for Deduplication:
        ```sql
        INSERT INTO Advanced_Configuration VALUES
        (106, 'data_transformation_steps', 'drop_duplicates', 1, 'order_by', '_commit_version');
        ```
    """
    # Check if watermark is a default/uninitialized value (treat as first run)
    is_default_watermark = (
        str(watermark_value) in ('1900-01-01 00:00:00', '-1') 
        if watermark_value else False
    )
    
    # Route based on first_run flag or default watermark values
    if first_run or is_default_watermark:
        # Initial load: Read full table with CDF columns
        new_data, new_watermark_value, source_details = _read_full_table_with_cdf_columns(
            extracted_table_name=extracted_table_name
        )
    else:
        # Incremental load: Read CDF changes
        new_data, new_watermark_value, source_details = _read_cdf_incremental(
            extracted_table_name=extracted_table_name,
            watermark_value=watermark_value
        )
    
    return new_data, new_watermark_value, source_details


def _ingest_from_table_with_watermark(
    source_query: str,
    extracted_table_name: str,
    watermark_column_data_type: str,
    watermark_column_name: str,
    watermark_value: str,
    watermark_table_name: str,
    first_run: bool
) -> tuple:
    """Handle ingestion from Fabric tables using traditional watermark filtering.
    
    Args:
        source_query: SQL query to execute
        extracted_table_name: Name of the table being queried
        watermark_column_data_type: Data type of watermark column ('datetime' or 'numeric')
        watermark_column_name: Name of watermark column(s)
        watermark_value: Previous watermark value for incremental loading
        watermark_table_name: Table name to extract watermark from
        first_run: Whether this is the first run (full load vs incremental)
    
    Returns:
        tuple: (new_data DataFrame, new_watermark_value, source_details string)
    """
    ingesting_from_table_log_info = f"Ingesting data from table, {extracted_table_name}."
    log_and_print(ingesting_from_table_log_info)
    
    # Construct watermark filter based on data type and value
    if watermark_value:
        new_watermark_value = _extract_and_validate_watermark(
            watermark_table_name = watermark_table_name, 
            watermark_column_name = watermark_column_name
        )

        watermark_filter = _construct_watermark_filter(
            watermark_column_data_type = watermark_column_data_type, 
            watermark_column_name = watermark_column_name, 
            watermark_value = watermark_value
        )
    else:
        new_watermark_value = ""
        watermark_filter = ""

    # Execute the source query
    executing_source_query_log_info = f"Executing source query: {source_query}"
    log_and_print(executing_source_query_log_info)

    source_details = source_query
    new_data = spark.sql(source_query)

    # Apply watermark filtering if applicable
    if not first_run and watermark_filter:
        applying_watermark_filter_log_info = f"Applying watermark filter: {watermark_filter}"
        log_and_print(applying_watermark_filter_log_info)

        new_data = new_data.filter(watermark_filter)
        source_details += f" WHERE {watermark_filter}"
    
    return new_data, new_watermark_value, source_details


def _ingest_from_table(
    source_query: str, 
    watermark_column_data_type: str, 
    watermark_column_name: str, 
    watermark_value: str, 
    watermark_table_name: str,
    use_change_data_feed: bool,
    first_run: bool
) -> tuple:
    """Orchestrate ingestion from Fabric tables with watermark filtering or Change Data Feed.
    
    This function routes to the appropriate ingestion method based on configuration:
    - Change Data Feed (CDF): Uses Delta Lake's built-in change tracking with _commit_timestamp
    - Traditional Watermark: Uses business columns to track changes
    
    Args:
        source_query: SQL query to execute
        watermark_column_data_type: Data type of watermark column ('datetime' or 'numeric')
        watermark_column_name: Name of watermark column(s) - only used for traditional watermarking
        watermark_value: Previous watermark value for incremental loading
        watermark_table_name: Table name to extract watermark from
        use_change_data_feed: If True, use Delta Change Data Feed instead of traditional watermarking
    
    Returns:
        tuple: (new_data DataFrame, new_watermark_value, source_details string)
    """
    extracted_table_name = source_query.replace("SELECT * FROM ", "")

    # Route to appropriate ingestion method
    if use_change_data_feed:
        return _ingest_from_table_with_cdf(
            extracted_table_name = extracted_table_name,
            watermark_value = watermark_value,
            first_run = first_run
        )
    else:
        return _ingest_from_table_with_watermark(
            source_query = source_query,
            extracted_table_name = extracted_table_name,
            watermark_column_data_type = watermark_column_data_type,
            watermark_column_name = watermark_column_name,
            watermark_value = watermark_value,
            watermark_table_name = watermark_table_name,
            first_run = first_run
        )

def convert_datetime_to_string(value):
    """Convert datetime object to string format if needed."""
    from datetime import datetime
    
    if type(value) == datetime:
        log_and_print("Converting watermark datetime value from datetime to string.")
        return value.strftime("%Y-%m-%d %H:%M:%S.%f")
    
    return value

def ingest_from_fabric_table(
    source_config: dict,
    watermark_config: dict,
    all_metadata: dict,
    first_run: bool,
    lineage_info: dict
) -> DataFrame:
    """
    Ingest data incrementally from Fabric Delta tables using watermark-based filtering or Change Data Feed.

    This function executes SQL queries against Fabric tables and applies either:
    1. Traditional watermark filtering - Uses business columns to track changes
    2. Change Data Feed (CDF) - Uses Delta Lake's built-in change tracking with _commit_timestamp

    CDF provides more reliable incremental loading by capturing all changes (inserts, updates, deletes)
    at the transaction level, eliminating the need for business watermark columns.

    The function automatically exits the notebook if no new data is found,
    preventing unnecessary processing and maintaining pipeline efficiency.

    Args:
        source_config (dict): Source configuration with query and table details
        watermark_config (dict): Watermark settings including use_change_data_feed flag
        custom_paths (dict): Path configurations for custom functions
        all_metadata (dict): Complete metadata dictionary
        first_run (bool): Flag indicating first run
        lineage_info (dict): Lineage tracking information for logging

    Returns:
        tuple: (new_data DataFrame, new_watermark_value string, source_details string)

    Raises:
        Exception: If an unsupported watermark column data type is specified (traditional mode).

    Performance Considerations:
        - CDF mode: Efficient change tracking at transaction level, no column scans needed
        - Traditional mode: Watermark filtering applied after query execution
        - For large datasets with traditional mode, consider adding watermark predicates to source_query
        - Indexes on watermark columns significantly improve traditional mode performance

    Notes:
        - CDF mode requires Delta tables with Change Data Feed enabled
        - CDF tracks changes using _commit_timestamp for better observability in logs
        - Traditional mode: Empty watermark values trigger full table scans (initial loads)
        - The function preserves the original query's column order and naming
        - Notebook exits gracefully if no new data is found
    """
    watermark_value = watermark_config['watermark_value']

    # Ingest using custom table function if provided in metadata
    if source_config['custom_table_ingestion_function']:
        new_data, new_watermark_value, source_details = _ingest_with_custom_table_function(
            custom_table_ingestion_function = source_config['custom_table_ingestion_function'],
            custom_table_ingestion_function_notebook = source_config['custom_table_ingestion_function_notebook'],
            all_metadata = all_metadata,
            watermark_table_name = source_config['watermark_table_name'],
            watermark_column_name = watermark_config['watermark_column_name'],
            watermark_column_data_type = watermark_config['watermark_column_data_type'],
            watermark_value = watermark_value
        )
    # otherwise, ingest all data from table
    else:
        new_data, new_watermark_value, source_details = _ingest_from_table(
            source_query = source_config['source_query'],
            watermark_column_data_type = watermark_config['watermark_column_data_type'],
            watermark_column_name = watermark_config['watermark_column_name'],
            watermark_value = watermark_value,
            watermark_table_name = source_config['watermark_table_name'],
            use_change_data_feed = watermark_config['use_change_data_feed'],
            first_run = first_run
        )

    # Check if any data was returned after filtering
    if new_data.isEmpty():
        exit_gracefully_no_data(
            source_details = source_details,
            watermark_value = new_watermark_value,
            lineage_info = lineage_info,
            first_run = first_run,
            context_message = "No new data returned from Fabric table after watermark filtering."
        )

    # Convert datetime objects to string format for logging
    new_watermark_value = convert_datetime_to_string(value = new_watermark_value)

    return new_data, new_watermark_value, source_details

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Initiate Logging Function

# CELL ********************

def log_and_print(
    message: str, 
    level: str = "info"
):
    """
    Reusable function to log and print messages with different severity levels.
    
    Args:
        message (str): The message to log and print
        level (str): The logging level - "info", "warn", or "error"
    """
    # https://learn.microsoft.com/en-us/fabric/data-engineering/azure-fabric-diagnostic-emitters-log-analytics#write-custom-application-logs
    logger = sc._jvm.org.apache.log4j.LogManager.getLogger("spark_application_logger")

    valid_levels = ["info", "warn", "error"]
    if level.lower() not in valid_levels:
        raise ValueError(f"Invalid log level '{level}'. Valid options are: {', '.join(valid_levels)}")
    
    level_upper = level.upper()
    if level.lower() == "warn":
        level_upper = "WARNING"
    elif level.lower() == "error":
        level_upper = "ERROR"
    
    formatted_message = f"[{level_upper}] {message}"
    
    if level.lower() == "info":
        logger.info(message)
    elif level.lower() == "warn":
        logger.warn(message)
    elif level.lower() == "error":
        logger.error(message)

    print(formatted_message)
    # Add a newline for better readability in console output
    print()

def exit_gracefully_no_data(
    source_details: str,
    watermark_value: Union[str, datetime, Any],
    lineage_info: dict,
    first_run: bool,
    context_message: str = ""
):
    """
    Reusable function to handle graceful exit when no data is found.
    
    This function handles the common pattern of exiting a notebook when no new data
    is available. On first run, it raises an exception (no data = configuration error).
    On subsequent runs, it exits gracefully (no data since watermark = normal).
    
    Args:
        source_details (str): Source path or description for error/exit messages
        watermark_value (Union[str, datetime, Any]): Current watermark value. Accepts string,
            datetime objects, or any type - will be converted to string for exit payload.
        lineage_info (dict): Lineage tracking info for exit payload
        first_run (bool): Whether this is the first run (True = error, False = graceful exit)
        context_message (str): Optional additional context for log messages
    
    Raises:
        Exception: If first_run=True and no data found (configuration error)
    
    Side Effects:
        Calls notebookutils.notebook.exit() if not first_run (terminates notebook)
    """
    # CRITICAL: On first run, no data is an ERROR - likely a configuration problem
    if first_run:
        error_msg = (
            f"FIRST RUN FAILURE: No data was found during initial load.\n"
            f"Context: {context_message}\n"
            f"Source: {source_details}\n"
            f"This likely indicates a configuration problem - please verify the source path, query, or custom function."
        )
        log_and_print(error_msg, "error")
        raise Exception(error_msg)
    
    # Not first run - exit gracefully (no new data since last watermark is normal)
    no_new_data_log_info = f"No new data found. {context_message}" if context_message else "No new data found since last data load."
    log_and_print(no_new_data_log_info)

    # Convert datetime objects to string format for exit payload
    new_watermark_value = convert_datetime_to_string(value=watermark_value)
    
    notebookutils.notebook.exit({
        "source_details": source_details,
        "status": "Processed",
        "data_quality_warnings": "",
        "records_processed": "0",
        "quarantined_records": "0",
        "new_watermark_value": new_watermark_value,
        "new_schema": "False",
        "source_medallion_layer": lineage_info['source_medallion_layer'],
        "source_type": lineage_info['source_type'],
        "target_medallion_layer": lineage_info['target_medallion_layer'],
        "target_type": lineage_info['target_type']
    })

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
