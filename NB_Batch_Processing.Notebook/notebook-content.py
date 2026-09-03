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
# META     },
# META     "warehouse": {}
# META   }
# META }

# MARKDOWN ********************

# # NB_Batch_Processing - Core Data Ingestion Engine
# 
# ## Overview
# This notebook serves as the primary data processing engine for the Fabric Data Platform Accelerator. It orchestrates batch data ingestion and transformation across all medallion architecture layers (Bronze, Silver, Gold) using a metadata-driven approach.
# 
# ### Key Capabilities
# - **Multi-Source Ingestion**: Processes data from files, Delta tables, and external databases
# - **Schema Evolution**: Automatically detects and handles schema changes with configurable actions
# - **Data Quality Management**: Executes comprehensive data quality checks with quarantine capabilities
# - **Transformation Pipeline**: Applies cleansing, custom transformations, and pre-built functions
# - **Advanced Features**: Supports SCD2 dimensions, surrogate key generation, and entity resolution
# 
# ### Integration Points
# - Executes helper functions from: `NB_Helper_Functions_1`, `NB_Helper_Functions_2`, `NB_Helper_Functions_3`
# - Integrates with metadata warehouse for configuration and logging
# - Supports dynamic Spark pool allocation for performance optimization


# CELL ********************

# MAGIC %%configure
# MAGIC {
# MAGIC     "defaultLakehouse": {
# MAGIC         "name": { 
# MAGIC             "parameterName": "default_lakehouse_name", 
# MAGIC             "defaultValue": "" 
# MAGIC         },
# MAGIC         "id": { 
# MAGIC             "parameterName": "default_lakehouse_id", 
# MAGIC             "defaultValue": "" 
# MAGIC         },
# MAGIC         "workspaceId": { 
# MAGIC             "parameterName": "default_lakehouse_workspace_id", 
# MAGIC             "defaultValue": "" 
# MAGIC         }
# MAGIC     },
# MAGIC     "environment": {
# MAGIC         "id": { 
# MAGIC             "parameterName": "spark_environment_id", 
# MAGIC             "defaultValue": "" 
# MAGIC         }
# MAGIC     }
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. Notebook Parameters
# 
# The following parameters control the behavior of this notebook. They are populated by the orchestration pipeline based on metadata configurations.

# PARAMETERS CELL ********************

# Metadata-driven parameters populated by orchestration pipeline
orchestration_metadata = '{}'      # JSON: Contains table ID, target details, and orchestration context
primary_config = '{}'              # JSON: Primary configuration including source, target, and processing settings
advanced_config = "[]"             # JSON Array: Advanced configurations for transformations and data quality
watermark_value = ""               # String: Previous high watermark for incremental processing (e.g., "20240904214312")
full_reload = ""                   # String: Set to "Yes" to drop and recreate target table
table_ddl = "[]"                   # JSON Array: Source database DDL for schema replication
latest_schema_details = '{}'       # JSON: Schema information from previous runs for change detection
workspace_id = ""                  # String: Fabric workspace GUID where processing occurs
event_payload = ""                 # String: Optional event data for event-driven scenarios
folder_path_from_trigger = ""      # String: Optional folder path for file-triggered ingestion
datastore_config = "[]"            # JSON Array: Datastore configuration from Datastore_Configuration table lookup
data_factory_run_id = ""           # String: Data Factory run ID so logs can be correlated between spark and data factory

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Initialize Helper Functions
# 
# - Load reusable helper functions from companion notebooks. These functions encapsulate common operations and ensure consistency across the solution.

# CELL ********************

%run NB_Helper_Functions_1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run NB_Helper_Functions_2

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run NB_Helper_Functions_3

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Parse Parameters + Metadata Config + Configure Spark Session
# - Create date dimension if needed


# CELL ********************

log_and_print(f"Data Factory Run ID: {data_factory_run_id}")

# ===========================================================================================
# PARSE ORCHESTRATION METADATA AND CONFIGURATION PARAMETERS
# ===========================================================================================
# This section transforms JSON-formatted metadata from the orchestration pipeline into
# Python dictionaries and variables that control the behavior of data processing operations.
# Each parameter is carefully extracted and validated to ensure robust execution.
# ===========================================================================================

# Parse all JSON metadata inputs into Python dictionaries
parsed_metadata = parse_json_metadata(
    orchestration_metadata_json = orchestration_metadata,
    primary_config_json = primary_config,
    advanced_config_json = advanced_config,
    table_ddl_json = table_ddl,
    latest_schema_details_json = latest_schema_details
)

# Unpack parsed metadata into individual variables for backward compatibility
orchestration_metadata = parsed_metadata['orchestration_metadata']
primary_config = parsed_metadata['primary_config']
advanced_config = parsed_metadata['advanced_config']
table_ddl = parsed_metadata['table_ddl']
latest_schema_details = parsed_metadata['latest_schema_details']

# Clean configuration data
advanced_config = clean_advanced_config(advanced_config)
latest_schema_details = normalize_schema_details(latest_schema_details)

# Extract schema tracking information for change detection
schema_tracking = extract_schema_tracking_info(latest_schema_details)
last_schema_id = schema_tracking['last_schema_id']
last_schema_details = schema_tracking['last_schema_details']

# Parse source configuration
source_config = parse_source_configuration(
    orchestration_metadata = orchestration_metadata,
    primary_config = primary_config,
    datastore_config = datastore_config,
    folder_path_from_trigger = folder_path_from_trigger
)

# Unpack for backward compatibility
table_id = source_config['table_id']
staging_folder_path = source_config['staging_folder_path']
wildcard_folder_path = source_config['wildcard_folder_path']
using_source_folder_path = source_config['using_source_folder_path']
source_datastore_name = source_config['source_datastore_name']
source_category = source_config['source_category']

# Parse target configuration
target_config = parse_target_configuration(
    orchestration_metadata = orchestration_metadata,
    primary_config = primary_config,
    datastore_config = datastore_config
)

# Unpack for backward compatibility
target_datastore_id = target_config['target_datastore_id']
target_datastore_workspace_id = target_config['target_datastore_workspace_id']
target_table_name = target_config['target_table_name']
target_abfss_path = target_config['target_abfss_path']
default_merge_type = target_config['default_merge_type']  # May have been updated
output_external_location = target_config['output_external_location']
enforce_not_null = target_config['enforce_not_null']
target_quarantined_abfss_path = target_config['target_quarantined_abfss_path']
lakehouse_table_output = target_config['lakehouse_table_output']
target_datastore_name = target_config['target_datastore_name']
target_datastore_medallion_name = target_config['target_datastore_medallion_name']
default_watermark_column_name = target_config['default_watermark_column_name']

# Parse file ingestion paths
custom_paths = parse_file_ingestion_paths(
    datastore_config = datastore_config,
    source_datastore_name = source_datastore_name,
    target_datastore_workspace_id = target_datastore_workspace_id,
    target_datastore_id = target_datastore_id,
    table_id = table_id,
    wildcard_folder_path = wildcard_folder_path,
    primary_config = primary_config
)

# Unpack for backward compatibility
file_staging_path = custom_paths['file_staging_path']
clean_up_temporary_path = custom_paths['clean_up_temporary_path']

# Parse watermark configuration
watermark_config = parse_watermark_configuration(
    primary_config = primary_config,
    default_merge_type = default_merge_type,
    default_watermark_column_name = default_watermark_column_name,
    staging_folder_path = staging_folder_path,
    using_source_folder_path = using_source_folder_path,
    watermark_value = watermark_value
)

# Unpack for backward compatibility
column_to_mark_source_data_deletion = watermark_config['column_to_mark_source_data_deletion']
delete_rows_with_value = watermark_config['delete_rows_with_value']
merge_type = watermark_config['merge_type']
default_merge_type = watermark_config['default_merge_type']  # May have been updated
merge_in_batches_with_columns = watermark_config['merge_in_batches_with_columns']
watermark_column_name = watermark_config['watermark_column_name']
watermark_value = watermark_config['watermark_value']

# Extract lineage information for logging
lineage_info = extract_lineage_information(
    source_config = source_config,
    target_config = target_config,
    datastore_config = datastore_config,
    merge_type = merge_type
)

# Construct all_metadata dictionary
all_metadata = {
    "orchestration_metadata": orchestration_metadata,
    "primary_config": primary_config,
    "advanced_config": advanced_config,
    "datastore_config": datastore_config,
    "event_payload": event_payload
}

# Parse advanced processing configuration
advanced_processing_config = parse_advanced_processing_configuration(
    primary_config = primary_config,
    using_source_folder_path = using_source_folder_path,
    target_datastore_medallion_name = target_datastore_medallion_name
)

# Unpack for backward compatibility
liquid_clustering_columns = advanced_processing_config['liquid_clustering_columns']
# Extract only the configs needed at this level (not column name standardization - handled in functions)
fail_on_new_schema = advanced_processing_config['fail_on_new_schema']
fail_on_column_data_type_change = advanced_processing_config['fail_on_column_data_type_change']
if_duplicate_primary_keys = advanced_processing_config['if_duplicate_primary_keys']
trim_data_in_string_columns = advanced_processing_config['trim_data_in_string_columns']
replace_blank_with_null_in_string_columns = advanced_processing_config['replace_blank_with_null_in_string_columns']

# Parse primary key configuration
primary_key_config = parse_primary_key_configuration(
    orchestration_metadata = orchestration_metadata
)

# Unpack for backward compatibility
primary_keys = primary_key_config['primary_keys']

# Parse advanced configuration steps
advanced_steps_config = parse_advanced_configuration_steps(
    advanced_config = advanced_config
)

# Unpack for backward compatibility
data_quality_steps = advanced_steps_config['data_quality_steps']
data_transformation_steps = advanced_steps_config['data_transformation_steps']

# Parse dimension table configuration
dimension_config = parse_dimension_table_configuration(
    primary_config = primary_config,
    advanced_config = advanced_config
)

# Unpack for backward compatibility
enable_scd2_dimension = dimension_config['enable_scd2_dimension']
fact_table_data_load = dimension_config['fact_table_data_load']

# Parse warehouse configuration (only executes Spark config if merge_type is 'warehouse_spark_connector')
warehouse_config = parse_warehouse_configuration(
    merge_type = merge_type,
    datastore_config = datastore_config,
    target_datastore_name = target_datastore_name,
    spark_module = spark
)

file_config = extract_file_configuration(
    primary_config = primary_config,
    fail_on_new_schema = fail_on_new_schema
) 

# ===========================================================================================
# SPARK SESSION CONFIGURATION BY LAKEHOUSE LAYER
# ===========================================================================================
# This section applies layer-specific Spark configurations to optimize performance based on
# the characteristics and requirements of each medallion architecture layer.
# ===========================================================================================

# Parse performance configuration
performance_config = parse_performance_configuration(
    primary_config = primary_config,
    target_datastore_medallion_name = target_datastore_medallion_name
)

# Unpack for backward compatibility
compute_statistics_on_columns = performance_config['compute_statistics_on_columns']
compute_statistics_on_first_n_columns = performance_config['compute_statistics_on_first_n_columns']
use_spark_config_for_lakehouse = performance_config['use_spark_config_for_lakehouse']

# Parse Spark configuration
spark_config = parse_spark_configuration(
    primary_config = primary_config
)

# Unpack for backward compatibility
enable_change_data_feed = spark_config['enable_change_data_feed']
spark_timestamp_rebase_mode_write = spark_config['spark_timestamp_rebase_mode_write']
spark_timestamp_rebase_mode_read = spark_config['spark_timestamp_rebase_mode_read']

apply_spark_configurations(
    spark_timestamp_rebase_mode_write = spark_timestamp_rebase_mode_write,
    spark_timestamp_rebase_mode_read = spark_timestamp_rebase_mode_read,
    enable_change_data_feed = enable_change_data_feed,
    use_spark_config_for_lakehouse = use_spark_config_for_lakehouse
)

# ===========================================================================================
# DATE DIMENSION TABLE INITIALIZATION
# ===========================================================================================
# Creates a comprehensive date dimension table if it doesn't exist.
# This is a prerequisite for fact and dimension table processing in star schemas.
# ===========================================================================================
create_date_dimension(
    fact_table_data_load = fact_table_data_load,
    target_datastore_workspace_id = target_datastore_workspace_id,
    target_datastore_id = target_datastore_id,
    date_table_schema_name = 'dbo',
    date_table_name = 'dim_date',
    date_dimension_table_key_column_name = "date_sk"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Full Reload Processing and Determine if Table Exists
# 
# Check if this is a first-time load or if a full reload is requested. Handle table existence and set appropriate flags for downstream processing.

# CELL ********************

# Execute table existence and first run determination
first_run, target_table_exists, watermark_value = determine_first_run_and_table_existence(
    target_table_name = target_table_name,
    full_reload = full_reload,
    watermark_value = watermark_value,
    target_abfss_path = target_abfss_path,
    lakehouse_table_output = lakehouse_table_output
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Data Ingestion
# 
# Read data from the configured source. Supports two primary patterns:
# - **Delta Tables**: Query-based ingestion from existing Delta tables
# - **Files**: File-based ingestion with various formats (CSV, JSON, Parquet, etc.)

# CELL ********************

new_data, new_watermark_value, source_details = route_to_ingestion_method(
    source_config = source_config,
    watermark_config = watermark_config,
    custom_paths = custom_paths,
    file_config = file_config,
    all_metadata = all_metadata,
    folder_path_from_trigger = folder_path_from_trigger,
    first_run = first_run,
    lineage_info = lineage_info
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Column Standardization and Metadata Enrichment
# 
# Standardize column names according to configured rules and add system metadata columns for audit and tracking purposes.

# CELL ********************

# Standardize column names and update key column references
new_data, primary_keys, liquid_clustering_columns = cleanse_column_names(
    df = new_data,
    primary_keys = primary_keys,
    liquid_clustering_columns = liquid_clustering_columns,
    advanced_processing_config = advanced_processing_config
)

# Add system metadata columns for audit trail and change tracking
new_data = add_timestamp_metadata_columns(
    df = new_data,
    target_table_name = target_table_name
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Create Target Schema If It Doesn't Exist
# Create the target schema if it doesn't exist and prepare for data writing.

# CELL ********************

# Extract schema abfss path from table abfss path and create if needed
create_schema_if_not_exists(
    target_abfss_path = target_abfss_path,
    lakehouse_table_output = lakehouse_table_output
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8. Schema DDL Processing
# 
# When ingesting from external databases, use the source DDL to create an exact schema replica in the Delta table. This ensures data type fidelity and constraint preservation.

# CELL ********************

# Create Delta table ddl with source-matched schema
ddl_header, column_definitions, ddl_footer = orchestrate_ddl_creation(
    target_table_name = target_table_name,
    table_ddl = table_ddl,
    enforce_not_null = enforce_not_null,
    liquid_clustering_columns = liquid_clustering_columns,
    output_external_location = output_external_location,
    source_category = source_category,
    advanced_processing_config = advanced_processing_config
)

# Use created DDL to create delta table and update table definitions in data
new_data, target_table_exists = implement_table_ddl(
    df = new_data,
    ddl_header = ddl_header,
    column_definitions = column_definitions,
    ddl_footer = ddl_footer,
    target_table_exists = target_table_exists
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 9. Data Cleansing Operations
# 
# Apply configured data cleansing rules including:
# - Blank to null conversion
# - Duplicate removal based on primary keys
# - Exact duplicate elimination
# - Column-specific cleansing logic

# CELL ********************

# Execute data cleansing operations based on metadata configuration
new_data = data_cleansing(
    df = new_data, 
    trim_data_in_string_columns = trim_data_in_string_columns, 
    replace_blank_with_null_in_string_columns = replace_blank_with_null_in_string_columns
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 10. Data Transformation Functions
# 
# Apply configured transformations from metadata including:
# - Column renaming
# - Derived column creation
# - Data filtering
# - Column removal
# - Surrogate Keys for Facts and Dimensions
# - Entity Matching
# - Custom Functions

# CELL ********************

# Execute pre-built transformation functions based on metadata configuration
new_data = transformation_functions(
    df = new_data, 
    data_transformation_steps = data_transformation_steps, 
    first_run = first_run, 
    target_table_name = target_table_name,
    primary_keys = primary_keys,
    dimension_config = dimension_config,
    watermark_config = watermark_config,
    custom_paths = custom_paths,
    merge_type = merge_type,
    all_metadata = all_metadata,
    lineage_info = lineage_info
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 11. Data Quality Validation
# 
# Execute comprehensive data quality checks including:
# - Primary key uniqueness validation
# - Data filter rules
# - Reference table lookups
# - Minimum record thresholds
# - Schema change detection

# CELL ********************

# Reorder columns
new_data = reorder_columns_for_output(new_data = new_data)

# Execute data quality checks
dq_warnings, dq_force_failures, new_data, quarantined_data = execute_data_quality_checks(
    new_data = new_data, 
    data_quality_steps = data_quality_steps, 
    if_duplicate_primary_keys = if_duplicate_primary_keys, 
    primary_keys = primary_keys, 
    target_table_name = target_table_name, 
    datastore_config = datastore_config, 
    merge_in_batches_with_columns = merge_in_batches_with_columns
)

# Analyze schema changes
new_schema, new_schema_details, new_data_schema_hash, schema_change_summary, schema_type_updates_summary, column_types_that_changed = analyze_schema_changes(
    new_data = new_data, 
    last_schema_id = last_schema_id, 
    last_schema_details = last_schema_details
)

# Handle schema change failures
dq_warnings, dq_force_failures = handle_schema_change_failures(
    new_schema = new_schema, 
    fail_on_new_schema = fail_on_new_schema, 
    first_run = first_run, 
    last_schema_id = last_schema_id, 
    schema_change_summary = schema_change_summary, 
    fail_on_column_data_type_change = fail_on_column_data_type_change, 
    column_types_that_changed = column_types_that_changed, 
    schema_type_updates_summary = schema_type_updates_summary, 
    dq_warnings = dq_warnings, 
    dq_force_failures = dq_force_failures
)

# Finalize processing
new_data = finalize_processing(
    dq_force_failures = dq_force_failures, 
    source_details = source_details, 
    dq_warnings = dq_warnings, 
    new_schema = new_schema, 
    new_data = new_data, 
    target_datastore_medallion_name = target_datastore_medallion_name, 
    new_data_schema_hash = new_data_schema_hash,
    lineage_info = lineage_info
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 12. Write Data to Target

# CELL ********************

new_data = add_scd2_columns_for_dimensions(
    new_data = new_data,
    column_to_mark_source_data_deletion = column_to_mark_source_data_deletion,
    delete_rows_with_value = delete_rows_with_value,
    source_timestamp_column_name = dimension_config['source_timestamp_column_name'],
    lakehouse_table_output = lakehouse_table_output,
    first_run = first_run,
    enable_scd2_dimension = enable_scd2_dimension,
)

set_column_statistics(
    df = new_data,
    compute_statistics_on_columns = compute_statistics_on_columns,
    compute_statistics_on_first_n_columns = compute_statistics_on_first_n_columns,
    lakehouse_table_output = lakehouse_table_output
)

create_delta_table(
    df = new_data,
    liquid_clustering_columns = liquid_clustering_columns,
    target_table_name = target_table_name,
    output_external_location = output_external_location,
    lakehouse_table_output = lakehouse_table_output,
    target_table_exists = target_table_exists,
)

# ===========================================================================================
# DATA WRITE ORCHESTRATION
# ===========================================================================================
# Route to appropriate write strategy based on configuration:
# - Batch mode: Process data in sequential batches (helpful for reloading data and recovery)
# - Standard mode: Process all data at once (optimal performance for most scenarios)
# ===========================================================================================
total_records_processed = write_data_orchestrator(
    df = new_data,
    first_run = first_run,
    target_config = target_config,
    watermark_config = watermark_config,
    primary_keys = primary_keys,
    dimension_config = dimension_config
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 13. Quarantine Data if Necessary
# 
# Write records that failed data quality checks to a dedicated quarantine table for review and remediation.

# CELL ********************

# Process quarantined records if any exist
quarantined_records = write_quarantined_data(
    quarantined_df = quarantined_data,
    target_quarantined_abfss_path = target_quarantined_abfss_path
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 14. Post-Processing After Ingesting Data
# 
# Apply final cleanup and adjustments for tables:
# - Adjust SCD2 date ranges for dimension tables
# - Handle external Delta tables written to ADLS Gen2. Drop the external table reference to allow OneLake shortcut creation.
# - Run Delta Lake optimization if liquid clustering is enabled. This reorganizes data files for better query performance.
# - Remove old Delta files based on retention policy. Only runs if no recent vacuum operations detected.
# - Remove temporary CSV's when reading Excel and XML Files

# CELL ********************

execute_scd2_post_processing(
    primary_keys = primary_keys,
    target_abfss_path = target_abfss_path,
    lakehouse_table_output = lakehouse_table_output,
    enable_scd2_dimension = enable_scd2_dimension,
    total_records_processed = total_records_processed
)

drop_external_table_for_shortcut(
    target_table_name = target_table_name,
    first_run = first_run,
    output_external_location = output_external_location,
    lakehouse_table_output = lakehouse_table_output
)

execute_vacuum_if_needed(
    lakehouse_table_output = lakehouse_table_output,
    target_table_name = target_table_name,
    target_abfss_path = target_abfss_path,
    min_operations_threshold = 50
)

cleanup_temporary_files(
    file_staging_path = file_staging_path,
    clean_up_temporary_path = clean_up_temporary_path
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #

# MARKDOWN ********************

# ## 15. Exit Notebook with Output Values
# 
# Return processing results to the orchestration pipeline for logging and monitoring. The exit payload includes all relevant metrics and status information.

# CELL ********************

# Exit notebook with comprehensive processing results
notebookutils.notebook.exit({
    "source_details": source_details,                     # Source information for logging
    "status": "Processed",                                # Processing status
    "data_quality_warnings": json.dumps(dq_warnings),     # Data quality warnings (if any)
    "records_processed": total_records_processed,         # Number of records written
    "quarantined_records": quarantined_records,           # Number of quarantined records
    "new_watermark_value": new_watermark_value,           # Updated watermark for next run
    "new_schema": str(new_schema),                        # Schema change indicator
    "new_schema_details": json.dumps(new_schema_details), # Detailed schema information
    "new_data_schema_hash": new_data_schema_hash,         # Schema hash for comparison
    "source_medallion_layer": lineage_info['source_medallion_layer'],     # Source medallion layer (Bronze/Silver/Gold/External)
    "source_type": lineage_info['source_type'],                           # Source system type (Fabric Lakehouse, Oracle, etc.)
    "target_medallion_layer": lineage_info['target_medallion_layer'],     # Target medallion layer (Bronze/Silver/Gold)
    "target_type": lineage_info['target_type']                            # Target system type (Fabric Lakehouse/Warehouse)
})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
