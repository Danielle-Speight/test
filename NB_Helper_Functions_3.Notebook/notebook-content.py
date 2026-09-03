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

# # NB_Helper_Functions_3 - Configuration and Advanced Processing Utilities
# 
# ## Overview
# This notebook serves as the third module in the helper functions suite, focusing on configuration parsing, parameter management, and advanced data processing operations. It bridges the gap between metadata-driven configuration and runtime execution, ensuring seamless integration across the data platform.
# 
# ### Key Responsibilities
# - **Parameter Parsing**: Transforms JSON metadata into actionable Python variables
# - **Spark Configuration**: Optimizes Spark settings based on target layer requirements
# - **Advanced Processing**: Implements SCD2 operations, schema management, and entity resolution
# - **Validation Functions**: Ensures data integrity through schema comparison and metadata updates
# 
# ### Integration Context
# This notebook is typically executed after `NB_Helper_Functions_1` and `NB_Helper_Functions_2`, inheriting their context while adding specialized processing capabilities for complex data engineering scenarios.
# 
# ---
# 
# ## 1. Configuration Parameter Parsing
# 
# The following section parses all input parameters from the orchestration pipeline and transforms them into variables used throughout the data processing workflow.


# CELL ********************

# ===========================================================================================
# CONFIGURATION PARSING HELPER FUNCTIONS
# ===========================================================================================
# These atomic functions parse and validate configuration inputs, making them independently
# testable while maintaining backward compatibility with existing code.
# ===========================================================================================

def parse_json_metadata(
    orchestration_metadata_json: str,
    primary_config_json: str,
    advanced_config_json: str,
    table_ddl_json: str,
    latest_schema_details_json: str
) -> dict:
    """
    Parse all JSON metadata inputs into Python dictionaries.
    
    This function serves as the entry point for metadata parsing, converting
    JSON strings from the orchestration pipeline into structured dictionaries
    for downstream processing.
    
    Args:
        orchestration_metadata_json (str): JSON containing table ID and target details
        primary_config_json (str): JSON containing primary configuration settings
        advanced_config_json (str): JSON array of advanced configurations
        table_ddl_json (str): JSON array containing source DDL definitions
        latest_schema_details_json (str): JSON containing previous schema information
    
    Returns:
        dict: Dictionary containing all parsed metadata:
            - orchestration_metadata (dict): Orchestration context
            - primary_config (dict): Primary configuration
            - advanced_config (list): List of advanced config dictionaries
            - table_ddl (list): DDL definitions
            - latest_schema_details (dict): Schema tracking information
    
    Example:
        >>> metadata = parse_json_metadata(
        ...     orchestration_metadata_json='{"Table_ID": 123}',
        ...     primary_config_json='{"source_details_table_name": "dbo.sales"}',
        ...     advanced_config_json='[]',
        ...     table_ddl_json='[]',
        ...     latest_schema_details_json='{"Schema_ID": "abc123"}'
        ... )
        >>> print(metadata['orchestration_metadata']['Table_ID'])
        123
    """
    return {
        'orchestration_metadata': json.loads(orchestration_metadata_json),
        'primary_config': json.loads(primary_config_json),
        'advanced_config': json.loads(advanced_config_json),
        'table_ddl': json.loads(table_ddl_json),
        'latest_schema_details': json.loads(latest_schema_details_json)
    }


def clean_advanced_config(advanced_config: list) -> list:
    """
    Remove NoData placeholder rows from advanced configuration.
    
    Args:
        advanced_config (list): Raw advanced configuration from metadata
    
    Returns:
        list: Filtered configuration without NoData rows
    
    Example:
        >>> config = [
        ...     {'Configuration_Category': 'data_quality'},
        ...     {'Configuration_Category': 'NoData'},
        ...     {'Configuration_Category': 'data_transformation_steps'}
        ... ]
        >>> cleaned = clean_advanced_config(config)
        >>> len(cleaned)
        2
    """
    return [row for row in advanced_config if row.get('Configuration_Category') != 'NoData']


def normalize_schema_details(latest_schema_details: dict) -> dict:
    """
    Convert NoData schema placeholder to empty dictionary.
    
    When no schema has been logged for a table, the metadata returns a
    'NoData' placeholder. This function normalizes it to an empty dict
    for consistent downstream handling.
    
    Args:
        latest_schema_details (dict): Raw schema details from metadata
    
    Returns:
        dict: Normalized schema details (empty dict if NoData)
    
    Example:
        >>> schema = {'Schema_ID': 'NoData'}
        >>> normalized = normalize_schema_details(schema)
        >>> normalized
        {}
    """
    if latest_schema_details.get('Schema_ID') == 'NoData':
        return {}
    return latest_schema_details


def extract_schema_tracking_info(latest_schema_details: dict) -> dict:
    """
    Extract schema ID and parsed schema details for change detection.
    
    Args:
        latest_schema_details (dict): Normalized schema details dictionary
    
    Returns:
        dict: Dictionary containing:
            - last_schema_id: Previous schema identifier
            - last_schema_details: Parsed list of (column, type) tuples
    
    Example:
        >>> schema = {
        ...     'Schema_ID': 'abc123',
        ...     'Schema_Details': "[('id', 'int'), ('name', 'string')]"
        ... }
        >>> tracking = extract_schema_tracking_info(schema)
        >>> tracking['last_schema_id']
        'abc123'
        >>> len(tracking['last_schema_details'])
        2
    """
    return {
        'last_schema_id': latest_schema_details.get('Schema_ID'),
        'last_schema_details': ast.literal_eval(latest_schema_details.get('Schema_Details', "[]"))
    }

# ===========================================================================================
# SOURCE CONFIGURATION EXTRACTION
# ===========================================================================================
# Extract and validate source-related configurations including connection details,
# query specifications, and file paths for various ingestion patterns.

# ===========================================================================================
# DATASTORE CONFIGURATION LOOKUP
# ===========================================================================================
# These functions parse and lookup datastore configuration from the Datastore_Configuration
# table results. The pipeline passes lookup activity results as a string representation
# of an array of dicts.

def _parse_datastore_config(datastore_config: str | list) -> list:
    """
    Parse datastore configuration from pipeline lookup activity results.
    
    The pipeline passes the lookup activity result as JSON, like:
    '[{"Datastore_Name": "bronze", "Datastore_Type": "Lakehouse", ...}]'
    
    Args:
        datastore_config: Either a JSON string from the pipeline or an already-parsed list.
    
    Returns:
        list: Parsed list of datastore configuration dictionaries.
    
    Example:
        >>> config_str = '[{"Datastore_Name": "bronze", "Datastore_ID": "abc123"}]'
        >>> parsed = _parse_datastore_config(config_str)
        >>> parsed[0]['Datastore_Name']
        'bronze'
    """
    if isinstance(datastore_config, list):
        return datastore_config
    
    if isinstance(datastore_config, str):
        if not datastore_config.strip():
            return []
        try:
            return json.loads(datastore_config)
        except json.JSONDecodeError as e:
            raise ValueError(
                f"Failed to parse datastore_config as JSON. "
                f"Expected format: '[{{\"Datastore_Name\": \"bronze\", ...}}]'. Error: {e}"
            )
    
    raise TypeError(f"datastore_config must be str or list, got {type(datastore_config)}")


def _get_datastore_config(datastore_config: str | list, datastore_name: str, property_name: str) -> str:
    """
    Get a specific property from the datastore configuration for a given datastore.
    
    This function looks up the datastore configuration from the Datastore_Configuration
    table results (passed from pipeline lookup activity) and returns the requested property.
    
    Args:
        datastore_config: Lookup activity result - either a string like 
                         "[{'Datastore_Name': 'bronze', ...}]" or an already-parsed list.
        datastore_name: The name of the datastore to look up (case-insensitive).
        property_name: The property to retrieve. Valid values:
                      - 'Datastore_ID': The GUID of the datastore artifact
                      - 'Workspace_ID': The GUID of the Fabric workspace
                      - 'Workspace_Name': The name of the Fabric workspace
                      - 'Medallion_Layer': The medallion layer (bronze/silver/gold)
                      - 'Endpoint': The SQL endpoint for warehouses
                      - 'Connection_ID': The Fabric Connection ID for authentication
                      - 'Datastore_Type': The type (Lakehouse/Warehouse)
    
    Returns:
        str: The requested property value.
    
    Raises:
        KeyError: If the datastore is not found in the configuration.
        ValueError: If the datastore_config cannot be parsed.
    
    Example:
        >>> config_str = "[{'Datastore_Name': 'bronze', 'Datastore_ID': 'abc123', 'Workspace_Name': 'Dev'}]"
        >>> _get_datastore_config(config_str, 'bronze', 'Datastore_ID')
        'abc123'
        >>> _get_datastore_config(config_str, 'bronze', 'Workspace_Name')
        'Dev'
    """
    parsed_config = _parse_datastore_config(datastore_config)
    
    # Find the datastore by name (case-insensitive)
    datastore_name_lower = datastore_name.strip().lower()
    
    for datastore in parsed_config:
        if datastore.get('Datastore_Name', '').strip().lower() == datastore_name_lower:
            value = datastore.get(property_name, '')
            if value is None:
                value = ''
            return str(value).strip()
    
    # Datastore not found - provide helpful error message
    available_datastores = [d.get('Datastore_Name', 'unknown') for d in parsed_config]
    raise KeyError(
        f"Datastore '{datastore_name}' not found in Datastore_Configuration. "
        f"Available datastores: {available_datastores}. "
        f"Add '{datastore_name}' to the Datastore_Configuration table. "
        f"See docs/FAQ.md for setup instructions."
    )


# ===========================================================================================
# LAKEHOUSE MOUNTING UTILITIES FOR CUSTOM FUNCTIONS
# ===========================================================================================
# These functions provide local file system access to lakehouse files for custom ingestion
# functions that need to use libraries requiring local paths (e.g., zipfile, PIL, PyPDF2).
#
# Documentation: https://learn.microsoft.com/en-us/fabric/data-engineering/notebook-utilities
#
# WHEN TO USE:
# - Python open() - e.g., open(path, 'r')
# - zipfile.ZipFile() for archives
# - PIL/Pillow for image processing
# - PyPDF2 for PDF parsing
# - Any library using os.path or pathlib internally
#
# WHEN NOT NEEDED (ABFSS works directly):
# - Spark readers: spark.read.csv(), .json(), .parquet(), .text()
# - Pandas: pd.read_csv(), pd.read_parquet(), pd.read_json()
# - notebookutils.fs operations
# ===========================================================================================

def _mount_lakehouse_for_local_access(file_paths: list, table_id: int) -> tuple:
    """
    Mount a lakehouse to enable local file system access for custom file ingestion.
    
    This function extracts the lakehouse root from ABFSS paths and mounts it to a
    unique mount point based on table_id, preventing conflicts when multiple custom
    functions run in the same Spark session.
    
    Args:
        file_paths (list): List of ABFSS file paths to process. At least one path required.
                          Format: abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Files/...
        table_id (int): Unique table identifier used to create mount point name.
    
    Returns:
        tuple: (mount_point, local_mount_path, lakehouse_root)
            - mount_point (str): Mount point name (e.g., "/mount_table_123")
            - local_mount_path (str): Local file system path where lakehouse is mounted
            - lakehouse_root (str): ABFSS root of the lakehouse (for path conversion)
    
    Example:
        >>> file_paths = ["abfss://ws123@onelake.dfs.fabric.microsoft.com/lh456/Files/data/file.csv"]
        >>> mount_point, local_path, lh_root = _mount_lakehouse_for_local_access(file_paths, 100)
        >>> print(mount_point)
        '/mount_table_100'
        >>> # Later: convert paths and process files
        >>> _unmount_lakehouse(mount_point)  # Cleanup when done
    
    Note:
        Always call _unmount_lakehouse() when done to prevent mount point accumulation.
    """
    if not file_paths:
        raise ValueError("file_paths cannot be empty - at least one ABFSS path is required")
    
    # Extract lakehouse root from first file path
    # ABFSS format: abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Files/...
    first_file = file_paths[0]
    if '/Files/' not in first_file and '/Tables/' not in first_file:
        raise ValueError(
            f"Invalid ABFSS path format. Expected '/Files/' or '/Tables/' in path. Got: {first_file}"
        )
    
    # Split to get lakehouse root (everything before /Files/ or /Tables/)
    if '/Files/' in first_file:
        lakehouse_root = first_file.split('/Files/')[0]
    else:
        lakehouse_root = first_file.split('/Tables/')[0]
    
    # Create unique mount point name using table_id
    mount_point = f"/mount_table_{table_id}"
    
    # Check existing mounts to avoid duplicate mounting
    existing_mounts = [m.mountPoint for m in notebookutils.fs.mounts()]
    
    if mount_point not in existing_mounts:
        notebookutils.fs.mount(
            lakehouse_root,
            mount_point,
            {"fileCacheTimeout": 120}  # Cache timeout in seconds
        )
    
    # Get the local file system path for the mount
    local_mount_path = notebookutils.fs.getMountPath(mount_point)
    
    return mount_point, local_mount_path, lakehouse_root


def _unmount_lakehouse(mount_point: str) -> None:
    """
    Unmount a lakehouse mount point to clean up resources.
    
    Call this function after completing file processing to prevent accumulation
    of mount points during long-running Spark sessions.
    
    Args:
        mount_point (str): The mount point name to unmount (e.g., "/mount_table_123")
    
    Example:
        >>> _unmount_lakehouse("/mount_table_100")
    
    Note:
        Safe to call even if mount point doesn't exist - will silently succeed.
    """
    try:
        notebookutils.fs.unmount(mount_point)
    except Exception:
        # Silently ignore if mount point doesn't exist
        pass


def _convert_abfss_paths_to_local(file_paths: list, local_mount_path: str, lakehouse_root: str) -> list:
    """
    Convert a list of ABFSS paths to local mounted file system paths.
    
    Use this function after calling _mount_lakehouse_for_local_access() to convert
    ABFSS paths to local paths that can be used with standard Python file I/O.
    
    Args:
        file_paths (list): List of ABFSS file paths to convert
        local_mount_path (str): Local mount path from _mount_lakehouse_for_local_access()
        lakehouse_root (str): Lakehouse root from _mount_lakehouse_for_local_access()
    
    Returns:
        list: List of local file system paths corresponding to the input ABFSS paths
    
    Example:
        >>> file_paths = ["abfss://ws@onelake.../lh/Files/data/file.csv"]
        >>> mount_point, local_path, lh_root = _mount_lakehouse_for_local_access(file_paths, 100)
        >>> local_files = _convert_abfss_paths_to_local(file_paths, local_path, lh_root)
        >>> # local_files[0] is now like "/synfs/123/mount_table_100/Files/data/file.csv"
        >>> with open(local_files[0], 'r') as f:
        ...     content = f.read()
    """
    local_paths = []
    for abfss_path in file_paths:
        # Extract relative path after lakehouse root
        relative_path = abfss_path.replace(lakehouse_root, '')
        local_paths.append(local_mount_path + relative_path)
    return local_paths


def parse_source_configuration(
    orchestration_metadata: dict,
    primary_config: dict,
    datastore_config: str | list,
    folder_path_from_trigger: str
) -> dict:
    """
    Extract and validate source-related configurations.
    
    This function parses all source-related settings including table identifiers,
    file paths, custom functions, and query generation logic. It handles multiple
    ingestion patterns: staging folders, wildcard paths, trigger-based paths, and
    direct table queries.
    
    Args:
        orchestration_metadata (dict): Orchestration context with table and target info
        primary_config (dict): Primary configuration dictionary
        datastore_config (str | list): Datastore configuration from Datastore_Configuration table.
                                       Either a string like "[{'Datastore_Name': 'bronze', ...}]" 
                                       or an already-parsed list.
        folder_path_from_trigger (str): Optional folder path from event trigger
    
    Returns:
        dict: Parsed source configuration containing:
            - table_id (int): Unique table identifier
            - target_datastore_name (str): Target lakehouse name (lowercase)
            - target_datastore_medallion_name (str): Medallion layer name
            - input_delta_table_external_location (str): External Delta table path
            - staging_folder_path (str): Staging folder path if specified
            - wildcard_folder_path (str): Wildcard folder path if specified
            - source_path (str): Resolved source path (first non-empty)
            - using_source_folder_path (bool): Whether using folder-based ingestion
            - custom_table_ingestion_function (str): Custom function name for table/SQL sources
            - custom_table_ingestion_function_notebook (str): Custom notebook name for table/SQL sources
            - custom_file_ingestion_function (str): Custom function name for file sources
            - custom_file_ingestion_function_notebook (str): Custom notebook name for file sources
            - source_table_name (str): Source table name (may be modified)
            - watermark_table_name (str): Table for watermark tracking
            - source_datastore_name (str): Source datastore name
            - source_workspace_name (str): Source workspace name (if applicable)
            - source_query (str): Generated SELECT query (empty for file ingestion)
            - connection_id (str): External connection identifier
            - source_category (str): Source type (lowercase)
            - file_extension (str): File format (lowercase)
            - expected_schema (str): Expected schema definition for contract validation
    
    Example:
        >>> config = parse_source_configuration(
        ...     orchestration_metadata={'Table_ID': 123, 'Target_Datastore': 'Silver'},
        ...     primary_config={'source_details_table_name': 'bronze.dbo.sales'},
        ...     datastore_config="[{'Datastore_Name': 'bronze', 'Workspace_Name': 'Dev'}]",
        ...     folder_path_from_trigger=''
        ... )
        >>> config['table_id']
        123
    """
    table_id = orchestration_metadata.get('Table_ID')

    input_delta_table_external_location = primary_config.get("source_details_input_delta_table_external_location", '').strip()
    
    # File path configurations
    staging_folder_path = primary_config.get("source_details_staging_folder_path")
    wildcard_folder_path = primary_config.get("source_details_wildcard_folder_path")
    source_path = staging_folder_path or wildcard_folder_path or folder_path_from_trigger
    using_source_folder_path = bool(source_path)
    
    # Custom function configurations - now explicit for table vs file sources
    custom_table_ingestion_function = primary_config.get("source_details_custom_table_ingestion_function", '')
    custom_table_ingestion_function_notebook = primary_config.get("source_details_custom_table_ingestion_function_notebook", '')
    custom_file_ingestion_function = primary_config.get("source_details_custom_file_ingestion_function", '')
    custom_file_ingestion_function_notebook = primary_config.get("source_details_custom_file_ingestion_function_notebook", '')
    
    # Determine source datastore and build query based on ingestion pattern
    source_workspace_name = None
    if staging_folder_path:
        source_datastore_name = primary_config.get("source_details_staging_lakehouse_name").strip().lower()
        source_query = ""
        # Staging uses folder timestamps, not table watermarks
        watermark_table_name = ""
    elif using_source_folder_path or custom_table_ingestion_function or custom_file_ingestion_function:
        source_datastore_name = primary_config.get("source_details_datastore_name", "bronze").strip().lower()
        source_query = ""
        watermark_table_name = primary_config.get("watermark_details_table_name", "")
    else:
        # Table-based ingestion: parse qualified table name and build query
        source_table_name = primary_config.get("source_details_table_name")
        source_datastore_name = source_table_name.split('.')[0].strip().lower()
        source_workspace_name = _get_datastore_config(datastore_config, source_datastore_name, 'Workspace_Name')
        source_query = f"SELECT * FROM `{source_workspace_name}`.{source_table_name.lower()}"
        watermark_table_name = primary_config.get("watermark_details_table_name", source_table_name)
    
    # Resolve watermark table to fully-qualified name (shared logic for all patterns)
    if watermark_table_name:
        watermark_datastore_name = watermark_table_name.split('.')[0].strip().lower()
        watermark_workspace_name = _get_datastore_config(datastore_config, watermark_datastore_name, 'Workspace_Name')
        watermark_full_table_name = f"`{watermark_workspace_name}`.{watermark_table_name.lower()}"
    else:
        watermark_full_table_name = ""
    
    # External source configuration
    connection_id = primary_config.get("source_details_connection_id", '').strip()
    source_category = primary_config.get("source_details_source", "").lower()
    file_extension = primary_config.get("source_details_file_extension", "").lower()
    
    # Expected schema for schema contract validation (works for both files and Delta tables)
    expected_schema = primary_config.get("source_details_schema", "").strip()
    
    return {
        'table_id': table_id,
        'input_delta_table_external_location': input_delta_table_external_location,
        'staging_folder_path': staging_folder_path,
        'wildcard_folder_path': wildcard_folder_path,
        'source_path': source_path,
        'using_source_folder_path': using_source_folder_path,
        'custom_table_ingestion_function': custom_table_ingestion_function,
        'custom_table_ingestion_function_notebook': custom_table_ingestion_function_notebook,
        'custom_file_ingestion_function': custom_file_ingestion_function,
        'custom_file_ingestion_function_notebook': custom_file_ingestion_function_notebook,
        'watermark_table_name': watermark_full_table_name,
        'source_datastore_name': source_datastore_name,
        'source_workspace_name': source_workspace_name,
        'source_query': source_query,
        'connection_id': connection_id,
        'source_category': source_category,
        'file_extension': file_extension,
        'expected_schema': expected_schema
    }

# ===========================================================================================
# LAKEHOUSE-SPECIFIC CONFIGURATION
# ===========================================================================================
# Set layer-specific defaults and extract workspace/lakehouse identifiers for each medallion layer.

SUPPORTED_MEDALLION_LAYERS = ('bronze', 'silver', 'gold')

def determine_medallion_layer_defaults(target_datastore_medallion_name: str) -> dict:
    """
    Set layer-specific defaults based on medallion architecture layer.
    
    Each layer (Bronze, Silver, Gold) has different optimization characteristics
    and default behaviors tailored to its role in the data pipeline.
    
    Args:
        target_datastore_medallion_name (str): Medallion layer name ('bronze', 'silver', 'gold')
    
    Returns:
        dict: Layer-specific defaults containing:
            - default_watermark_column_name (str): Default column for incremental processing
            - default_merge_type (str): Default merge strategy for the layer
    
    Layer Characteristics:
        - Bronze: Raw ingestion, append-only by default, no watermark
        - Silver: Curated data, merge by default, modified_datetime watermark
        - Gold: Analytics-ready, merge by default, modified_datetime watermark
    
    Example:
        >>> defaults = determine_medallion_layer_defaults('silver')
        >>> defaults['default_merge_type']
        'merge'
        >>> defaults['default_watermark_column_name']
        'delta__modified_datetime'
    """
    if target_datastore_medallion_name == 'gold':
        # Gold layer defaults - optimized for analytics workloads
        result = {
            'default_watermark_column_name': "delta__modified_datetime",
            'default_merge_type': "merge"
        }
    elif target_datastore_medallion_name == 'silver':
        # Silver layer defaults - balanced for transformation and storage
        result = {
            'default_watermark_column_name': "delta__modified_datetime",
            'default_merge_type': "merge"
        }
    elif target_datastore_medallion_name == 'bronze':
        # Bronze layer defaults - optimized for fast ingestion
        result = {
            'default_watermark_column_name': "",
            'default_merge_type': "append"
        }
    else:
        allowed_layers = ", ".join(f"'{layer}'" for layer in SUPPORTED_MEDALLION_LAYERS)
        error_message = (
            f"Unsupported target_datastore_medallion_name '{target_datastore_medallion_name}'. "
            f"Expected one of [{allowed_layers}]."
        )
        log_and_print(error_message)
        raise ValueError(error_message)
    
    medallion_log_info = f"The target_datastore_medallion_name is {target_datastore_medallion_name}. The default merge type is now set to '{result['default_merge_type']}' and default watermark column is now set to '{result['default_watermark_column_name']}'."
    log_and_print(medallion_log_info)
    
    return result

# ===========================================================================================
# TARGET CONFIGURATION EXTRACTION
# ===========================================================================================
# Define target destination details including lakehouse, table/file paths, and storage options.

def parse_target_configuration(
    orchestration_metadata: dict,
    primary_config: dict,
    datastore_config: str | list
) -> dict:
    """
    Parse target destination configuration including paths and settings.
    
    This function determines whether the target is a table or file, constructs
    the appropriate ABFSS paths, and extracts related configuration settings.
    
    Args:
        orchestration_metadata (dict): Orchestration context
        primary_config (dict): Primary configuration dictionary
        datastore_config (str | list): Datastore configuration from Datastore_Configuration table.
                                       Either a string like "[{'Datastore_Name': 'bronze', ...}]" 
                                       or an already-parsed list.
    
    Returns:
        dict: Parsed target configuration containing:
            - target_workspace_name (str): Fabric workspace name
            - target_datastore_id (str): Datastore GUID
            - target_datastore_workspace_id (str): Workspace GUID
            - target_table_name (str): Table name (empty for file targets)
            - target_folder_path (str): Folder path (only for file targets)
            - target_abfss_path (str): Full ABFSS path to target
            - default_merge_type (str): Updated merge type (may be 'output_file')
            - output_external_location (str): External storage path
            - enforce_not_null (bool): Whether to enforce NOT NULL constraints
            - schema_name_for_path (str): Schema name for path construction
            - table_name_for_path (str): Table name for path construction
    
    Example:
        >>> config = parse_target_configuration(
        ...     orchestration_metadata={'Target_Entity': 'dbo.customers', 'Target_Datastore': 'silver'},
        ...     primary_config={},
        ...     datastore_config="[{'Datastore_Name': 'silver', 'Workspace_Name': 'Analytics', 'Medallion_Layer': 'silver', 'Datastore_ID': 'abc', 'Workspace_ID': 'xyz'}]"
        ... )
        >>> config['target_workspace_name']
        'Analytics'
    """
    target_datastore_name = orchestration_metadata.get('Target_Datastore').strip().lower()
    
    target_datastore_medallion_name = _get_datastore_config(datastore_config, target_datastore_name, 'Medallion_Layer').lower().strip()

    # Determine layer-specific defaults
    layer_defaults = determine_medallion_layer_defaults(target_datastore_medallion_name)

    default_watermark_column_name = layer_defaults['default_watermark_column_name']
    default_merge_type = layer_defaults['default_merge_type']
    
    target_workspace_name = _get_datastore_config(datastore_config, target_datastore_name, 'Workspace_Name')
    target_datastore_id = _get_datastore_config(datastore_config, target_datastore_name, 'Datastore_ID')
    target_datastore_workspace_id = _get_datastore_config(datastore_config, target_datastore_name, 'Workspace_ID')
    
    target_entity = orchestration_metadata.get('Target_Entity')
    
    # Determine if target is a table or file based on path pattern
    if '/' in target_entity:
        # File-based output
        log_and_print(f"Target_Entity has a / and is assumed to be a folder path. The default merge type is now set to 'output_file'.")
        full_target_table_name = ""
        target_folder_path = target_entity
        target_abfss_path = f"abfss://{target_datastore_workspace_id}@onelake.dfs.fabric.microsoft.com/{target_datastore_id}/Files/{target_folder_path}"
        default_merge_type = "output_file"
        schema_name_for_path = None
        table_name_for_path = None
        target_quarantined_abfss_path = None
    else:
        # Table-based output
        target_table_name = target_entity
        target_folder_path = None
        schema_name_for_path = target_table_name.split('.')[0].lower()
        table_name_for_path = target_table_name.split('.')[1].lower()
        target_abfss_path = f"abfss://{target_datastore_workspace_id}@onelake.dfs.fabric.microsoft.com/{target_datastore_id}/Tables/{schema_name_for_path}/{table_name_for_path}"
        quarantine_table_name = primary_config.get("target_details_quarantine_table_name", f"{target_table_name}_quarantined")
        quarantine_schema_name_for_path = quarantine_table_name.split('.')[0].lower()
        quarantine_table_name_for_path = quarantine_table_name.split('.')[1].lower()
        target_quarantined_abfss_path = f"abfss://{target_datastore_workspace_id}@onelake.dfs.fabric.microsoft.com/{target_datastore_id}/Tables/{quarantine_schema_name_for_path}/{quarantine_table_name_for_path}"

        full_target_table_name = f"`{target_workspace_name}`.{target_datastore_name.lower()}.{target_table_name.lower()}"

    # Additional target settings
    output_external_location = primary_config.get("target_details_external_location", '').strip()
    enforce_not_null = primary_config.get("target_details_enforce_not_null", "false").strip().lower() == "true"        
    log_and_print(f"Target ABFSS Path: {target_abfss_path}")

    merge_type = primary_config.get("target_details_merge_type", "").strip().lower()

    lakehouse_table_output = bool(full_target_table_name) and merge_type != 'warehouse_spark_connector'

    replace_where_column = primary_config.get("target_details_warehouse_write_mode_replace_where_column", "").strip()
    warehouse_write_mode = primary_config.get('target_details_warehouse_write_mode', '').lower().strip()
    target_excel_sheet_name = primary_config.get('target_details_sheet_name', 'Sheet1').strip()
    target_output_delimiter = primary_config.get('target_details_delimiter', '').strip()

    return {
        'target_workspace_name': target_workspace_name,
        'target_datastore_id': target_datastore_id,
        'target_datastore_workspace_id': target_datastore_workspace_id,
        'target_table_name': full_target_table_name,
        'target_folder_path': target_folder_path,
        'target_abfss_path': target_abfss_path,
        'default_merge_type': default_merge_type,
        'output_external_location': output_external_location,
        'enforce_not_null': enforce_not_null,
        'schema_name_for_path': schema_name_for_path,
        'table_name_for_path': table_name_for_path,
        'target_quarantined_abfss_path': target_quarantined_abfss_path,
        'lakehouse_table_output': lakehouse_table_output,
        'replace_where_column': replace_where_column,
        'target_datastore_name': target_datastore_name,
        'target_datastore_medallion_name': target_datastore_medallion_name,
        'warehouse_write_mode': warehouse_write_mode,
        'target_excel_sheet_name': target_excel_sheet_name,
        'target_output_delimiter': target_output_delimiter,
        'default_watermark_column_name': default_watermark_column_name
    }

# ===========================================================================================
# FILE INGESTION PATHS
# ===========================================================================================
# Configure paths for source files and staging areas used during file-based ingestion.

def parse_file_ingestion_paths(
    datastore_config: str | list,
    source_datastore_name: str,
    target_datastore_workspace_id: str,
    target_datastore_id: str,
    table_id: int,
    wildcard_folder_path: str,
    primary_config: dict
) -> dict:
    """
    Parse and construct paths for file-based ingestion and staging.
    
    This function builds ABFSS paths for source file locations and staging areas.
    It also determines if temporary file cleanup is needed based on file extensions.
    
    Args:
        datastore_config (str | list): Datastore configuration from Datastore_Configuration table.
                                       Either a string like "[{'Datastore_Name': 'bronze', ...}]" 
                                       or an already-parsed list.
        source_datastore_name (str): Source datastore name
        target_datastore_workspace_id (str): Target workspace GUID
        target_datastore_id (str): Target datastore GUID
        table_id (int): Unique table identifier
        wildcard_folder_path (str): Wildcard folder path if specified
        primary_config (dict): Primary configuration (will be mutated with file_staging_path)
    
    Returns:
        dict: Dictionary containing:
            - source_files_datastore_id (str): Source datastore GUID
            - source_files_datastore_workspace_id (str): Source workspace GUID
            - source_files_lakehouse_path (str): ABFSS path to source files
            - file_staging_path (str): ABFSS path for temporary staging
            - clean_up_temporary_path (bool): Whether to cleanup temp files
    
    Side Effects:
        Mutates primary_config dict by adding 'file_staging_path' key
    
    Example:
        >>> paths = parse_file_ingestion_paths(
        ...     datastore_config="[{'Datastore_Name': 'bronze', 'Datastore_ID': 'lh-123', 'Workspace_ID': 'ws-456'}]",
        ...     source_datastore_name='bronze',
        ...     target_datastore_workspace_id='ws-456',
        ...     target_datastore_id='lh-789',
        ...     table_id=100,
        ...     wildcard_folder_path='',
        ...     primary_config={}
        ... )
        >>> paths['source_files_datastore_id']
        'lh-123'
    """
    # Determine source lakehouse IDs for file-based ingestion
    source_files_datastore_id = _get_datastore_config(datastore_config, source_datastore_name, 'Datastore_ID')
    source_files_datastore_workspace_id = _get_datastore_config(datastore_config, source_datastore_name, 'Workspace_ID')
    source_files_lakehouse_path = f"abfss://{source_files_datastore_workspace_id}@onelake.dfs.fabric.microsoft.com/{source_files_datastore_id}/Files"
    
    # Create staging path for temporary file processing
    file_staging_path = f"abfss://{target_datastore_workspace_id}@onelake.dfs.fabric.microsoft.com/{target_datastore_id}/Files/staging_for_file_ingestion/{table_id}"
    
    # Mutate primary_config to add staging path (maintains backward compatibility)
    primary_config['file_staging_path'] = file_staging_path
    
    # Determine if temporary file cleanup is needed (Excel/XML files)
    clean_up_temporary_path = bool(
        wildcard_folder_path and 
        wildcard_folder_path.lower().endswith(('.xml', '.xls', '.xlsx'))
    )
    
    return {
        'source_files_datastore_id': source_files_datastore_id,
        'source_files_datastore_workspace_id': source_files_datastore_workspace_id,
        'source_files_lakehouse_path': source_files_lakehouse_path,
        'file_staging_path': file_staging_path,
        'clean_up_temporary_path': clean_up_temporary_path
    }

# ===========================================================================================
# LINEAGE INFORMATION EXTRACTION
# ===========================================================================================
# Derive source and target lineage details (medallion layer and system type) for logging.

def extract_lineage_information(
    source_config: dict,
    target_config: dict,
    datastore_config: str | list,
    merge_type: str
) -> dict:
    """
    Extract lineage information for logging and tracking.
    
    This function derives the source and target medallion layers and system types
    based on the configuration. This information is used for:
    - Data lineage tracking in logs
    - Understanding data flow through medallion architecture
    - Identifying external vs internal data sources
    
    Args:
        source_config (dict): Source configuration from parse_source_configuration()
        target_config (dict): Target configuration from parse_target_configuration()
        datastore_config (str | list): Datastore configuration from Datastore_Configuration table.
                                       Either a string like "[{'Datastore_Name': 'bronze', ...}]" 
                                       or an already-parsed list.
        merge_type (str): The merge type being used (affects target type determination)
    
    Returns:
        dict: Lineage information containing:
            - source_medallion_layer (str): Source medallion layer (Bronze/Silver/Gold)
            - source_type (str): Source system type (Fabric Lakehouse (Files) or Fabric Lakehouse (Tables))
            - target_medallion_layer (str): Target medallion layer (Bronze/Silver/Gold)
            - target_type (str): Target system type (Fabric Lakehouse or Fabric Warehouse)
    
    Source Type Logic:
        - If using_source_folder_path is True → Fabric Lakehouse (Files)
        - Otherwise → Fabric Lakehouse (Tables)
    
    Source Medallion Layer Logic:
        - Lookup from datastore_config based on source_datastore_name
        - source_datastore_name is already set to staging_lakehouse_name for external DB ingestion
    
    Target Type Logic:
        - If merge_type == 'warehouse_spark_connector' → Fabric Warehouse
        - Else → Fabric Lakehouse
    
    Example:
        >>> lineage = extract_lineage_information(
        ...     source_config={'source_datastore_name': 'bronze', 'using_source_folder_path': True},
        ...     target_config={'target_datastore_medallion_name': 'silver'},
        ...     datastore_config="[{'Datastore_Name': 'bronze', 'Medallion_Layer': 'bronze'}]",
        ...     merge_type='merge'
        ... )
        >>> lineage['source_medallion_layer']
        'Bronze'
        >>> lineage['source_type']
        'Fabric Lakehouse (Files)'
    """
    source_datastore_name = source_config.get('source_datastore_name', '').strip().lower()
    staging_folder_path = source_config.get('staging_folder_path', '')
    using_source_folder_path = source_config.get('using_source_folder_path', False)
    
    # Determine Source Type and Medallion Layer
    # Note: External database sources are handled by pipelines before this notebook runs,
    # so by the time we're here, sources are either Delta tables or Files in a Fabric Lakehouse
    
    if staging_folder_path:
        # External DB staged to lakehouse Files - source_datastore_name is already set to staging_lakehouse_name
        source_type = 'Fabric Lakehouse (Files)'
        source_medallion_layer = _get_datastore_config(datastore_config, source_datastore_name, 'Medallion_Layer').strip().title()
    elif using_source_folder_path:
        # File-based ingestion from lakehouse Files section - source_datastore_name is set to datastore_name
        source_type = 'Fabric Lakehouse (Files)'
        source_medallion_layer = _get_datastore_config(datastore_config, source_datastore_name, 'Medallion_Layer').strip().title()
    else:
        # Delta table source (Fabric Lakehouse Tables section)
        source_type = 'Fabric Lakehouse (Tables)'
        source_medallion_layer = _get_datastore_config(datastore_config, source_datastore_name, 'Medallion_Layer').strip().title()

    # Determine Target Medallion Layer (already parsed in target_config)
    target_medallion_layer = target_config.get('target_datastore_medallion_name', '').strip().title()
    
    # Determine Target Type
    if merge_type == 'warehouse_spark_connector':
        target_type = 'Fabric Warehouse'
    else:
        target_type = 'Fabric Lakehouse'
    
    lineage_info = {
        'source_medallion_layer': source_medallion_layer,
        'source_type': source_type,
        'target_medallion_layer': target_medallion_layer,
        'target_type': target_type
    }
    
    log_and_print(f"Lineage Information - Source: {source_medallion_layer} ({source_type}) → Target: {target_medallion_layer} ({target_type})")
    
    return lineage_info

# ===========================================================================================
# WATERMARK AND INCREMENTAL PROCESSING CONFIGURATION
# ===========================================================================================

def parse_watermark_configuration(
    primary_config: dict,
    default_merge_type: str,
    default_watermark_column_name: str,
    staging_folder_path: str,
    using_source_folder_path: bool,
    watermark_value: str
) -> dict:
    """
    Parse watermark and incremental processing configuration.
    
    This function handles complex logic for watermark columns, merge strategies,
    and incremental processing settings. It considers soft deletes, batch processing,
    Change Data Feed (CDF), and layer-specific defaults.
    
    Args:
        primary_config (dict): Primary configuration dictionary
        default_merge_type (str): Default merge type from medallion layer
        default_watermark_column_name (str): Default watermark column from layer
        staging_folder_path (str): Staging folder path if specified
        using_source_folder_path (bool): Whether using folder-based ingestion
        watermark_value (str): Current watermark value
    
    Returns:
        dict: Dictionary containing:
            - column_to_mark_source_data_deletion (str): Soft delete tracking column
            - delete_rows_with_value (str): Value indicating deletion
            - merge_type (str): Final merge type (may be overridden)
            - default_merge_type (str): Updated default (may be 'merge_and_delete')
            - merge_in_batches_with_columns (list): Columns for batch processing
            - watermark_column_data_type (str): Watermark column data type
            - watermark_column_name (str): Watermark column name
            - use_watermark_column (bool): Whether to use watermark filtering
            - use_change_data_feed (bool): Whether to use Change Data Feed for incremental loading
            - watermark_value (str): Updated watermark value (may be reset)
    
    Example:
        >>> config = parse_watermark_configuration(
        ...     primary_config={'watermark_details_column_name': 'modified_date'},
        ...     default_merge_type='merge',
        ...     default_watermark_column_name='delta__modified_datetime',
        ...     staging_folder_path='',
        ...     using_source_folder_path=False,
        ...     watermark_value='2024-01-01'
        ... )
        >>> config['use_watermark_column']
        True
    """
    # Change Data Feed configuration
    use_change_data_feed = primary_config.get("watermark_details_use_change_data_feed", "false").strip().lower() == "true"
    
    # Soft delete tracking
    column_to_mark_source_data_deletion = primary_config.get(
        "target_details_column_to_mark_source_data_deletion", ""
    ).strip()
    delete_rows_with_value = primary_config.get(
        "target_details_delete_rows_with_value", ""
    ).strip()
    
    # Override soft delete settings if Change Data Feed is enabled
    if use_change_data_feed:
        # CDF provides _change_type column with 'delete' value for deleted records
        column_to_mark_source_data_deletion = "_change_type"
        delete_rows_with_value = "delete"
        log_and_print("Change Data Feed is enabled. Soft delete tracking set to use _change_type = 'delete' for handling deleted records.")
    
    # Override merge type if soft deletes are configured
    if delete_rows_with_value:
        default_merge_type = "merge_and_delete"
        log_and_print(f"target_details_delete_rows_with_value has a provided value from the metadata. The default value for merge type is now set to 'merge_and_delete'.")
    
    # Get final merge type
    merge_type = primary_config.get("target_details_merge_type", default_merge_type).strip().lower()
    
    # Parse batch merge columns
    merge_in_batches_with_columns = primary_config.get("target_details_merge_in_batches_with_columns", "")
    merge_in_batches_with_columns = [col.strip() for col in merge_in_batches_with_columns.split(",") if col.strip()]
    
    # Watermark column configuration
    watermark_column_data_type = primary_config.get("watermark_details_data_type", "datetime").strip()
    watermark_column_name = primary_config.get("watermark_details_column_name", default_watermark_column_name)
    
    # Override watermark for staging folder ingestion
    if staging_folder_path:
        watermark_column_name = ""
        watermark_column_data_type = ""
        log_and_print("Setting watermark_details_column_name and watermark_column_data_type to '' because staging_folder_path is being used. Last modified folder timestamps will be used for watermarking.")
    
    # Override watermark settings if Change Data Feed is enabled
    if use_change_data_feed:
        # CDF uses _commit_timestamp for watermarking automatically
        watermark_column_name = ""
        watermark_column_data_type = ""
        log_and_print("Change Data Feed is enabled. Watermark column settings are not used - CDF uses _commit_version for incremental processing.")
    
    # Default to true - if user doesn't want watermarking, they set it explicitly to false
    use_watermark_column_default = "true"
    
    # Get final use_watermark_column value
    use_watermark_column = primary_config.get(
        "watermark_details_use_watermark_column", use_watermark_column_default
    ).strip().lower() == 'true'
    
    # Log the watermark setting
    if use_watermark_column:
        log_and_print(f"Watermark filtering enabled. Current watermark_value: '{watermark_value}'")
    else:
        log_and_print(f"Watermark filtering disabled (explicitly set to false). Clearing watermark_value.")
        watermark_value = ""
    
    # Reset watermark for file-based ingestion with default 1900-01-01 value
    # Empty string works for both base path (skips int comparison) and wildcard path (skips timestamp filter)
    if using_source_folder_path and watermark_value and '1900-01-01' in watermark_value:
        watermark_value = ""
        log_and_print(f"File-based ingestion with default 1900-01-01 watermark detected. Clearing watermark_value to process all files/folders.")
    
    return {
        'column_to_mark_source_data_deletion': column_to_mark_source_data_deletion,
        'delete_rows_with_value': delete_rows_with_value,
        'merge_type': merge_type,
        'default_merge_type': default_merge_type,
        'merge_in_batches_with_columns': merge_in_batches_with_columns,
        'watermark_column_data_type': watermark_column_data_type,
        'watermark_column_name': watermark_column_name,
        'use_watermark_column': use_watermark_column,
        'use_change_data_feed': use_change_data_feed,
        'watermark_value': watermark_value
    }

# ===========================================================================================
# ADVANCED PROCESSING CONFIGURATION
# ===========================================================================================

def parse_advanced_processing_configuration(
    primary_config: dict,
    using_source_folder_path: bool,
    target_datastore_medallion_name: str
) -> dict:
    """
    Parse advanced processing settings for data quality and schema management.
    
    This function extracts configuration for:
    - Liquid clustering columns for performance optimization
    - Schema evolution failure settings
    - Duplicate primary key handling
    - Column name standardization settings (independent boolean flags)
    
    Args:
        primary_config (dict): Primary configuration dictionary
        using_source_folder_path (bool): if source is a folder path, not a table
        target_datastore_medallion_name (str): bronze, silver, or gold
    
    Returns:
        dict: Dictionary containing:
            - liquid_clustering_columns (list): Columns for liquid clustering
            - fail_on_new_schema (bool): Fail when new columns detected
            - fail_on_column_data_type_change (bool): Fail on type changes
            - if_duplicate_primary_keys (str): Action for duplicate keys
            - trim (bool): Trim leading/trailing whitespace from column names
            - apply_case (str): Convert column names case ('lower', 'upper', 'title')
            - replace_non_alphanumeric_with_underscore (bool): Replace non-alphanumeric characters with underscores in column names
            - regex_find (str): Regex pattern to find in column names
            - regex_replace (str): Replacement value for regex matches
            - exact_find (str): Comma-separated exact strings to find
            - exact_replace (str): Single replacement value for exact matches
            - trim_data_in_string_columns (str): Comma-separated list of columns to trim whitespace, or "*" for all string columns
            - replace_blank_with_null_in_string_columns (str): Comma-separated list of columns to replace blanks with null, or "*" for all string columns
    """
    # Liquid clustering for performance
    liquid_clustering_columns = primary_config.get("target_details_liquid_clustering_columns", "")
    
    liquid_clustering_columns = [col.strip() for col in liquid_clustering_columns.split(",") if col.strip()]
    
    # Schema evolution settings
    fail_on_new_schema = primary_config.get(
        "target_details_fail_on_new_schema", "false"
    ).strip().lower() == "true"
    
    fail_on_column_data_type_change = primary_config.get(
        "target_details_fail_on_column_data_type_change", "false"
    ).strip().lower() == "true"
    
    # Duplicate primary key handling
    if_duplicate_primary_keys = primary_config.get(
        "target_details_if_duplicate_primary_keys", ""
    ).strip().lower()

    # Column name standardization - no magic defaults, user must explicitly configure
    trim_column_names = primary_config.get(
        "column_cleansing_trim", "false"
    ).strip().lower() == 'true'
    
    apply_case_column_names = primary_config.get(
        "column_cleansing_apply_case", ""
    ).strip().lower()
    
    replace_non_alphanumeric_chars_with_underscore_in_column_names = primary_config.get(
        "column_cleansing_replace_non_alphanumeric_with_underscore", "false"
    ).strip().lower() == 'true'
    
    # Regex patterns: Users provide standard regex syntax in SQL metadata (e.g., '\d+', '[0-9]+$')
    # No special escaping needed - SQL string literals preserve backslashes correctly
    regex_find_in_column_names = primary_config.get(
        "column_cleansing_regex_find", ""
    ).strip()
    
    regex_replace_in_column_names = primary_config.get(
        "column_cleansing_regex_replace", ""
    ).strip()
    
    exact_find_in_column_names = primary_config.get(
        "column_cleansing_exact_find", ""
    ).strip()
    
    exact_replace_in_column_names = primary_config.get(
        "column_cleansing_exact_replace", ""
    ).strip()

    # Data cleansing - no magic defaults, user must explicitly configure
    trim_data_in_string_columns = primary_config.get("data_cleansing_trim_data_in_string_columns", "").strip()
    replace_blank_with_null_in_string_columns = primary_config.get("data_cleansing_replace_blank_with_null_in_string_columns", "").strip()

    return {
        'liquid_clustering_columns': liquid_clustering_columns,
        'fail_on_new_schema': fail_on_new_schema,
        'fail_on_column_data_type_change': fail_on_column_data_type_change,
        'if_duplicate_primary_keys': if_duplicate_primary_keys,
        'trim_column_names': trim_column_names,
        'apply_case_column_names': apply_case_column_names,
        'replace_non_alphanumeric_chars_with_underscore_in_column_names': replace_non_alphanumeric_chars_with_underscore_in_column_names,
        'regex_find_in_column_names': regex_find_in_column_names,
        'regex_replace_in_column_names': regex_replace_in_column_names,
        'exact_find_in_column_names': exact_find_in_column_names,
        'exact_replace_in_column_names': exact_replace_in_column_names,
        'trim_data_in_string_columns': trim_data_in_string_columns,
        'replace_blank_with_null_in_string_columns': replace_blank_with_null_in_string_columns
    }
    
# ===========================================================================================
# PRIMARY KEY CONFIGURATION
# ===========================================================================================

def parse_primary_key_configuration(orchestration_metadata: dict) -> dict:
    """
    Parse primary key configuration from orchestration metadata.
    
    Extracts and processes the primary key column names, handling
    comma-separated lists and whitespace trimming.
    
    Args:
        orchestration_metadata (dict): Orchestration metadata dictionary
    
    Returns:
        dict: Dictionary containing:
            - primary_keys (str): Comma-separated list of primary key column names
    
    Example:
        >>> config = parse_primary_key_configuration(
        ...     orchestration_metadata={'Primary_Keys': 'id, customer_id, order_id'}
        ... )
        >>> config['primary_keys']
        ['id', 'customer_id', 'order_id']
    """
    primary_keys_raw = orchestration_metadata.get("Primary_Keys")

    primary_keys = [key.strip() for key in primary_keys_raw.split(",")] if primary_keys_raw else []
    
    return {
        'primary_keys': primary_keys
    }

# ===========================================================================================
# ADVANCED CONFIGURATION STEPS (DATA QUALITY & TRANSFORMATION)
# ===========================================================================================

def parse_advanced_configuration_steps(advanced_config: list) -> dict:
    """
    Parse advanced configuration steps for data quality and transformation.
    
    Extracts and parses JSON configuration for:
    - Data quality validation steps
    - Data transformation steps
    
    Args:
        advanced_config (list): List of advanced configuration dictionaries
    
    Returns:
        dict: Dictionary containing:
            - data_quality_steps (list): Parsed data quality configurations
            - data_transformation_steps (list): Parsed transformation configurations
    
    Example:
        >>> config = parse_advanced_configuration_steps(
        ...     advanced_config=[
        ...         {'Configuration_Category': 'data_quality', 'advanced_settings': '{"check": "not_null"}'},
        ...         {'Configuration_Category': 'data_transformation_steps', 'advanced_settings': '{"rename": "col1"}'}
        ...     ]
        ... )
        >>> len(config['data_quality_steps'])
        1
    """
    if not advanced_config:
        return {
            'data_quality_steps': [],
            'data_transformation_steps': []
        }
    
    # Parse data quality steps
    data_quality_steps = [
        json.loads(values.get('advanced_settings'))
        for values in advanced_config
        if values.get('Configuration_Category') == 'data_quality'
    ]
    
    # Parse data transformation steps
    data_transformation_steps = [
        json.loads(values.get('advanced_settings'))
        for values in advanced_config
        if values.get('Configuration_Category') == 'data_transformation_steps'
    ]
    
    return {
        'data_quality_steps': data_quality_steps,
        'data_transformation_steps': data_transformation_steps
    }

# ===========================================================================================
# DIMENSION TABLE AND SCD CONFIGURATION
# ===========================================================================================

def parse_dimension_table_configuration(
    primary_config: dict,
    advanced_config: list
) -> dict:
    """
    Parse dimension table configuration for slowly changing dimensions (SCD).
    
    Extracts settings for SCD Type 2 dimension tables including:
    - Surrogate key column name from create_surrogate_key transformation
    - SCD enablement from primary config
    - Surrogate key logic from attach_dimension_surrogate_key transformations
    
    Args:
        primary_config (dict): Primary configuration dictionary
        advanced_config (list): List of advanced configuration dictionaries
    
    Returns:
        dict: Dictionary containing:
            - dimension_table_key_column_name (str): Surrogate key column name (from create_surrogate_key)
            - enable_scd2_dimension (bool): Whether to enable SCD Type 2 (from primary config)
            - source_timestamp_column_name (str): Timestamp column used to drive SCD Type 2 scd_start_date/scd_end_date values
            - fact_table_data_load (list): Parsed surrogate key configurations for fact table loading
    
    Example:
        >>> config = parse_dimension_table_configuration(
        ...     primary_config={'target_details_enable_scd2_dimension': 'true'},
        ...     advanced_config=[{
        ...         'advanced_settings': '{"Category": "create_surrogate_key", "column_name": "customer_sk", "type": "auto_increment"}'
        ...     }]
        ... )
        >>> config['enable_scd2_dimension']
        True
        >>> config['dimension_table_key_column_name']
        'customer_sk'
    """
    # Extract create_surrogate_key configuration from advanced_config to get the surrogate key column name
    create_surrogate_key_config = {}
    if advanced_config:
        for item in advanced_config:
            parsed = json.loads(item.get('advanced_settings', '{}'))
            if parsed.get('Category', '') == 'create_surrogate_key':
                create_surrogate_key_config = parsed
                break
    
    # Surrogate key column name (from create_surrogate_key 'column_name', fallback to default)
    dimension_table_key_column_name = create_surrogate_key_config.get(
        "column_name", "Key_SK"
    ).strip()
    
    # SCD Type 2 enablement (from primary_config target_details category)
    enable_scd2_dimension = primary_config.get(
        "target_details_enable_scd2_dimension",
         "false"
    ).lower().strip() == "true"
    
    # SCD Type 2 comparison column - the timestamp column used for scd_start_date/scd_end_date
    source_timestamp_column_name = primary_config.get(
        "target_details_source_timestamp_column_name",
        "delta__modified_datetime"
    ).strip()
    
    # Parse attach_dimension_surrogate_key logic from advanced configuration (fact table loading)
    fact_table_data_load = []
    if advanced_config:
        fact_table_data_load = [
            json.loads(values.get('advanced_settings'))
            for values in advanced_config 
            if 'attach' in json.loads(values.get('advanced_settings')).get('Category', '').lower()
        ]
    
    return {
        'dimension_table_key_column_name': dimension_table_key_column_name,
        'enable_scd2_dimension': enable_scd2_dimension,
        'source_timestamp_column_name': source_timestamp_column_name,
        'fact_table_data_load': fact_table_data_load
    }

# ===========================================================================================
# WAREHOUSE CONFIGURATION
# ===========================================================================================

def parse_warehouse_configuration(
    merge_type: str,
    datastore_config: str | list,
    target_datastore_name: str,
    spark_module
) -> dict:
    """
    Parse warehouse configuration and set up SQL endpoint if needed.
    
    Configures Spark to connect to Data Warehouse when merge_type is 'warehouse_spark_connector'.
    Retrieves warehouse endpoint from the Datastore_Configuration table.
    
    Args:
        merge_type (str): Merge type from target configuration
        datastore_config (str | list): Datastore configuration from Datastore_Configuration table.
                                       Either a string like "[{'Datastore_Name': 'bronze', ...}]" 
                                       or an already-parsed list.
        target_datastore_name (str): Target datastore name for warehouse
        spark_module: Spark module reference (injected for testability)
    
    Returns:
        dict: Dictionary containing:
            - warehouse_configured (bool): Whether warehouse was configured
            - warehouse_endpoint (str, optional): Warehouse SQL endpoint if configured
    
    Example:
        >>> config = parse_warehouse_configuration(
        ...     merge_type='warehouse_spark_connector',
        ...     datastore_config="[{'Datastore_Name': 'my_warehouse', 'Endpoint': 'ws-123.datawarehouse.windows.net'}]",
        ...     target_datastore_name='my_warehouse',
        ...     spark_module=spark
        ... )
        >>> config['warehouse_configured']
        True
    """
    warehouse_configured = False
    warehouse_endpoint = None
    
    if merge_type == "warehouse_spark_connector":
        # Get warehouse endpoint from datastore configuration
        warehouse_endpoint = _get_datastore_config(datastore_config, target_datastore_name, 'Endpoint')
        
        # Configure Spark to use warehouse
        spark_module.conf.set(
            f"spark.datawarehouse.{target_datastore_name}.sqlendpoint",
            f"{warehouse_endpoint},1433"
        )
        warehouse_configured = True
    
    return {
        'warehouse_configured': warehouse_configured,
        'warehouse_endpoint': warehouse_endpoint
    }


def _resolve_file_header_settings(primary_config: Dict[str, Any]) -> Tuple[bool, Any]:
    """Return the header flag and pandas header configuration."""
    file_has_header_row = primary_config.get("source_details_file_has_header_row", 'true').lower() == 'true'
    pandas_header_config = 0 if file_has_header_row else None
    return file_has_header_row, pandas_header_config


def _resolve_encoding(primary_config: Dict[str, Any]) -> str:
    """Return the requested file encoding with a UTF-8 default."""
    return primary_config.get("source_details_encoding", "utf-8")


def _resolve_xml_settings(primary_config: Dict[str, Any]) -> Tuple[Any, Any]:
    """Parse XML XPath and namespace strings into usable settings."""
    xml_xpath = primary_config.get("source_details_xml_xpath")
    namespace_keys = primary_config.get("source_details_xml_namespaces_keys")
    namespace_values = primary_config.get("source_details_xml_namespaces_values")

    if namespace_keys or namespace_values:
        if not namespace_keys or not namespace_values:
            raise Exception(
                "Both xml namespace keys and values must be provided when configuring XML namespaces."
            )

        namespace_key_list = [key.strip() for key in namespace_keys.split(",") if key.strip()]
        namespace_value_list = [value.strip() for value in namespace_values.split(",") if value.strip()]

        if len(namespace_key_list) != len(namespace_value_list):
            raise Exception(
                "XML namespace keys and values must have the same number of entries."
            )

        xml_namespaces = dict(zip(namespace_key_list, namespace_value_list))
    else:
        xml_namespaces = None

    return xml_xpath, xml_namespaces


def _resolve_on_bad_records_mode(on_bad_records: str) -> Any:
    """Translate configuration value into Spark read mode."""
    if not on_bad_records:
        return None

    normalized_value = on_bad_records.lower().strip()
    if normalized_value == "fail":
        return "FAILFAST"
    if normalized_value == "drop":
        return "DROPMALFORMED"
    if normalized_value == "quarantine":
        return "PERMISSIVE"

    raise Exception(
        f"Invalid on_bad_records value, {on_bad_records}. Valid value are fail, drop, and quarantine"
    )


def _build_schema_kwargs(schema: Any, mode: Any) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Construct parquet and generic schema kwargs."""
    if not schema:
        return {}, {}
    
    # Convert DDL string to StructType for Spark readers
    if isinstance(schema, str):
        from pyspark.sql.types import _parse_datatype_string
        schema = _parse_datatype_string(schema)

    parquet_schema_kwargs = {"schema": schema}
    schema_kwargs: Dict[str, Any] = {
        "mode": mode,
        "schema": schema
    }

    if mode == "PERMISSIVE":
        schema_kwargs['columnNameOfCorruptRecord'] = "delta__corrupt_record"

    return parquet_schema_kwargs, schema_kwargs


def extract_file_configuration(
    primary_config: Dict[str, Any],
    fail_on_new_schema: bool
) -> Dict[str, Any]:
    """Extract and process configuration parameters."""
    delimiter = primary_config.get("source_details_delimiter", ",")
    sheet_name = primary_config.get("source_details_sheet_name", "")
    file_has_header_row, pandas_header_config = _resolve_file_header_settings(primary_config)

    multiline = primary_config.get("source_details_multiline", 'false').lower() == 'true'
    schema = primary_config.get("source_details_schema")
    on_bad_records = primary_config.get("source_details_on_bad_records", "quarantine")
    mode = _resolve_on_bad_records_mode(on_bad_records)
    encoding = _resolve_encoding(primary_config)
    xml_xpath, xml_namespaces = _resolve_xml_settings(primary_config)

    allow_missing_columns = not fail_on_new_schema
    parquet_schema_kwargs, schema_kwargs = _build_schema_kwargs(schema, mode)

    process_one_file_at_a_time = primary_config.get("source_details_process_one_file_at_a_time", "false").lower() == "true"
    
    # Preserve original schema string for post-read validation (used by Parquet readers)
    # This enables schema contract validation without applying schema during read
    expected_schema_string = schema if isinstance(schema, str) else None

    return {
        'delimiter': delimiter,
        'file_has_header_row': file_has_header_row,
        'sheet_name': sheet_name,
        'pandas_header_config': pandas_header_config,
        'multiline': multiline,
        'encoding': encoding,
        'allow_missing_columns': allow_missing_columns,
        'parquet_schema_kwargs': parquet_schema_kwargs,
        'schema_kwargs': schema_kwargs,
        'xml_xpath': xml_xpath,
        'xml_namespaces': xml_namespaces,
        'process_one_file_at_a_time': process_one_file_at_a_time,
        'expected_schema_string': expected_schema_string
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Spark Session Configuration
# 
# This section configures the Spark session with optimized settings based on the target lakehouse layer. Each layer (Bronze, Silver, Gold) has specific performance characteristics and requirements that are addressed through tailored Spark configurations.
# 
# ### Configuration Strategy
# - **Bronze**: Optimized for high-speed ingestion with minimal transformations
# - **Silver**: Balanced configuration for both read and write operations
# - **Gold**: Optimized for analytical query performance and complex transformations

# CELL ********************

# ===========================================================================================
# PERFORMANCE OPTIMIZATION SETTINGS
# ===========================================================================================

def parse_performance_configuration(
    primary_config: dict,
    target_datastore_medallion_name: str
) -> dict:
    """
    Parse performance optimization configuration including compute statistics.
    
    Extracts configuration for:
    - Column-level statistics computation
    - Number of columns for statistics (if configured)
    - Spark configuration enablement for lakehouse tables
    
    Args:
        primary_config (dict): Primary configuration dictionary
        target_datastore_medallion_name (str): Target medallion layer name (bronze/silver/gold)
    
    Returns:
        dict: Dictionary containing:
            - compute_statistics_on_columns (str): Comma-separated column names
            - compute_statistics_on_first_n_columns (str): Number of columns for stats
            - column_stats_configured (bool): Whether statistics are configured
            - use_spark_config_for_lakehouse (str): Layer name for Spark config enablement
    
    Example:
        >>> config = parse_performance_configuration(
        ...     primary_config={
        ...         'target_details_compute_statistics_on_columns': 'col1,col2',
        ...         'target_details_use_spark_config_for_lakehouse': 'silver'
        ...     },
        ...     target_datastore_medallion_name='silver'
        ... )
        >>> config['column_stats_configured']
        True
    """
    # Column statistics configuration
    compute_statistics_on_columns = primary_config.get(
        "target_details_compute_statistics_on_columns", ""
    )
    
    compute_statistics_on_first_n_columns = primary_config.get(
        "target_details_compute_statistics_on_first_n_columns", ""
    )
    
    column_stats_configured = bool(compute_statistics_on_columns) or bool(compute_statistics_on_first_n_columns)
    
    # Spark configuration enablement (target layer for which to enable Spark configs)
    use_spark_config_for_lakehouse = primary_config.get(
        "target_details_use_spark_config_for_lakehouse",
        target_datastore_medallion_name
    ).strip().lower()
    
    return {
        'compute_statistics_on_columns': compute_statistics_on_columns,
        'compute_statistics_on_first_n_columns': compute_statistics_on_first_n_columns,
        'column_stats_configured': column_stats_configured,
        'use_spark_config_for_lakehouse': use_spark_config_for_lakehouse
    }

# ===========================================================================================
# OTHER SPARK CONFIG
# ===========================================================================================

def parse_spark_configuration(primary_config: dict) -> dict:
    """
    Parse Spark configuration settings for Delta table operations.
    
    Extracts configuration for:
    - Change data feed enablement
    - Timestamp rebase mode for write operations (from target_details)
    - Timestamp rebase mode for read operations (from source_details)
    
    Args:
        primary_config (dict): Primary configuration dictionary
    
    Returns:
        dict: Dictionary containing:
            - enable_change_data_feed (bool): Enable CDC on Delta tables
            - spark_timestamp_rebase_mode_write (str): Write rebase mode
            - spark_timestamp_rebase_mode_read (str): Read rebase mode
    
    Example:
        >>> config = parse_spark_configuration(
        ...     primary_config={
        ...         'target_details_enable_change_data_feed': 'true',
        ...         'target_details_spark_timestamp_rebase_mode': 'LEGACY'
        ...     }
        ... )
        >>> config['enable_change_data_feed']
        True
    """
    # Change data feed for CDC
    enable_change_data_feed = primary_config.get(
        "target_details_enable_change_data_feed", "false"
    ).strip().lower() == "true"
    
    # Default timestamp rebase mode
    default_timestamp_rebase_mode = "CORRECTED"
    
    # Timestamp rebase modes for legacy Parquet compatibility
    # Write mode is stored in target_details
    spark_timestamp_rebase_mode_write = primary_config.get(
        "target_details_spark_timestamp_rebase_mode",
        default_timestamp_rebase_mode
    ).strip().upper()
    
    # Read mode is stored in source_details
    spark_timestamp_rebase_mode_read = primary_config.get(
        "source_details_spark_timestamp_rebase_mode",
        default_timestamp_rebase_mode
    ).strip().upper()
    
    return {
        'enable_change_data_feed': enable_change_data_feed,
        'spark_timestamp_rebase_mode_write': spark_timestamp_rebase_mode_write,
        'spark_timestamp_rebase_mode_read': spark_timestamp_rebase_mode_read
    }

def parse_bronze_layer_spark_configuration() -> dict:
    """
    Parse Spark configuration settings optimized for Bronze layer ingestion.
    
    Bronze layer is optimized for high-speed data ingestion with minimal processing overhead.
    Configuration focuses on fast writes while maintaining data quality for downstream processing.
    
    Args:
        None
    Returns:
        dict: Bronze layer Spark configuration with keys:
            - optimize_write_enabled (bool): Whether to enable optimize write
            - optimize_write_partitioned_enabled (bool): Whether to enable partitioned optimize write
            - v_order_enabled (bool): Whether to enable V-Order Parquet optimization
            - compute_statistics_on_columns (str): Columns for statistics computation
            - checkpoint_interval (str): Delta checkpoint interval
    
    Example:
        >>> config = parse_bronze_layer_spark_configuration(
        ...     staging_folder_path = "/lakehouse/Bronze_Files/staging",
        ...     column_stats_configured = False
        ... )
        >>> print(config['optimize_write_enabled'])
        False
    
    Reference:
        https://support.fabric.microsoft.com/en-us/blog/optimizing-spark-compute-for-medallion-architectures-in-microsoft-fabric
    """
    log_and_print("Setting spark config for Bronze data")
    
    # Bronze layer optimizations - fast ingestion focused
    optimize_write_enabled = False
    optimize_write_partitioned_enabled = True
    v_order_enabled = False
    checkpoint_interval = "25"
    
    return {
        'optimize_write_enabled': optimize_write_enabled,
        'optimize_write_partitioned_enabled': optimize_write_partitioned_enabled,
        'v_order_enabled': v_order_enabled,
        'checkpoint_interval': checkpoint_interval
    }


def parse_silver_layer_spark_configuration() -> dict:
    """
    Parse Spark configuration settings optimized for Silver layer processing.
    
    Silver layer uses a balanced configuration for data cleansing, transformation,
    and storage efficiency. Configuration balances read and write performance.
    
    Args:
        None
    
    Returns:
        dict: Silver layer Spark configuration with keys:
            - optimize_write_enabled (bool): Whether to enable optimize write
            - optimize_write_partitioned_enabled (bool): Whether to enable partitioned optimize write
            - v_order_enabled (bool): Whether to enable V-Order Parquet optimization
            - checkpoint_interval (str): Delta checkpoint interval
    
    Example:
        >>> config = parse_silver_layer_spark_configuration()
        >>> print(config['optimize_write_enabled'])
        True
    
    Reference:
        https://support.fabric.microsoft.com/en-us/blog/optimizing-spark-compute-for-medallion-architectures-in-microsoft-fabric
    """
    log_and_print("Setting spark config for Silver data")
    
    # Silver layer optimizations - balanced approach
    optimize_write_enabled = True
    optimize_write_partitioned_enabled = True
    v_order_enabled = False
    
    return {
        'optimize_write_enabled': optimize_write_enabled,
        'optimize_write_partitioned_enabled': optimize_write_partitioned_enabled,
        'v_order_enabled': v_order_enabled,
        'checkpoint_interval': None
    }


def parse_gold_layer_spark_configuration() -> dict:
    """
    Parse Spark configuration settings optimized for Gold layer analytics.
    
    Gold layer is optimized for analytical query performance and large-scale aggregations.
    Configuration prioritizes read performance for business intelligence and reporting workloads.
    
    Args:
        None
    
    Returns:
        dict: Gold layer Spark configuration with keys:
            - optimize_write_enabled (bool): Whether to enable optimize write
            - optimize_write_partitioned_enabled (bool): Whether to enable partitioned optimize write
            - v_order_enabled (bool): Whether to enable V-Order Parquet optimization
            - checkpoint_interval (str): Delta checkpoint interval
    
    Example:
        >>> config = parse_gold_layer_spark_configuration()
        >>> print(config['optimize_write_enabled'])
        True
        >>> print(config['v_order_enabled'])
        True
    
    Reference:
        https://support.fabric.microsoft.com/en-us/blog/optimizing-spark-compute-for-medallion-architectures-in-microsoft-fabric
    """
    log_and_print("Setting spark config for Gold data")
    
    # Gold layer optimizations - read performance focused
    optimize_write_enabled = True
    optimize_write_partitioned_enabled = True
    v_order_enabled = True  # Enable V-Order for optimal query performance
    
    return {
        'optimize_write_enabled': optimize_write_enabled,
        'optimize_write_partitioned_enabled': optimize_write_partitioned_enabled,
        'v_order_enabled': v_order_enabled,
        'checkpoint_interval': None
    }


def apply_layer_specific_spark_configurations(
    optimize_write_enabled: bool,
    optimize_write_partitioned_enabled: bool,
    v_order_enabled: bool,
    checkpoint_interval: str
) -> None:
    """
    Apply layer-specific Spark configurations for write optimization and Parquet settings.
    
    This function applies the configuration settings determined by the medallion layer
    parser functions (Bronze/Silver/Gold) to the active Spark session.
    
    Args:
        optimize_write_enabled (bool): Whether to enable optimize write
        optimize_write_partitioned_enabled (bool): Whether to enable partitioned optimize write
        v_order_enabled (bool): Whether to enable V-Order Parquet optimization
    
    Returns:
        None
    
    Example:
        >>> apply_layer_specific_spark_configurations(
        ...     optimize_write_enabled = True,
        ...     optimize_write_partitioned_enabled = True,
        ...     v_order_enabled = True
        ...     layer_name = "gold"
        ... )
    
    Reference:
        https://support.fabric.microsoft.com/en-us/blog/optimizing-spark-compute-for-medallion-architectures-in-microsoft-fabric
    """
    # Performance settings - maximize query performance
    spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", optimize_write_enabled)
    log_and_print(f"Spark Config: spark.databricks.delta.optimizeWrite.enabled = {optimize_write_enabled}")
    
    spark.conf.set("spark.databricks.delta.optimizeWrite.partitioned.enabled", optimize_write_partitioned_enabled)
    log_and_print(f"Spark Config: spark.databricks.delta.optimizeWrite.partitioned.enabled = {optimize_write_partitioned_enabled}")

    # Parquet V-Order optimization
    spark.conf.set('spark.sql.parquet.vorder.enabled', v_order_enabled)
    log_and_print(f"Spark Config: spark.sql.parquet.vorder.enabled = {v_order_enabled}")

    if checkpoint_interval:
        # Apply checkpoint optimization
        spark.conf.set("spark.databricks.delta.properties.defaults.checkpointInterval", checkpoint_interval)
        log_and_print(f"Spark Config: spark.databricks.delta.properties.defaults.checkpointInterval = {checkpoint_interval}")

def apply_spark_configurations(
    spark_timestamp_rebase_mode_write: str,
    spark_timestamp_rebase_mode_read: str,
    enable_change_data_feed: str,
    use_spark_config_for_lakehouse: str
) -> None:
    """
    Apply Spark configurations that apply to all medallion layers.
    
    This function sets up essential Spark configurations for Parquet processing,
    Delta Lake optimizations, and data management features. These settings are
    layer-agnostic and form the foundation for all data processing operations.
    
    Args:
        spark_timestamp_rebase_mode_write (str): Rebase mode for timestamps when writing Parquet files
            (e.g., "CORRECTED", "LEGACY")
        spark_timestamp_rebase_mode_read (str): Rebase mode for timestamps when reading Parquet files
        enable_change_data_feed (str): Whether to enable Delta Lake change data feed ("True"/"False")
        use_spark_config_for_lakehouse (str)
    Returns:
        None
    
    Example:
        >>> apply_base_spark_configurations(
        ...     spark_timestamp_rebase_mode_write = "CORRECTED",
        ...     spark_timestamp_rebase_mode_read = "CORRECTED",
        ...     enable_change_data_feed = "True"
        ... )
    
    Reference:
        https://milescole.dev/data-engineering/2025/06/30/Spark-v-DuckDb-v-Polars-v-Daft-Revisited.html
    """
    # Set Spark configuration for datetime and int96 rebase mode when writing Parquet files
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", spark_timestamp_rebase_mode_write)
    log_and_print(f"Spark Config: spark.sql.parquet.datetimeRebaseModeInWrite = {spark_timestamp_rebase_mode_write}")
    spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", spark_timestamp_rebase_mode_write)
    log_and_print(f"Spark Config: spark.sql.parquet.int96RebaseModeInWrite = {spark_timestamp_rebase_mode_write}")

    # Set Spark configuration for datetime and int96 rebase mode when reading Parquet files
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", spark_timestamp_rebase_mode_read)
    log_and_print(f"Spark Config: spark.sql.parquet.datetimeRebaseModeInRead = {spark_timestamp_rebase_mode_read}")
    spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", spark_timestamp_rebase_mode_read)
    log_and_print(f"Spark Config: spark.sql.parquet.int96RebaseModeInRead = {spark_timestamp_rebase_mode_read}")

    # Schema evolution settings
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "True")
    log_and_print("Spark Config: spark.databricks.delta.schema.autoMerge.enabled = True")

    # Change data feed
    spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", enable_change_data_feed)
    log_and_print(f"Spark Config: spark.databricks.delta.properties.defaults.enableChangeDataFeed = {enable_change_data_feed}")

    # This cuts the overhead of Delta table snapshot generation 
    # (the process of identifying and caching the list of files that are active in the version of the table being queried) by ~50%.
    spark.conf.set("spark.microsoft.delta.snapshot.driverMode.enabled", True)
    log_and_print("Spark Config: spark.microsoft.delta.snapshot.driverMode.enabled = True")

    # Support fast GDPR style deletes, negligible write performance impact
    spark.conf.set("spark.databricks.delta.properties.defaults.enableDeletionVectors", "True")
    log_and_print("Spark Config: spark.databricks.delta.properties.defaults.enableDeletionVectors = True")

    # SQL compliance settings
    spark.conf.set("spark.sql.ansi.enabled", "True")
    log_and_print("Spark Config: spark.sql.ansi.enabled = True")

    # Column mapping for schema flexibility
    spark.conf.set("spark.databricks.delta.properties.defaults.columnMapping.mode", "name")
    log_and_print("Spark Config: spark.databricks.delta.properties.defaults.columnMapping.mode = name")

    # Performance settings - trigger compaction automatically
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "True")
    log_and_print("Spark Config: spark.databricks.delta.autoCompact.enabled = True")

    # Performance settings - compaction
    # https://learn.microsoft.com/en-us/fabric/data-engineering/table-compaction?tabs=sparksql
    spark.conf.set("spark.microsoft.delta.optimize.fast.enabled", "True")
    log_and_print("Spark Config: spark.microsoft.delta.optimize.fast.enabled = True")

    spark.conf.set("spark.microsoft.delta.optimize.fileLevelTarget.enabled", "True")
    log_and_print("Spark Config: spark.microsoft.delta.optimize.fileLevelTarget.enabled = True")

    # https://learn.microsoft.com/en-us/fabric/data-engineering/tune-file-size?tabs=sparksql#adaptive-target-file-size
    spark.conf.set("spark.microsoft.delta.targetFileSize.adaptive.enabled", "True")
    log_and_print("Spark Config: spark.microsoft.delta.targetFileSize.adaptive.enabled = True")

    # Parse layer-specific configuration based on medallion layer
    if use_spark_config_for_lakehouse == 'bronze':
        layer_config = parse_bronze_layer_spark_configuration()

    elif use_spark_config_for_lakehouse == 'silver':
        layer_config = parse_silver_layer_spark_configuration()

    elif use_spark_config_for_lakehouse == 'gold':
        layer_config = parse_gold_layer_spark_configuration()
    else:
        raise Exception(f"Either `{f'{target_datastore_name}_datastore_medallion_name'}` or `target_details_use_spark_config_for_lakehouse` is set incorrectly. The only allowed values are `bronze`, `silver`, or `gold`.")

    # Apply layer-specific Spark configurations
    apply_layer_specific_spark_configurations(
        optimize_write_enabled = layer_config['optimize_write_enabled'],
        optimize_write_partitioned_enabled = layer_config['optimize_write_partitioned_enabled'],
        v_order_enabled = layer_config['v_order_enabled'],
        checkpoint_interval = layer_config['checkpoint_interval']
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Date Dimension Table Creation
# 
# This section provides utilities for creating standard date dimension tables, which are fundamental components of any star schema. The date dimension enables time-based analysis and provides a rich set of attributes for temporal reporting.
# 
# Key features:
# - **Comprehensive Date Attributes**: Year, quarter, month, week, day attributes
# - **Business Calendar Support**: Weekend flags, month names, week numbers
# - **Sorting Columns**: Pre-calculated sort orders for proper visualization
# - **Extended Date Range**: Covers 100+ years (1950-2050) by default
# - **Null Row Support**: Includes -1 surrogate key for unknown dates

# CELL ********************

def create_date_dimension(
    fact_table_data_load: list,
    target_datastore_workspace_id: str,
    target_datastore_id: str,
    date_table_schema_name: str = 'dbo',
    date_table_name: str = 'dim_date',
    date_dimension_table_key_column_name: str = "date_sk"
):
    """
    Create a comprehensive date dimension table for analytical workloads.

    This function generates a date dimension with a century of dates and rich
    temporal attributes. The table serves as a conformed dimension across all
    fact tables, enabling consistent time-based analysis and reporting.

    The dimension includes pre-calculated attributes to optimize query performance
    and simplify report development. All common date hierarchies and groupings
    are materialized to avoid runtime calculations.

    Args:
        fact_table_data_load (list): Parsed surrogate key configurations from metadata for fact table loading.
                                     If empty, function exits without creating date dimension.
        target_datastore_workspace_id (str): Fabric workspace GUID where date dimension will be created
        target_datastore_id (str): Lakehouse GUID where date dimension will be created
        date_table_schema_name (str): Schema name for the date dimension table (default: 'dbo')
        date_table_name (str): Table name for the date dimension (default: 'dim_date')
        date_dimension_table_key_column_name (str): Name for the surrogate key column 
                                                        in YYYYMMDD integer format (default: 'Key_SK')

    Table Structure:
        - Surrogate Key: Integer in YYYYMMDD format for efficient joins
        - Date Attributes: Full date, year, quarter, month, day
        - Descriptive Fields: Month names, day names, formatted strings
        - Business Flags: Weekend indicator for business day calculations
        - Sort Columns: Negative values for descending order in reports
        - Hierarchies: Year-Month combinations for drill-down analysis
        - Null Row: -1 surrogate key for unknown/missing dates in fact tables

    Performance Considerations:
        - Static table with ~36,500 rows (100 years: 1950-2050)
        - Integer surrogate key for optimal join performance
        - Pre-calculated attributes eliminate runtime functions
        - Suitable for broadcasting in distributed queries

    Behavior:
        - Only creates dimension if attach_dimension_surrogate_key is configured (fact tables only)
        - Skips creation for dimension tables (which use create_surrogate_key)
        - Skips creation if date dimension table already exists at the target path
        - Creates table in the target lakehouse specified by workspace and datastore IDs

    Usage Example:
        Date dimensions are typically joined to fact tables:
        ```sql
        SELECT d.Month_Name, SUM(f.Sales_Amount)
        FROM fact_sales f
        JOIN dim_date d ON f.date_sk = d.date_sk
        GROUP BY d.Month_Name, d.Sort_Month
        ORDER BY d.Sort_Month DESC
        ```
    """
    # Exit early if no attach_dimension_surrogate_key logic is configured
    # Only fact tables (with attach_dimension_surrogate_key) need the date dimension
    # Dimension tables (with create_surrogate_key) do not need it auto-created
    if not fact_table_data_load:
        return
    
    # Construct ABFSS path to date dimension table location
    date_dimension_path = (
        f"abfss://{target_datastore_workspace_id}@onelake.dfs.fabric.microsoft.com/"
        f"{target_datastore_id}/Tables/{date_table_schema_name}/{date_table_name}"
    )
    
    # Skip creation if date dimension already exists
    date_dimension_table_exists = notebookutils.fs.exists(date_dimension_path)

    if date_dimension_table_exists:
        return 
    
    creating_date_dimension_log_info = "Creating date dimension table."
    log_and_print(creating_date_dimension_log_info)

    # Define date range spanning 100 years (1950-2050)
    start_date = date(1950, 1, 1)
    end_date = date(2050, 12, 31)
    
    # Generate DataFrame with sequential dates across the entire range
    date_df = spark.createDataFrame(
        [(start_date + timedelta(days=i),)
        for i in range((end_date - start_date).days + 1)],
        ["date"]
    )
    
    # Transform into comprehensive date dimension with all analytical attributes
    # Surrogate key uses YYYYMMDD integer format for efficient joins
    date_dim_df = date_df.withColumn(date_dimension_table_key_column_name, f.date_format("date", "yyyyMMdd").cast("int")) \
        .withColumn("Date", f.col("date")) \
        .withColumn("Date_Text", f.date_format("date", "yyyy-MM-dd")) \
        .withColumn("Year", f.year("date")) \
        .withColumn("Quarter", f.quarter("date")) \
        .withColumn("Month", f.month("date")) \
        .withColumn("Month_Name", f.date_format("date", "MMMM")) \
        .withColumn("Month_Name_Abbrev", f.date_format("date", "MMM")) \
        .withColumn("Day", f.dayofmonth("date")) \
        .withColumn("Day_Name", f.date_format("date", "EEEE")) \
        .withColumn("Day_Of_Week", f.dayofweek("date")) \
        .withColumn("Week_Of_Year", f.weekofyear("date")) \
        .withColumn("Is_Weekend", f.expr("CASE WHEN dayofweek(date) IN (1, 7) THEN TRUE ELSE FALSE END")) \
        .withColumn("Month_Year", f.concat_ws(", ", f.date_format("date", "MMM"), f.year("date"))) \
        .withColumn("Sort_Year", -f.year("date")) \
        .withColumn("Sort_Quarter", -f.quarter("date")) \
        .withColumn("Sort_Month", -f.month("date")) \
        .withColumn("Sort_Day", -f.date_format("date", "yyyyMMdd").cast("int")) \
        .withColumn("Sort_Day_Of_Week", -f.dayofweek("date")) \
        .withColumn("Sort_Week_Of_Year", -f.weekofyear("date")) \
        .withColumn("Sort_Year_Month", (f.year("date") * 100 + f.month("date")) * -1)

    def set_all_columns_nullable(schema):
        """Make all columns nullable to support null row addition."""
        new_fields = [StructField(field.name, field.dataType, True) for field in schema.fields]
        return StructType(new_fields)

    # Update schema to allow nulls (required for adding null row with -1 key)
    updated_schema = set_all_columns_nullable(date_dim_df.schema)
    date_dim_df = spark.createDataFrame(date_dim_df.rdd, schema = updated_schema)
    
    # Add null row with -1 surrogate key for unknown/missing dates in fact tables
    date_dim_df_null_row = spark.createDataFrame([(-1,)], [date_dimension_table_key_column_name])
    date_dim_df = date_dim_df.unionByName(date_dim_df_null_row, allowMissingColumns = True)

    # Write to target lakehouse as foundational dimension table
    date_dim_df.write.mode("overwrite").option("overwriteSchema", "true").save(date_dimension_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Slowly Changing Dimensions (SCD) Type 2 Support
# 
# This section provides utility functions for implementing Type 2 Slowly Changing Dimensions, which track historical changes in dimension data over time.
# 
# ### SCD2 Column Addition Function
# Adds the necessary tracking columns for SCD2 implementation:
# - **scd_start_date**: When the record version became effective
# - **scd_end_date**: When the record version was superseded (NULL for current records)
# - **scd_active**: Flag indicating if this is the current version (1) or historical (0)

# CELL ********************

def _add_scd2_columns_with_deletion_tracking(
    new_data,
    column_to_mark_source_data_deletion: str,
    delete_rows_with_value: str,
    source_timestamp_column_name: str
):
    """
    Add SCD2 columns with deletion tracking logic.
    
    Args:
        new_data (DataFrame): Input dimension data
        column_to_mark_source_data_deletion (str): Column indicating if record was deleted at source
        delete_rows_with_value (str): Value of column when data is deleted
        source_timestamp_column_name (str): Column containing the business timestamp for versioning
    
    Returns:
        DataFrame: DataFrame with SCD2 columns added
    """
    if delete_rows_with_value.isdigit():
        delete_rows_with_value = int(delete_rows_with_value)

    new_data = (
        new_data.withColumn("scd_start_date", f.col(source_timestamp_column_name))
        .withColumn("scd_end_date",
            f.when(
                f.col(column_to_mark_source_data_deletion) == delete_rows_with_value, f.col(source_timestamp_column_name)
            ).otherwise(f.lit(None).cast(TimestampType())),
        )
        .withColumn("scd_active", f.when(f.col(column_to_mark_source_data_deletion) == delete_rows_with_value, 0).otherwise(1))
    )
    return new_data


def _add_scd2_columns_without_deletion_tracking(
    new_data,
    source_timestamp_column_name: str
):
    """
    Add SCD2 columns without deletion tracking (all records treated as active).
    
    Args:
        new_data (DataFrame): Input dimension data
        source_timestamp_column_name (str): Column containing the business timestamp for versioning
    
    Returns:
        DataFrame: DataFrame with SCD2 columns added
    """
    new_data = (
        new_data.withColumn("scd_start_date", f.col(source_timestamp_column_name))
        .withColumn("scd_end_date", f.lit(None).cast(TimestampType()))
        .withColumn("scd_active", f.lit(1))
    )
    return new_data


def add_scd2_columns_for_dimensions(
    new_data,
    column_to_mark_source_data_deletion: str,
    delete_rows_with_value: str,
    source_timestamp_column_name: str,
    lakehouse_table_output: bool,
    first_run: bool,
    enable_scd2_dimension: bool
):
    """
    Add SCD Type 2 tracking columns to dimension data for historical change tracking.

    This function enhances dimension tables with temporal columns required for SCD2 
    implementation, enabling point-in-time analysis and historical reporting.

    Args:
        new_data (DataFrame): Input dimension data requiring SCD2 columns
        column_to_mark_source_data_deletion (str): Column indicating if record was deleted at source
        delete_rows_with_value (str): value of column when data is deleted
        source_timestamp_column_name (str): Column containing the business timestamp for versioning

    Returns:
        DataFrame: Enhanced DataFrame with SCD2 tracking columns:
                  - scd_start_date: Record version effective date
                  - scd_end_date: Record version end date (NULL for current)
                  - scd_active: Current version indicator (1=current, 0=historical)

    Implementation Notes:
        - Deleted records get both start and end dates set to the source_timestamp_column_name value
        - Active records have NULL end dates to indicate they are current
        - The source_timestamp_column_name column serves as the version timestamp
    """

    if not lakehouse_table_output or not first_run or not enable_scd2_dimension:
        return new_data

    adding_scd2_columns_log_info = "Adding scd2 columns to dataframe before delta table creation."
    log_and_print(adding_scd2_columns_log_info)
    
    if column_to_mark_source_data_deletion:
        new_data = _add_scd2_columns_with_deletion_tracking(
            new_data=new_data,
            column_to_mark_source_data_deletion=column_to_mark_source_data_deletion,
            delete_rows_with_value=delete_rows_with_value,
            source_timestamp_column_name=source_timestamp_column_name
        )
    else:
        new_data = _add_scd2_columns_without_deletion_tracking(
            new_data=new_data,
            source_timestamp_column_name=source_timestamp_column_name
        )
    
    return new_data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. SCD2 Post-Processing Updates
# 
# After initial SCD2 processing, this function ensures that single-version dimension records have their start dates set to the beginning of time (1900-01-01). This ensures these records are always available for historical fact table joins.

# CELL ********************

def post_processing_scd2_update(
    primary_keys: str,
    target_abfss_path: str):
    """
    Adjust SCD2 start dates for single-version dimension records to ensure historical availability.

    This post-processing step identifies dimension records that have only one version and sets 
    their start date to 1900-01-01. This ensures these records are available for all historical 
    fact data, preventing join failures for early transactions.

    Args:
        primary_keys (list): Natural/business key columns identifying unique dimension members
        target_abfss_path (str): Target table name abfss path

    Business Logic:
        - Single-version records represent dimension members that have never changed
        - Setting their start date to 1900-01-01 ensures they match all historical facts
        - Multi-version records retain their actual change dates for accurate history

    Performance Considerations:
        - Only processes records with exactly one version
        - Uses bulk merge operation for efficiency
        - Minimal impact on large dimension tables
    """
    # Create concatenated primary key for grouping
    pks_concat = 'concat('+ ','.join(primary_keys) + ')'

    # Identify dimension records with only one version
    dimensions_with_one_row = spark.sql(
        f"""
        SELECT      *
        FROM        delta.`{target_abfss_path}`
        WHERE       {pks_concat} IN (
            SELECT      {pks_concat} 
            FROM        delta.`{target_abfss_path}`
            GROUP BY    {pks_concat}
            HAVING      COUNT(*) = 1
        )
        """)
    
    # Set start date to beginning of time for universal availability    
    dimensions_with_one_row = dimensions_with_one_row.withColumn("scd_start_date", f.to_date(f.lit('1900-01-01'), 'yyyy-mm-dd'))

    # Get Delta table reference for merge operation
    target_table = DeltaTable.forPath(spark, target_abfss_path)

    # Define merge aliases
    target_alias = "target_df"
    source_alias = "source_df"

    # Build null-safe merge condition
    merge_condition = get_merge_condition(
        merge_keys = primary_keys, right_alias = source_alias, left_alias = target_alias
    )
    
    # Execute merge to update start dates
    target_table.alias(target_alias).merge(
        source=dimensions_with_one_row.alias(source_alias), condition=merge_condition
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_schema_changes(new_schema: list, old_schema: list):
    """
    Compare two schemas and identify all differences including new, dropped, and changed columns.

    This function performs a comprehensive schema comparison to detect evolution patterns,
    which is critical for maintaining data quality and managing schema drift in the lakehouse.

    Args:
        new_schema (list): Current schema as list of (column_name, data_type) tuples
        old_schema (list): Previous schema as list of (column_name, data_type) tuples

    Returns:
        list: List of dictionaries describing schema changes:
              - column: Column name affected
              - data_type: Data type information (varies by change type)
              - change_type: Type of change ('Column Added', 'Column Dropped', 'Column Type Changed')

    Schema Change Detection:
        - Column Added: Present in new schema but not in old
        - Column Dropped: Present in old schema but not in new
        - Column Type Changed: Same column name but different data type

    Usage:
        Schema changes are logged for audit trails and can trigger different behaviors
        based on configuration (fail, warn, or auto-adapt).
    """
    # Convert schemas to dictionaries for efficient comparison
    old_schema_dict = dict(old_schema)
    new_schema_dict = dict(new_schema)

    # Identify additions - columns in new schema not in old
    new_columns = [{"column": col, "data_type": new_schema_dict[col], "change_type": "Column Added"} 
                for col in set(new_schema_dict.keys()) - set(old_schema_dict.keys())]

    # Identify deletions - columns in old schema not in new
    dropped_columns = [{"column": col, "data_type": old_schema_dict[col], "change_type": "Column Dropped"} 
                    for col in set(old_schema_dict.keys()) - set(new_schema_dict.keys())]

    # Identify type changes - same column name but different data type
    data_type_changes = [{"column": col, "data_type": f"{old_schema_dict[col]} --> {new_schema_dict[col]}", 
                        "change_type": "Column Type Changed"} 
                        for col in old_schema_dict.keys() & new_schema_dict.keys() 
                        if old_schema_dict[col] != new_schema_dict[col]]

    # Combine all changes into a single comprehensive list
    schema_changes = new_columns + dropped_columns + data_type_changes

    return schema_changes

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Spark Table Metadata Management
# 
# This section handles the updating of column descriptions in Spark tables, particularly for documenting primary keys. This enhances table discoverability and understanding for downstream consumers.

# CELL ********************

def update_spark_column_descriptions(column_descriptions: list, primary_keys: list, target_datastore_name: str, target_table_name: str, target_workspace_name):
    """
    Update Spark table column descriptions to indicate primary key status.

    This function enhances table metadata by adding or removing "PRIMARY KEY" indicators
    in column descriptions, improving documentation and data discovery capabilities.

    Args:
        column_descriptions (list): Current column metadata from DESCRIBE TABLE
        primary_keys (list): List of columns that are primary keys
        target_datastore_name (str): Target lakehouse name
        target_table_name (str): Target table name
        target_workspace_name (str): Workspace containing the lakehouse

    Column Description Logic:
        - Primary key columns get "PRIMARY KEY" prefix (if not already present)
        - Non-primary key columns have "PRIMARY KEY" removed (if present)
        - Existing descriptions are preserved and appended after primary key indicator

    Benefits:
        - Improves table documentation for data consumers
        - Enables programmatic discovery of primary keys
        - Maintains consistency across all tables in the platform
    """
    
    # Ensure primary_keys is a list, default to empty if not provided
    if not primary_keys:
        primary_keys = []
        
    for column_description in column_descriptions:
        col_name = column_description['col_name']
        comment = column_description['comment']
        
        # Normalize comment handling
        if not comment:
            comment = ""
        else:
            comment = comment.strip()
            
        # Add PRIMARY KEY to description if column is a primary key
        if col_name in primary_keys and 'PRIMARY KEY' not in comment:
            print(f'ALTER TABLE `{target_workspace_name}`.{target_datastore_name}.{target_table_name} ALTER COLUMN `{col_name}` COMMENT "{comment}"')
            if not comment:
                comment = "PRIMARY KEY"
            else:
                comment = f"PRIMARY KEY; {comment}"
            spark.sql(f'ALTER TABLE `{target_workspace_name}`.{target_datastore_name}.{target_table_name} ALTER COLUMN `{col_name}` COMMENT "{comment}"')
        
        # Remove PRIMARY KEY from description if column is not a primary key
        if col_name not in primary_keys and 'PRIMARY KEY' in comment:
            comment = comment.replace("PRIMARY KEY; ", "").replace("PRIMARY KEY", "")
            print(f'ALTER TABLE `{target_workspace_name}`.{target_datastore_name}.{target_table_name} ALTER COLUMN `{col_name}` COMMENT "{comment}"')
            spark.sql(f'ALTER TABLE `{target_workspace_name}`.{target_datastore_name}.{target_table_name} ALTER COLUMN `{col_name}` COMMENT "{comment}"')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Schema Hashing Utilities
# 
# This section provides utilities for generating deterministic hash values from schema definitions. These hashes are used to detect schema changes efficiently without comparing full schema strings.

# CELL ********************

def get_md5_of_string(input_string):
    """
    Generate MD5 hash of a string for schema comparison and change detection.

    This function creates a deterministic hash value that uniquely represents a schema,
    enabling efficient comparison of schemas across different runs without storing or
    comparing the full schema definition.

    Args:
        input_string (str): String representation of schema or any content to hash

    Returns:
        str: 32-character hexadecimal MD5 hash

    Use Cases:
        - Schema change detection: Compare hash values to detect any schema modifications
        - Schema versioning: Track schema evolution over time
        - Efficient storage: Store compact hash instead of full schema definition

    Note:
        MD5 is used for its speed and deterministic output. Cryptographic security
        is not required for this use case.
    """
    return hashlib.md5(input_string.encode("UTF-8")).hexdigest()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8. Entity Resolution Functions
# 
# This section contains advanced functions for Master Data Management (MDM) and entity resolution. These functions enable matching and linking of records across different data sources using various comparison techniques including exact matching, fuzzy matching, and phonetic algorithms.
# 
# ### Key Capabilities
# - **N-Gram Filtering**: Pre-filters potential matches using MinHash LSH for scalability
# - **Multiple Comparison Types**: Exact, fuzzy (Levenshtein), phonetic (Soundex), and numeric percent difference
# - **Configurable Matching Logic**: Define auto-match and manual review thresholds
# - **Self-Join Support**: Compare records within the same dataset for deduplication

# CELL ********************

def ngram_matching(
    primary_dataset_df: DataFrame,
    secondary_dataset_df: DataFrame,
    primary_dataset_id_column: str,
    secondary_dataset_id_column: str
    ) -> DataFrame:
    """
    Perform efficient N-gram based pre-filtering for fuzzy matching using MinHash LSH.

    This function implements a scalable approach to identify potential matches between
    two datasets using Locality Sensitive Hashing (LSH). It significantly reduces the
    comparison space for expensive fuzzy matching operations.

    Args:
        primary_dataset_df (DataFrame): First dataset with 'id' and 'name' columns
        secondary_dataset_df (DataFrame): Second dataset with 'id' and 'name' columns
        primary_dataset_id_column (str): Name for primary dataset ID in output
        secondary_dataset_id_column (str): Name for secondary dataset ID in output

    Returns:
        DataFrame: Potential matches with columns [primary_dataset_id_column, secondary_dataset_id_column]

    Algorithm Steps:
        1. Tokenize text into words for comparison
        2. Convert words to TF (Term Frequency) feature vectors
        3. Apply MinHash LSH for efficient similarity detection
        4. Find pairs with Jaccard distance ≤ 0.8 (configurable threshold)

    Performance Benefits:
        - Reduces O(n²) comparisons to approximately O(n)
        - Enables fuzzy matching on large datasets (millions of records)
        - Configurable threshold balances precision vs. recall
    """
    # Step 1: Tokenize names into words for comparison
    tokenizer = Tokenizer(inputCol="name", outputCol="words")
    primary_dataset_words = tokenizer.transform(primary_dataset_df)
    secondary_dataset_words = tokenizer.transform(secondary_dataset_df)
    
    # Step 2: Convert words to Term Frequency feature vectors
    # Using 10,000 features provides good balance of accuracy and performance
    hash_tf = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=10000)
    primary_dataset_featurized = hash_tf.transform(primary_dataset_words)
    secondary_dataset_featurized = hash_tf.transform(secondary_dataset_words)
    
    # Step 3: Fit MinHash LSH model for similarity detection
    # 5 hash tables provides good accuracy with reasonable performance
    mh = MinHashLSH(inputCol="rawFeatures", outputCol="hashes", numHashTables=5)
    model = mh.fit(primary_dataset_featurized)
    
    # Step 4: Transform both datasets to include hash signatures
    primary_dataset_hashed = model.transform(primary_dataset_featurized)
    secondary_dataset_hashed = model.transform(secondary_dataset_featurized)
    
    # Step 5: Find similar pairs using approximate similarity join
    # Threshold of 0.8 captures most relevant matches while filtering noise
    similar_pairs = model.approxSimilarityJoin(
        primary_dataset_hashed, secondary_dataset_hashed, threshold=.3, distCol="JaccardDistance"
    )
    
    # Step 6: Extract and rename ID columns for output
    result = similar_pairs.select(
        f.col("datasetA.id").alias(primary_dataset_id_column),
        f.col("datasetB.id").alias(secondary_dataset_id_column)
    )

    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def _get_id_columns(
    config: Dict[str, Any]
) -> Tuple[str, str]:
    """Get primary and secondary dataset ID column names."""
    primary_id = f"{config['primary_dataset_alias']}_match_id"
    secondary_id = f"{config['secondary_dataset_alias']}_match_id"
    return primary_id, secondary_id

def _apply_self_comparison_filter(
    df_comparison: DataFrame, 
    config: Dict[str, Any]
) -> DataFrame:
    """Apply self-comparison filter to avoid duplicate comparisons."""
    if config['is_self_comparison']:
        primary_id, secondary_id = _get_id_columns(
            config = config
        )
        return df_comparison.filter(f.col(primary_id) < f.col(secondary_id))
    return df_comparison

def _merge_comparison_results(
    df_comparison_all: DataFrame, 
    df_comparison: DataFrame, 
    config: Dict[str, Any], 
    join_type: str = "FULL OUTER JOIN"
) -> DataFrame:
    """Merge new comparison results with accumulated results using SQL."""
    primary_id, secondary_id = _get_id_columns(
        config = config
    )
    id_columns = (primary_id, secondary_id)
    
    df_comparison_all_non_id_cols = [c for c in df_comparison_all.columns if c not in id_columns]
    df_comparison_non_id_cols = [c for c in df_comparison.columns if c not in id_columns]
    non_id_cols = df_comparison_non_id_cols + df_comparison_all_non_id_cols
    non_id_cols_str = ', '.join(non_id_cols)

    df_comparison.createOrReplaceTempView('df_comparison')
    df_comparison_all.createOrReplaceTempView('df_comparison_all')

    return spark.sql(f"""
        SELECT      COALESCE(a.{primary_id}, b.{primary_id}) `{primary_id}`
                    ,COALESCE(a.{secondary_id}, b.{secondary_id}) `{secondary_id}`
                    ,{non_id_cols_str}
        FROM        df_comparison_all as a
        {join_type} df_comparison as b
        ON          a.{primary_id} = b.{primary_id}
        AND         a.{secondary_id} = b.{secondary_id}
    """)

def _extract_entity_configuration(
    data_transformation_config: Dict[str, Any]
) -> Dict[str, Any]:
    """Extract and validate configuration parameters for entity resolution."""
    secondary_dataset_query = data_transformation_config.get("secondary_dataset_query")
    is_self_comparison = not bool(secondary_dataset_query)
    
    primary_dataset_comparison_fields = [field.strip() for field in data_transformation_config.get("primary_dataset_comparison_fields").split(",")]
    
    if is_self_comparison:
        secondary_dataset_comparison_fields = primary_dataset_comparison_fields
    else:
        secondary_dataset_comparison_fields = [field.strip() for field in data_transformation_config.get("secondary_dataset_comparison_fields").split(",")]

    comparison_types = [ctype.strip() for ctype in data_transformation_config.get("comparison_types").split(",")]

    Invalid_comparison_types = set(comparison_types) - set(['fuzzy', 'exact', 'soundex', 'percent_difference'])
    if Invalid_comparison_types:
        raise Exception(f"The following comparison types were invalid: {Invalid_comparison_types}. " +
                       "The only valid comparison types are `fuzzy`, `exact`, and `soundex`, `percent_difference`.")

    return {
        'secondary_dataset_query': secondary_dataset_query,
        'is_self_comparison': is_self_comparison,
        'primary_dataset_comparison_fields': primary_dataset_comparison_fields,
        'secondary_dataset_comparison_fields': secondary_dataset_comparison_fields,
        'comparison_types': comparison_types,
        'primary_dataset_alias': data_transformation_config.get("primary_dataset_alias").strip(),
        'secondary_dataset_alias': data_transformation_config.get("secondary_dataset_alias").strip(),
        'only_fuzzy_match_if_one_exact_match': data_transformation_config.get("only_fuzzy_match_if_one_exact_match", "true").strip().lower() == "true",
        'use_ngram_filtering_for_fuzzy_comparisons': data_transformation_config.get("use_ngram_filtering_for_fuzzy_comparisons", "false").strip().lower() == "true",
        'auto_match_logic': data_transformation_config.get("auto_match_logic"),
        'match_with_manual_review_logic': data_transformation_config.get("match_with_manual_review_logic").strip()
    }

def _prepare_datasets(
    primary_dataset_df: DataFrame, 
    config: Dict[str, Any]
) -> Tuple[DataFrame, DataFrame, List[Tuple[str, str, str]]]:
    """Prepare primary and secondary datasets with unique identifiers and aliased columns."""
    df1 = primary_dataset_df.alias(config['primary_dataset_alias']).withColumn("match_id", f.monotonically_increasing_id())

    if config['is_self_comparison']:
        df2 = df1.select("*").alias(config['secondary_dataset_alias'])
    else:
        df2 = spark.sql(config['secondary_dataset_query']).alias(config['secondary_dataset_alias']).withColumn("match_id", f.monotonically_increasing_id())

    # Alias columns with dataset prefixes
    df1_cols = [f.col(c).alias(f"{config['primary_dataset_alias']}_{c}") for c in df1.columns]
    df1 = df1.select(*df1_cols)
    df2_cols = [f.col(c).alias(f"{config['secondary_dataset_alias']}_{c}") for c in df2.columns]
    df2 = df2.select(*df2_cols)

    comparisons = list(zip(config['primary_dataset_comparison_fields'], config['secondary_dataset_comparison_fields'], config['comparison_types']))

    return df1, df2, comparisons

def _perform_exact_soundex_comparisons(
    df1: DataFrame, 
    df2: DataFrame, 
    comparisons: List[Tuple[str, str, str]], 
    config: Dict[str, Any]
) -> DataFrame:
    """Perform exact and soundex comparisons between datasets."""
    equality_comparisons = [(comparison[0], comparison[1], comparison[2]) 
                           for comparison in comparisons 
                           if comparison[2] in ('soundex', 'exact')]

    primary_id, secondary_id = _get_id_columns(
        config = config
    )
    df_comparison_all = spark.createDataFrame([], f"{primary_id} int, {secondary_id} int")

    for field1, field2, comparison_type in equality_comparisons:
        df1_field = f"{config['primary_dataset_alias']}_{field1}"
        df2_field = f"{config['secondary_dataset_alias']}_{field2}"

        if comparison_type == "exact":
            df1_comparison = df1.select(f.col(primary_id), f.col(df1_field))
            df2_comparison = df2.select(f.col(secondary_id), f.col(df2_field))
        elif comparison_type == "soundex":
            df1_comparison = df1.select(f.col(primary_id), f.soundex(f.col(df1_field)).alias(df1_field))
            df2_comparison = df2.select(f.col(secondary_id), f.soundex(f.col(df2_field)).alias(df2_field))

        df_comparison = df1_comparison.join(df2_comparison, f.col(df1_field) == f.col(df2_field), "inner")
        df_comparison = _apply_self_comparison_filter(
            df_comparison = df_comparison,
            config = config
        )
        df_comparison = df_comparison.withColumn(f"{df1_field}_{df2_field}_{comparison_type}", f.lit(1))
        df_comparison_all = _merge_comparison_results(
            df_comparison_all = df_comparison_all,
            df_comparison = df_comparison,
            config = config
        )

    return df_comparison_all

def _perform_fuzzy_comparisons(
    df1: DataFrame, 
    df2: DataFrame, 
    comparisons: List[Tuple[str, str, str]], 
    df_comparison_all: DataFrame, 
    config: Dict[str, Any]
) -> DataFrame:
    """Perform fuzzy matching using Levenshtein distance."""
    fuzzy_comparisons = [(comparison[0], comparison[1], comparison[2]) 
                         for comparison in comparisons 
                         if comparison[2] == 'fuzzy']

    primary_id, secondary_id = _get_id_columns(
        config = config
    )

    for field1, field2, _ in fuzzy_comparisons:
        df1_field = f"{config['primary_dataset_alias']}_{field1}"
        df2_field = f"{config['secondary_dataset_alias']}_{field2}"

        # Prepare cleaned fields for fuzzy comparison
        df1_comparison = df1.select(f.col(primary_id), f.col(df1_field)).withColumn(
            f"{df1_field}_clean4fuzz", 
            f.regexp_replace(f.regexp_replace(f.trim(f.lower(f.col(df1_field))), r"[^\w\s]", " "), r"\s+", " ")
        )
        df2_comparison = df2.select(f.col(secondary_id), f.col(df2_field)).withColumn(
            f"{df2_field}_clean4fuzz", 
            f.regexp_replace(f.regexp_replace(f.trim(f.lower(f.col(df2_field))), r"[^\w\s]", " "), r"\s+", " ")
        )

        # Calculate dataset sizes once for decision logic
        has_existing_matches = df_comparison_all.count() > 0
        
        # Decide which comparison strategy to use
        if config['only_fuzzy_match_if_one_exact_match'] and has_existing_matches:
            # Option 1: Join against existing exact/soundex matches (when flag is enabled)
            df_comparison = df_comparison_all.join(df1_comparison, primary_id, 'left') \
                                             .join(df2_comparison, secondary_id, 'left') \
                                             .select(primary_id, secondary_id, df1_field, df2_field,
                                                     f"{df1_field}_clean4fuzz", f"{df2_field}_clean4fuzz")
        elif config['use_ngram_filtering_for_fuzzy_comparisons']:
            # Use n-gram pre-filtering for scalability (only when explicitly enabled or auto-determined)
            df1_ngram = df1_comparison.select(f.col(primary_id).alias('id'), f.col(f"{df1_field}_clean4fuzz").alias('name'))
            df2_ngram = df2_comparison.select(f.col(secondary_id).alias('id'), f.col(f"{df2_field}_clean4fuzz").alias('name'))

            ngram_matches = ngram_matching(
                primary_dataset_df = df1_ngram,
                secondary_dataset_df = df2_ngram,
                primary_dataset_id_column = primary_id,
                secondary_dataset_id_column = secondary_id
            )
            df_comparison = ngram_matches.join(df1_comparison, primary_id, 'left') \
                                         .join(df2_comparison, secondary_id, 'left') \
                                         .select(primary_id, secondary_id, df1_field, df2_field, 
                                                f"{df1_field}_clean4fuzz", f"{df2_field}_clean4fuzz")
        else:
            # No n-gram filtering - use direct cross join
            df_comparison = df1_comparison.crossJoin(df2_comparison) \
                                          .select(primary_id, secondary_id, df1_field, df2_field,
                                                  f"{df1_field}_clean4fuzz", f"{df2_field}_clean4fuzz")

        df_comparison = _apply_self_comparison_filter(
            df_comparison = df_comparison,
            config = config
        )
        
        # Calculate normalized Levenshtein similarity
        df_comparison = df_comparison.withColumn(f"{df1_field}_{df2_field}_fuzzy", 
            1 - (f.levenshtein(f.col(f"{df1_field}_clean4fuzz"), f.col(f"{df2_field}_clean4fuzz")) / 
                 f.greatest(f.length(f.col(f"{df1_field}_clean4fuzz")), f.length(f.col(f"{df2_field}_clean4fuzz")))))

        df_comparison_all = _merge_comparison_results(
            df_comparison_all = df_comparison_all,
            df_comparison = df_comparison,
            config = config
        )

    return df_comparison_all

def _perform_percent_difference_comparisons(
    df1: DataFrame, 
    df2: DataFrame, 
    comparisons: List[Tuple[str, str, str]], 
    df_comparison_all: DataFrame, 
    config: Dict[str, Any]
) -> DataFrame:
    """Perform percent difference comparisons for numeric fields."""
    percent_difference_comparisons = [(comparison[0], comparison[1], comparison[2]) 
                                     for comparison in comparisons 
                                     if comparison[2] == 'percent_difference']

    primary_id, secondary_id = _get_id_columns(
        config = config
    )

    for field1, field2, _ in percent_difference_comparisons:
        df1_field = f"{config['primary_dataset_alias']}_{field1}"
        df2_field = f"{config['secondary_dataset_alias']}_{field2}"

        df1_comparison = df1.select(f.col(primary_id), f.col(df1_field))
        df2_comparison = df2.select(f.col(secondary_id), f.col(df2_field))

        # Check if we have existing comparisons to join against
        has_existing_matches = df_comparison_all.count() > 0
        
        if has_existing_matches:
            # Join against existing comparisons
            df_comparison = df_comparison_all.join(df1_comparison, primary_id, 'left') \
                                             .join(df2_comparison, secondary_id, 'left') \
                                             .select(primary_id, secondary_id, df1_field, df2_field)
        else:
            # No existing comparisons - do cross join
            df_comparison = df1_comparison.crossJoin(df2_comparison) \
                                          .select(primary_id, secondary_id, df1_field, df2_field)

        df_comparison = _apply_self_comparison_filter(
            df_comparison = df_comparison,
            config = config
        )
        
        # Calculate percent difference
        df_comparison = df_comparison.withColumn(
            f"{df1_field}_{df2_field}_percent_difference",
            f.abs((f.col(df2_field) - f.col(df1_field)) / f.col(df1_field) * 100)
        )

        df_comparison_all = _merge_comparison_results(
            df_comparison_all = df_comparison_all,
            df_comparison = df_comparison,
            config = config,
            join_type = "LEFT JOIN" if has_existing_matches else "FULL OUTER JOIN"
        )

    return df_comparison_all

def _apply_matching_rules(
    df_comparison_all: DataFrame, 
    config: Dict[str, Any]
) -> DataFrame:
    """Apply auto-match and manual review logic to categorize results."""
    df_comparison_all = df_comparison_all.alias('df_comparison_all')
    primary_id, secondary_id = _get_id_columns(
        config = config
    )

    auto_matches = df_comparison_all.filter(config['auto_match_logic']).alias('auto_matches')
    auto_matches = auto_matches.select(f.lit("Auto_Match").alias("match_outcome"), "*")

    non_auto_matches = df_comparison_all.join(
        auto_matches,
        on=(f.col(f"df_comparison_all.{primary_id}") == f.col(f"auto_matches.{primary_id}")) &
           (f.col(f"df_comparison_all.{secondary_id}") == f.col(f"auto_matches.{secondary_id}")),
        how="leftanti"
    ).select(df_comparison_all["*"])

    manual_review_matches = non_auto_matches.filter(config['match_with_manual_review_logic'])
    manual_review_matches = manual_review_matches.select(f.lit("Match_For_Manual_Review").alias("match_outcome"), "*")

    return auto_matches.unionByName(manual_review_matches).alias('matches')

def _apply_transitive_mapping(
    matches_full_dataset: DataFrame, 
    config: Dict[str, Any]
) -> DataFrame:
    """Use networkx to find transitive relationships between matched records."""
    import networkx as nx
    
    primary_id, secondary_id = _get_id_columns(
        config = config
    )
    mapping_pairs = matches_full_dataset.select(primary_id, secondary_id).toPandas().values.tolist()

    G = nx.Graph()
    G.add_edges_from(mapping_pairs)

    group_map = {}
    for group_id, component in enumerate(nx.connected_components(G), start=1):
        for node in component:
            group_map[node] = group_id

    # Handle empty group_map to avoid schema inference error
    if group_map:
        mapping_groups = spark.createDataFrame(group_map.items(), ["id", "match_group_id"])
    else:
        # Create empty DataFrame with explicit schema
        from pyspark.sql.types import StructType, StructField, LongType
        schema = StructType([
            StructField("id", LongType(), False),
            StructField("match_group_id", LongType(), False)
        ])
        mapping_groups = spark.createDataFrame([], schema)

    return matches_full_dataset.join(
        mapping_groups,
        matches_full_dataset[primary_id] == mapping_groups['id'],
        how="left"
    ).select(mapping_groups["match_group_id"], matches_full_dataset["*"])

def _assemble_final_results(
    df1: DataFrame, 
    df2: DataFrame, 
    matches: DataFrame, 
    matches_full_dataset_grouped: DataFrame, 
    config: Dict[str, Any]
) -> DataFrame:
    """Combine matched and unmatched records into final result set."""
    primary_id, secondary_id = _get_id_columns(
        config = config
    )

    # Join matches with original data
    matches_columns = set(matches.columns)
    df1_columns = [col.lower() for col in df1.columns if col.lower() not in matches_columns]
    df2_columns = [col for col in df2.columns if col.lower() not in matches_columns]

    matches_full_dataset = matches.join(df1, primary_id, 'left') \
                                  .join(df2, secondary_id, 'left') \
                                  .select(matches["*"], *df1_columns, *df2_columns)

    # Identify unmatched records
    if config['is_self_comparison']:
        df1_no_matches = df1.join(matches_full_dataset_grouped, 
                                 on=(df1[primary_id] == matches_full_dataset_grouped[primary_id]) | 
                                    (df1[primary_id] == matches_full_dataset_grouped[secondary_id]),
                                 how='leftanti')
    else:
        df1_no_matches = df1.join(matches_full_dataset_grouped, primary_id, 'leftanti')
    
    df1_no_matches = df1_no_matches.withColumn('match_outcome', f.lit('No_Match'))
    all_data = matches_full_dataset_grouped.unionByName(df1_no_matches, allowMissingColumns=True)

    if not config['is_self_comparison']:
        df2_no_matches = df2.join(matches_full_dataset_grouped, secondary_id, 'leftanti') \
                            .withColumn('match_outcome', f.lit('No_Match'))
        all_data = all_data.unionByName(df2_no_matches, allowMissingColumns=True)

    # Clean up intermediate columns
    all_data_columns = [col for col in all_data.columns if not col.endswith('_clean4fuzz')]
    return all_data.select(*all_data_columns)

def entity_resolution(
    primary_dataset_df: DataFrame, 
    data_transformation_config: Dict[str, Any]
) -> DataFrame:
    """
    Comprehensive entity resolution with multiple comparison techniques for record matching.

    This function implements a sophisticated entity matching system that combines multiple
    comparison techniques to identify duplicate or related records across datasets. It supports
    both cross-dataset matching and self-deduplication scenarios.

    Args:
        primary_dataset_df (DataFrame): Primary dataset for entity resolution
        data_transformation_config (dict): Configuration dictionary containing:
            - secondary_dataset_query: SQL query for secondary dataset (optional)
            - primary_dataset_comparison_fields: Comma-separated field names
            - secondary_dataset_comparison_fields: Comma-separated field names
            - comparison_types: Comma-separated comparison types
            - primary_dataset_alias: Alias for primary dataset
            - secondary_dataset_alias: Alias for secondary dataset
            - only_fuzzy_match_if_one_exact_match: "true"/"false" (default: "true")
            - use_ngram_filtering_for_fuzzy_comparisons: "true"/"false" (default: "false")
                * "true": Always use MinHash LSH n-gram pre-filtering for fuzzy matches
                * "false": Never use n-gram filtering (direct cross join + Levenshtein for fuzzy)
            - auto_match_logic: SQL expression for automatic matches
            - match_with_manual_review_logic: SQL expression for review matches

    Returns:
        DataFrame: Complete dataset with match outcomes:
            - All records from primary dataset
            - match_outcome column: 'Auto_Match', 'Match_For_Manual_Review', or 'No_Match'
            - Comparison score columns for each field comparison
            - Matched records from secondary dataset (if applicable)

    Comparison Types Supported:
        1. exact: Direct equality comparison
        2. soundex: Phonetic matching for names
        3. fuzzy: Levenshtein distance-based similarity (0-1 scale)
        4. percent_difference: Numeric difference as percentage

    N-gram Filtering Behavior (for fuzzy matching only):
        - Word-level tokenization works well for multi-word text (e.g., company names)
        - Single-word comparisons (e.g., "Robert" vs "Roberta") may not benefit from n-grams
        - For small datasets, cross join is often faster than n-gram overhead
        - N-gram filtering only applies to fuzzy (Levenshtein) comparisons, not exact/soundex
        - Default is "false" - use "true" only for large multi-word text datasets

    Processing Flow:
        1. Prepare datasets with unique identifiers
        2. Perform exact and soundex comparisons
        3. Apply fuzzy matching (with optional pre-filtering)
        4. Calculate percent differences for numeric fields
        5. Apply matching rules to categorize results
        6. Combine matched and unmatched records

    Performance Optimizations:
        - Optional n-gram pre-filtering for large-scale fuzzy matching
        - Incremental join approach to build comparison results
        - Self-join optimization to avoid duplicate comparisons
    """
    running_entity_resolution_log_info = f"Running entity resolution logic."
    log_and_print(running_entity_resolution_log_info)
    
    config = _extract_entity_configuration(
        data_transformation_config = data_transformation_config
    )
    df1, df2, comparisons = _prepare_datasets(
        primary_dataset_df = primary_dataset_df,
        config = config
    )
    
    # Perform all comparison types
    df_comparison_all = _perform_exact_soundex_comparisons(
        df1 = df1,
        df2 = df2,
        comparisons = comparisons,
        config = config
    )
    df_comparison_all = _perform_fuzzy_comparisons(
        df1 = df1,
        df2 = df2,
        comparisons = comparisons,
        df_comparison_all = df_comparison_all,
        config = config
    )
    df_comparison_all = _perform_percent_difference_comparisons(
        df1 = df1,
        df2 = df2,
        comparisons = comparisons,
        df_comparison_all = df_comparison_all,
        config = config
    )
    
    matches = _apply_matching_rules(
        df_comparison_all = df_comparison_all,
        config = config
    )
    
    # Join with original data and apply transitive mapping
    primary_id, secondary_id = _get_id_columns(
        config = config
    )
    matches_columns = set(matches.columns)
    df1_columns = [col.lower() for col in df1.columns if col.lower() not in matches_columns]
    df2_columns = [col for col in df2.columns if col.lower() not in matches_columns]

    matches_full_dataset = matches.join(df1, primary_id, 'left') \
                                  .join(df2, secondary_id, 'left') \
                                  .select(matches["*"], *df1_columns, *df2_columns)
    
    matches_full_dataset_grouped = _apply_transitive_mapping(
        matches_full_dataset = matches_full_dataset,
        config = config
    )
    
    return _assemble_final_results(
        df1 = df1,
        df2 = df2,
        matches = matches,
        matches_full_dataset_grouped = matches_full_dataset_grouped,
        config = config
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================================
# CUSTOM FUNCTION LOADING CONSTANTS
# ===========================================================================================

# Path for custom function .py files in lakehouse Files folder
# CI/CD deploys notebooks to: /lakehouse/default/Files/custom_functions/
CUSTOM_FUNCTIONS_FOLDER = "custom_functions"


def instantiate_notebook(
    notebook_name: str,
    max_retries: int = 3
) -> None:
    """
    Load custom functions into the current Spark session using a dual-path strategy.
    
    This function supports two loading methods:
    1. **Production path (preferred)**: Load from .py files in lakehouse Files/custom_functions/
    2. **Development fallback**: Use getDefinition() API to load from notebook
    
    The function first attempts to load from .py files (deployed via CI/CD), 
    then falls back to the notebook API for development environments where 
    notebooks haven't been deployed yet.

    Args:
        notebook_name (str): Name of the custom function module/notebook to load.
                            For .py files, this is the filename without extension.
                            For notebooks, this is the Fabric notebook name.
        max_retries (int): Maximum retry attempts for notebook API fallback (default: 3)
    
    Example:
        >>> # In production (loads from Files/custom_functions/NB_Custom_Products.py)
        >>> instantiate_notebook("NB_Custom_Products")
        >>> 
        >>> # In dev (falls back to notebook API if .py file doesn't exist)
        >>> instantiate_notebook("NB_Custom_Products")
        >>> 
        >>> # Now custom functions are available
        >>> result = my_custom_transformation(df)
    
    Note:
        - Production: .py files are read and executed via IPython run_cell()
        - Development: Notebook API + IPython execution (fallback when .py files not deployed)
        - Both paths execute in the shared session namespace so custom functions
          can reference helpers from NB_Helper_Functions (e.g., _mount_lakehouse_for_local_access)
    """
    # Try loading from lakehouse .py file first (production path)
    if _try_load_from_lakehouse_file(notebook_name):
        return
    
    # Fall back to notebook API (development path)
    log_and_print(f"No .py file found for '{notebook_name}', falling back to notebook API (dev mode).")
    _load_via_notebook_api(notebook_name, max_retries)


def _try_load_from_lakehouse_file(module_name: str) -> bool:
    """
    Attempt to load custom functions from a .py file in the lakehouse Files folder.
    
    This is the preferred production method - .py files are deployed via CI/CD
    and executed in the current IPython session so that custom functions can
    reference helper functions from NB_Helper_Functions (e.g., log_and_print,
    _mount_lakehouse_for_local_access, _get_datastore_config).
    
    Args:
        module_name (str): Name of the module (without .py extension)
    
    Returns:
        bool: True if successfully loaded, False if file doesn't exist
    
    Note:
        The .py file is read and executed via IPython's run_cell() rather than
        importlib. This ensures functions are defined in the shared session
        namespace where they can access helper functions from other notebooks.
        Files are loaded from: /lakehouse/default/Files/custom_functions/
    """
    # Construct the full path to the .py file
    # In Fabric, /lakehouse/default/ maps to the default attached lakehouse
    py_file_path = f"/lakehouse/default/Files/{CUSTOM_FUNCTIONS_FOLDER}/{module_name}.py"
    
    # Check if the file exists
    try:
        file_exists = notebookutils.fs.exists(f"Files/{CUSTOM_FUNCTIONS_FOLDER}/{module_name}.py")
    except Exception:
        # If we can't check (e.g., no lakehouse attached), assume file doesn't exist
        file_exists = False
    
    if not file_exists:
        return False
    
    log_and_print(f"Loading custom functions from '{py_file_path}' (production mode).")
    
    try:
        # Read the .py file content
        with open(py_file_path, 'r', encoding='utf-8') as f:
            code = f.read()
        
        # Execute in the IPython session namespace so functions can reference
        # helpers like _mount_lakehouse_for_local_access, log_and_print, etc.
        result = get_ipython().run_cell(code)
        if result.error_in_exec is not None:
            raise result.error_in_exec
        
        log_and_print(f"Successfully loaded custom functions from '{module_name}.py'.")
        return True
        
    except Exception as e:
        log_and_print(f"Failed to load from .py file: {str(e)}. Will try notebook API.")
        return False


def _load_via_notebook_api(
    notebook_name: str,
    max_retries: int
) -> None:
    """
    Load custom functions using the Fabric notebook getDefinition() API.
    
    This is the development fallback method - used when .py files haven't been
    deployed via CI/CD. Retrieves notebook content via API and executes code 
    cells via IPython.
    
    In production environments, .py files should be deployed to the lakehouse
    Files folder, which loads directly from disk instead of this method.
    
    Args:
        notebook_name (str): Name of the Fabric notebook to load
        max_retries (int): Maximum number of retry attempts
    
    Raises:
        Exception: If all retry attempts fail
    """
    last_exception = None
    
    for attempt in range(1, max_retries + 1):
        try:
            log_and_print(f"Instantiating notebook '{notebook_name}' via API (attempt {attempt}/{max_retries}).")
            
            code_cells = _get_notebook_code_cells(notebook_name)
            _execute_code_cells(code_cells)
            
            log_and_print(f"Successfully instantiated notebook '{notebook_name}'.")
            return  # Success
            
        except Exception as e:
            last_exception = e
            log_and_print(f"Attempt {attempt}/{max_retries} failed: {str(e)}")
            
            if attempt < max_retries:
                wait_time = 2 ** attempt  # Exponential backoff: 2, 4, 8 seconds
                log_and_print(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
    
    raise Exception(f"Failed to instantiate notebook '{notebook_name}' after {max_retries} attempts. Last error: {str(last_exception)}")


def _get_notebook_code_cells(notebook_name: str) -> list:
    """
    Retrieve and parse a Fabric notebook, returning only the code cells.
    
    Args:
        notebook_name (str): Name of the Fabric notebook to retrieve
        
    Returns:
        list: List of code cell dictionaries from the notebook
    """
    # Get notebook definition (returns .ipynb JSON format)
    notebook_definition = notebookutils.notebook.getDefinition(notebook_name)
    log_and_print(f"Notebook definition retrieved ({len(notebook_definition)} characters total, showing first 500): {notebook_definition[:500]}...")
    
    # Parse the notebook JSON and extract code cells
    notebook_json = json.loads(notebook_definition)
    cells = notebook_json.get('cells', [])
    code_cells = [cell for cell in cells if cell.get('cell_type') == 'code']
    
    log_and_print(f"Found {len(code_cells)} code cells to execute.")
    return code_cells


def _execute_code_cells(code_cells: list) -> None:
    """
    Execute a list of notebook code cells using IPython's run_cell().
    
    Args:
        code_cells (list): List of cell dictionaries with 'source' keys
    """
    ipython = get_ipython()
    
    for cell in code_cells:
        source = cell.get('source', [])
        # Handle both list of lines and single string formats
        code = ''.join(source) if isinstance(source, list) else source
        if code.strip():  # Skip empty cells
            result = ipython.run_cell(code)
            if result.error_in_exec is not None:
                raise result.error_in_exec

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def execute_scd2_post_processing(
    primary_keys: list,
    target_abfss_path: str,
    lakehouse_table_output: bool,
    enable_scd2_dimension: bool,
    total_records_processed: int
) -> None:
    """
    Execute SCD2 post-processing to fix date ranges for dimension tables.
    
    This function adjusts SCD2 end dates for historical records to ensure proper
    temporal accuracy in slowly changing dimension tables.
    
    Args:
        primary_keys (list): List of primary key column names
        target_abfss_path (str): ABFSS path to the target Delta table
        lakehouse_table_output: Whether output is a lakehouse table
        enable_scd2_dimension: Whether SCD2 is enabled
        total_records_processed: Number of records processed
    
    Returns:
        None
    
    """
    if lakehouse_table_output and enable_scd2_dimension and total_records_processed > 0:
        log_and_print("Running post processing for SCD2 dimension tables.")
        post_processing_scd2_update(
            primary_keys = primary_keys,
            target_abfss_path = target_abfss_path
        )

def drop_external_table_for_shortcut(
    target_table_name: str,
    first_run: bool,
    output_external_location: bool, 
    lakehouse_table_output: bool
) -> None:
    """
    Drop external table reference to enable OneLake shortcut creation.
    
    After writing to an external location (e.g., ADLS Gen2), the table metadata
    must be dropped to allow OneLake shortcuts to be created for the Delta files.
    
    Args:
        target_table_name (str): Fully qualified table name (workspace.datastore.schema.table)
        first_run (bool): first run for dataset
        output_external_location (bool), output is external location in adls gen2 
        lakehouse_table_output: output is lakehouse table
    Returns:
        None
    
    """
    if first_run and output_external_location and lakehouse_table_output:
        log_and_print("Dropping external table to enable OneLake shortcut creation.")
        spark.sql(f"DROP TABLE {target_table_name}")

def check_recent_vacuum_operations(
    target_table_name: str,
    history_limit: int = 50
) -> tuple:
    """
    Check table history for recent vacuum operations.
    
    This function queries the Delta table history to determine if vacuum operations
    have been run recently, which helps avoid redundant vacuum executions.
    
    Args:
        target_table_name (str): Fully qualified table name
        history_limit (int): Number of recent operations to check (default: 50)
    
    Returns:
        tuple: (total_operations, number_of_vacuum_operations)
            - total_operations (int): Total number of operations in table history
            - number_of_vacuum_operations (int): Count of VACUUM START operations
    
    """
    table_operations = spark.sql(
        f"DESCRIBE HISTORY {target_table_name}"
    )
    
    total_operations = table_operations.count()
    
    number_of_vacuum_operations = (
        table_operations
        .orderBy("timestamp", ascending = False)
        .limit(history_limit)
        .filter("operation = 'VACUUM START'")
        .count()
    )
    
    return total_operations, number_of_vacuum_operations

def execute_vacuum_on_table(target_abfss_path: str) -> None:
    """
    Execute vacuum operation on a Delta table to remove old files.
    
    Vacuum removes data files that are no longer referenced by the Delta table,
    freeing up storage space. Files are removed based on the retention policy.
    
    Args:
        target_abfss_path (str): ABFSS path to the target Delta table
    
    Returns:
        None
    
    Note:
        Default retention period is 7 days. Files older than this will be removed.
    
    Example:
        >>> execute_vacuum_on_table(
        ...     target_abfss_path = 'abfss://workspace@onelake.dfs.fabric.microsoft.com/Gold.Lakehouse/Tables/fact_sales'
        ... )
    """
    log_and_print("Running vacuum command against table.")
    target_table = DeltaTable.forPath(spark, target_abfss_path)
    target_table.vacuum()

def execute_vacuum_if_needed(
    lakehouse_table_output: bool,
    target_table_name: str,
    target_abfss_path: str,
    min_operations_threshold: int = 50
) -> None:
    """
    Execute vacuum operation only if needed based on table history.
    
    This function checks if vacuum has been run recently and only executes it
    if no recent vacuum operations are found and the table has sufficient history.
    
    Args:
        target_table_name (str): Fully qualified table name
        target_abfss_path (str): ABFSS path to the target Delta table
        min_operations_threshold (int): Minimum operations before vacuum (default: 50)
    
    Returns:
        None
    
    """
    if not lakehouse_table_output:
        return

    total_operations, number_of_vacuum_operations = check_recent_vacuum_operations(
        target_table_name = target_table_name
    )
    
    # Run vacuum if no recent operations found and table has sufficient history
    if number_of_vacuum_operations == 0 and total_operations > min_operations_threshold:
        execute_vacuum_on_table(target_abfss_path = target_abfss_path)
    elif number_of_vacuum_operations > 0:
        log_and_print(f"Skipping vacuum - {number_of_vacuum_operations} recent vacuum operation(s) detected.")
    else:
        log_and_print(f"Skipping vacuum - table has only {total_operations} operations (threshold: {min_operations_threshold}).")

def cleanup_temporary_files(
    file_staging_path: str,
    clean_up_temporary_path: bool
) -> None:
    """
    Remove temporary CSV files created during Excel or XML ingestion.
    
    When ingesting Excel or XML files, temporary CSV conversions are created
    in a staging area. This function cleans up those temporary files.
    
    Args:
        file_staging_path (str): Path to the temporary file staging directory
        clean_up_temporary_path (bool): whether there's temporary data to delete
    
    Returns:
        None
    
    """
    if clean_up_temporary_path:
        log_and_print("Deleting temporary CSV files used when ingesting Excel or XML data.")
        notebookutils.fs.rm(file_staging_path, True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_schema_if_not_exists(
    target_abfss_path: str,
    lakehouse_table_output: bool
) -> None:
    """    
    This function parses the target_abfss_path to extract the schema,
    then creates the schema in the target datastore using notebookutils if it doesn't already exist.
    
    Args:
        target_abfss_path (str): Abfss path for schema
        lakehouse_table_output (bool): Whether output is lakehouse table
    
    Note:
        - Idempotent operation - safe to call multiple times
    """
    if not lakehouse_table_output:
        return

    # Extract path without table name
    schema_abfss_path = target_abfss_path.rsplit('/', 1)[0]
    
    schema_name = schema_abfss_path.rsplit('/', 1)[1]

    log_and_print(f"Creating schema, `{schema_name}`, for lakehouse if it doesn't exist.")

    notebookutils.fs.mkdirs(schema_abfss_path)  

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def drop_table_for_full_reload(
    target_table_name: str
) -> None:
    """
    Drop the target table to prepare for a full reload.
    
    This function drops the existing table if it exists, allowing a complete
    refresh of the data. Used when full_reload = 'Yes' in metadata configuration.
    
    Args:
        target_table_name (str): Fully qualified table name (schema.table)
    
    Returns:
        None
    
    Example:
        >>> drop_table_for_full_reload(
        ...     target_workspace_name = 'Analytics Workspace',
        ...     target_table_name = '`workspacename`.datastorename.dbo.customers'
        ... )
    """
    full_reload_log_info = "Running full reload process."
    log_and_print(full_reload_log_info)
    
    drop_table_statement = f"DROP TABLE IF EXISTS {target_table_name}"
    spark.sql(drop_table_statement)


def check_target_table_exists(
    target_abfss_path: str
) -> bool:
    """
    Check if the target table exists in the specified workspace and datastore.
    
    This function verifies table existence using Spark's catalog, which is used
    to determine if this is a first-time load or an incremental update.
    
    Args:
        target_abfss_path (str): abfss path for target table
    
    Returns:
        bool: True if table exists, False otherwise
    
    Example:
        >>> exists = check_target_table_exists(
        ...     target_abfss_path = ''
        ... )
        >>> print(f"Table exists: {exists}")
    """
    target_table_exists = notebookutils.fs.exists(target_abfss_path)
    table_exists_log_info = f"Table exists: {target_table_exists}"
    log_and_print(table_exists_log_info)
    return target_table_exists

def determine_first_run_and_table_existence(
    target_table_name: str,
    full_reload: str,
    watermark_value: str,
    target_abfss_path: str,
    lakehouse_table_output: bool
) -> tuple:
    """
    Determine if this is a first run and whether the target table exists.
    
    This orchestrator function handles the full reload workflow and table existence
    checking logic, coordinating between different helper functions.
    
    Args:
        target_table_name (str): Name of the target table
        full_reload (str): 'Yes' to drop and recreate table, otherwise incremental
        watermark_value (str): watermark value for incremental data processing
        target_abfss_path (str): abfss path for target table
        lakehouse_table_output (bool): whether target is a lakehouse table
    Returns:
        tuple: (first_run, target_table_exists, lakehouse_table_output, watermark_value)
            - first_run (bool): True if this is the first load for the table
            - target_table_exists (bool): True if the target table currently exists
            - watermark_value (str): Reset to empty string for full reload
    
    Example:
        >>> first_run, exists, is_lakehouse, watermark = determine_first_run_and_table_existence(
        ...     target_table_name = 'workspacename.datastorename.dbo.customers',
        ...     full_reload = 'No',
        ...     watermark_value = ''
        ...     lakehouse_table_output = True,
        ... )
    """
    # Initialize variables
    first_run = False
    target_table_exists = False
    
    if not lakehouse_table_output:
        # No target table specified (e.g., writing to files or warehouse)
        return first_run, target_table_exists, watermark_value
    
    # Check target table existence and handle full reload
    if full_reload == 'Yes':
        drop_table_for_full_reload(
            target_table_name = target_table_name
        )
        # Reset watermark for full reload
        watermark_value = ""
        # Reset first run flag for full reload
        first_run = True
        target_table_exists = False
    else:
        target_table_exists = check_target_table_exists(
            target_abfss_path = target_abfss_path
        )
        # Set first run flag based on table existence
        first_run = not target_table_exists
    
    return first_run, target_table_exists, watermark_value

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
