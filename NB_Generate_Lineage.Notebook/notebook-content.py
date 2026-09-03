# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# MARKDOWN ********************

# # 📊 Data Pipeline Lineage Generator
# 
# This notebook generates data lineage records by analyzing:
# - **Metadata tables**: Orchestration, Primary Config, Advanced Config
# - **Execution logs**: Data_Pipeline_Logs
# 
# The lineage is built using **NetworkX** graph analysis and persisted to `Data_Pipeline_Lineage` 
# table for consumption in Power BI with Copilot narrative support.
# 
# ## Features
# - Track source-to-target relationships over time
# - Capture medallion layer context (Bronze → Silver → Gold)
# - Identify external source systems (Oracle, SQL Server, etc.)
# - Generate natural language summaries for Copilot
# - Support impact analysis (upstream/downstream dependencies)

# CELL ********************

# Parameters (can be overridden when running the notebook)
# Set trigger_name to filter lineage to a specific trigger, or leave None for all triggers
trigger_name = None  # e.g., "SalesDataProduct"

# Set execution_id to filter to a specific pipeline run, or leave None for all runs
execution_id = None  # e.g., "abc123-def456-..."

# Name of your metadata warehouse
metadata_warehouse_name = "Metadata"

# Whether to persist lineage to the Data_Pipeline_Lineage table
persist_to_table = True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. Import Libraries and Define Lineage Functions

# CELL ********************

import networkx as nx
from datetime import datetime
import uuid
from typing import Dict, List, Optional, Tuple, Any
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType, BooleanType
)
from pyspark.sql import functions as f
import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants

print("✅ Libraries imported successfully")
print(f"NetworkX version: {nx.__version__}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define helper classes and functions

class LineageNode:
    """Represents a node in the lineage graph (source or target entity)"""
    
    def __init__(
        self,
        entity_name: str,
        datastore: str,
        medallion_layer: str,
        system_type: str = None,
        node_type: str = "entity"
    ):
        self.entity_name = entity_name
        self.datastore = datastore
        self.medallion_layer = medallion_layer
        self.system_type = system_type
        self.node_type = node_type
        
        # Build node_id: avoid double-prepending datastore if entity_name already includes it
        # This handles Delta table sources where table_name is already a 3-part name (datastore.schema.table)
        if datastore and entity_name:
            if entity_name.lower().startswith(f"{datastore.lower()}."):
                # entity_name already includes the datastore prefix (e.g., "bronze.dbo.sales")
                self.node_id = entity_name
            else:
                # entity_name is just schema.table or table, prepend the datastore
                self.node_id = f"{datastore}.{entity_name}"
        else:
            self.node_id = entity_name or datastore or "unknown"
    
    def __hash__(self):
        return hash(self.node_id)
    
    def __eq__(self, other):
        return isinstance(other, LineageNode) and self.node_id == other.node_id
    
    def to_dict(self) -> Dict:
        return {
            "entity_name": self.entity_name,
            "datastore": self.datastore,
            "medallion_layer": self.medallion_layer,
            "system_type": self.system_type,
            "node_type": self.node_type,
            "node_id": self.node_id
        }


def derive_medallion_layer(datastore: str) -> str:
    """Derive medallion layer from datastore name"""
    if not datastore:
        return "Unknown"
    datastore_lower = datastore.lower()
    if "bronze" in datastore_lower:
        return "Bronze"
    elif "silver" in datastore_lower:
        return "Silver"
    elif "gold" in datastore_lower:
        return "Gold"
    return "Unknown"


def derive_source_system_type(config_df, table_id: int) -> str:
    """Derive source system type from primary configuration"""
    source_configs = config_df.filter(
        (config_df.Table_ID == table_id) & 
        (config_df.Configuration_Category == "source_details")
    ).collect()
    
    source_type = None
    has_table_name = False
    has_wildcard_path = False
    
    for row in source_configs:
        config_name = row.Configuration_Name
        config_value = row.Configuration_Value
        
        if config_name == "source":
            source_type = config_value
        elif config_name == "table_name":
            has_table_name = True
        elif config_name == "wildcard_folder_path":
            has_wildcard_path = True
    
    if source_type:
        return source_type
    elif has_wildcard_path:
        return "file"
    elif has_table_name:
        return "delta_table"
    
    return "unknown"


def derive_source_entity_and_datastore(config_df, table_id: int) -> Tuple[str, str, str]:
    """Derive source entity, datastore, and medallion layer from primary configuration"""
    source_configs = config_df.filter(
        (config_df.Table_ID == table_id) & 
        (config_df.Configuration_Category == "source_details")
    ).collect()
    
    source_entity = None
    source_datastore = None
    schema_name = None
    table_name_val = None
    wildcard_path = None
    datastore_name = None
    source_type = None
    
    for row in source_configs:
        config_name = row.Configuration_Name
        config_value = row.Configuration_Value
        
        if config_name == "table_name":
            # For Delta tables, table_name is the full 3-part name (datastore.schema.table)
            # For external databases, table_name is just the source table name
            source_entity = config_value
            table_name_val = config_value  # Store the raw table name value
            parts = config_value.split(".") if config_value else []
            if len(parts) == 3:
                source_datastore = parts[0]
                source_entity = config_value
            elif len(parts) == 2:
                source_entity = config_value
        elif config_name == "schema_name":
            schema_name = config_value
        elif config_name == "source":
            source_type = config_value
        elif config_name == "wildcard_folder_path":
            wildcard_path = config_value
        elif config_name == "datastore_name":
            datastore_name = config_value
    
    # For external database sources (Oracle, SQL Server, PostgreSQL, etc.)
    if source_type and source_type not in ["delta_table", "file"]:
        # Build the source entity name: schema_name.table_name (no prefix needed - source_type is captured separately)
        if schema_name and table_name_val:
            source_entity = f"{schema_name}.{table_name_val}"
        elif schema_name:
            source_entity = schema_name
        elif table_name_val:
            source_entity = table_name_val
        else:
            source_entity = "unknown"
        source_datastore = source_type
        return source_entity, source_datastore, "External"
    
    # For file sources
    if wildcard_path:
        source_entity = f"Files/{wildcard_path}"
        source_datastore = datastore_name or "bronze"
        return source_entity, source_datastore, derive_medallion_layer(source_datastore)
    
    # For delta table sources
    if source_entity:
        source_medallion = derive_medallion_layer(source_datastore) if source_datastore else "Unknown"
        return source_entity, source_datastore, source_medallion
    
    return None, None, None


def extract_extended_source_details(config_df, table_id: int, workspace_variables: dict) -> Dict:
    """
    Extract extended source details for lineage including connection_id and ABFSS path.
    
    Returns:
        Dict with keys: connection_id, workspace_name, abfss_path, is_external
    """
    source_configs = config_df.filter(
        (config_df.Table_ID == table_id) & 
        (config_df.Configuration_Category == "source_details")
    ).collect()
    
    # Parse all source config values
    config_values = {}
    for row in source_configs:
        config_values[row.Configuration_Name] = row.Configuration_Value
    
    source_type = config_values.get("source")
    datastore_name = config_values.get("datastore_name")
    table_name = config_values.get("table_name")
    wildcard_path = config_values.get("wildcard_folder_path")
    connection_id = config_values.get("connection_id")  # Only for external sources
    
    result = {
        "connection_id": None,
        "workspace_name": None,
        "abfss_path": None,
        "is_external": False
    }
    
    # External database sources - only capture connection_id
    if source_type and source_type not in ["delta_table", "file"]:
        result["is_external"] = True
        result["connection_id"] = connection_id
        return result
    
    # For delta_table sources, extract datastore from the 3-part table_name if not explicitly set
    # e.g., table_name = "bronze.dbo.orders" -> datastore_name = "bronze"
    if not datastore_name and table_name:
        parts = table_name.split(".") if table_name else []
        if len(parts) == 3:
            datastore_name = parts[0]
    
    # Internal Fabric sources - build ABFSS path
    if datastore_name:
        # Get workspace variables for this datastore
        workspace_id_key = f"{datastore_name.lower()}_datastore_workspace_id"
        datastore_id_key = f"{datastore_name.lower()}_datastore_id"
        workspace_name_key = f"{datastore_name.lower()}_datastore_workspace_name"
        
        workspace_id = workspace_variables.get(workspace_id_key)
        datastore_id = workspace_variables.get(datastore_id_key)
        result["workspace_name"] = workspace_variables.get(workspace_name_key)
        
        if workspace_id and datastore_id:
            if wildcard_path:
                # File source
                result["abfss_path"] = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{datastore_id}/Files/{wildcard_path}"
            elif table_name:
                # Table source - parse schema.table from 3-part name
                parts = table_name.split(".") if table_name else []
                if len(parts) == 3:
                    schema_name = parts[1].lower()
                    tbl_name = parts[2].lower()
                    result["abfss_path"] = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{datastore_id}/Tables/{schema_name}/{tbl_name}"
                elif len(parts) == 2:
                    schema_name = parts[0].lower()
                    tbl_name = parts[1].lower()
                    result["abfss_path"] = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{datastore_id}/Tables/{schema_name}/{tbl_name}"
    
    return result


def extract_target_details(orchestration_row, workspace_variables: dict) -> Dict:
    """
    Extract target details for lineage including workspace name and ABFSS path.
    
    Returns:
        Dict with keys: workspace_name, abfss_path
    """
    target_datastore = orchestration_row.Target_Datastore.strip().lower()
    target_entity = orchestration_row.Target_Entity
    
    # Get workspace variables for target datastore
    workspace_id_key = f"{target_datastore}_datastore_workspace_id"
    datastore_id_key = f"{target_datastore}_datastore_id"
    workspace_name_key = f"{target_datastore}_datastore_workspace_name"
    
    workspace_id = workspace_variables.get(workspace_id_key)
    datastore_id = workspace_variables.get(datastore_id_key)
    workspace_name = workspace_variables.get(workspace_name_key)
    
    result = {
        "workspace_name": workspace_name,
        "abfss_path": None
    }
    
    if workspace_id and datastore_id and target_entity:
        if "/" in target_entity:
            # File target
            result["abfss_path"] = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{datastore_id}/Files/{target_entity}"
        else:
            # Table target - parse schema.table
            parts = target_entity.split(".")
            if len(parts) == 2:
                schema_name = parts[0].lower()
                tbl_name = parts[1].lower()
                result["abfss_path"] = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{datastore_id}/Tables/{schema_name}/{tbl_name}"
    
    return result


def build_source_details_from_node(source_node: Dict, workspace_variables: dict) -> Dict:
    """
    Build source details (workspace_name, abfss_path) directly from a source node's attributes.
    
    Used for custom function sources where the Table_ID's primary config doesn't have
    the source info (because custom functions can read from multiple tables).
    
    Args:
        source_node: Node attributes dict with entity_name, datastore, medallion_layer, etc.
        workspace_variables: Dict of workspace variables from Datastore_Configuration
    
    Returns:
        Dict with keys: connection_id, workspace_name, abfss_path, is_external
    """
    result = {
        "connection_id": None,
        "workspace_name": None,
        "abfss_path": None,
        "is_external": False
    }
    
    entity_name = source_node.get("entity_name", "")
    datastore = source_node.get("datastore", "")
    medallion_layer = source_node.get("medallion_layer", "")
    
    # External sources don't have ABFSS paths
    if medallion_layer == "External":
        result["is_external"] = True
        return result
    
    # Get datastore from entity_name if not set (e.g., "bronze.dbo.orders" -> "bronze")
    if not datastore and entity_name:
        parts = entity_name.split(".")
        if len(parts) >= 2:
            datastore = parts[0]
    
    if not datastore:
        return result
    
    # Get workspace variables for this datastore
    datastore_lower = datastore.lower()
    workspace_id_key = f"{datastore_lower}_datastore_workspace_id"
    datastore_id_key = f"{datastore_lower}_datastore_id"
    workspace_name_key = f"{datastore_lower}_datastore_workspace_name"
    
    workspace_id = workspace_variables.get(workspace_id_key)
    datastore_id = workspace_variables.get(datastore_id_key)
    result["workspace_name"] = workspace_variables.get(workspace_name_key)
    
    if not workspace_id or not datastore_id:
        return result
    
    # Parse entity_name to build ABFSS path
    # entity_name could be: "bronze.dbo.orders" (3-part) or "dbo.orders" (2-part) or "Files/path/file.csv"
    if "Files/" in entity_name or "/" in entity_name:
        # File source
        file_path = entity_name.split(":", 1)[-1] if ":" in entity_name else entity_name
        result["abfss_path"] = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{datastore_id}/{file_path}"
    else:
        # Table source
        parts = entity_name.split(".")
        if len(parts) == 3:
            # datastore.schema.table
            schema_name = parts[1].lower()
            tbl_name = parts[2].lower()
            result["abfss_path"] = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{datastore_id}/Tables/{schema_name}/{tbl_name}"
        elif len(parts) == 2:
            # schema.table
            schema_name = parts[0].lower()
            tbl_name = parts[1].lower()
            result["abfss_path"] = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{datastore_id}/Tables/{schema_name}/{tbl_name}"
    
    return result


def extract_join_sources(advanced_df, table_id: int) -> List[Dict]:
    """Extract join table sources from advanced configuration"""
    join_configs = advanced_df.filter(
        (advanced_df.Table_ID == table_id) & 
        (advanced_df.Configuration_Category == "data_transformation_steps") &
        (advanced_df.Configuration_Name == "join_data")
    ).collect()
    
    join_instances = {}
    for row in join_configs:
        instance = row.Configuration_Name_Instance_Number
        if instance not in join_instances:
            join_instances[instance] = {}
        join_instances[instance][row.Configuration_Attribute_Name] = row.Configuration_Attribute_Value
    
    joins = []
    for instance, attrs in join_instances.items():
        if "right_table_name" in attrs:
            joins.append({
                "instance": instance,
                "right_table_name": attrs.get("right_table_name"),
                "join_type": attrs.get("join_type", "inner"),
                "join_condition": attrs.get("join_condition")
            })
    
    return joins


def extract_dimension_surrogate_key_sources(advanced_df, table_id: int) -> List[Dict]:
    """Extract dimension table sources from attach_dimension_surrogate_key configurations"""
    dim_configs = advanced_df.filter(
        (advanced_df.Table_ID == table_id) & 
        (advanced_df.Configuration_Category == "data_transformation_steps") &
        (advanced_df.Configuration_Name == "attach_dimension_surrogate_key")
    ).collect()
    
    dim_instances = {}
    for row in dim_configs:
        instance = row.Configuration_Name_Instance_Number
        if instance not in dim_instances:
            dim_instances[instance] = {}
        dim_instances[instance][row.Configuration_Attribute_Name] = row.Configuration_Attribute_Value
    
    dimensions = []
    for instance, attrs in dim_instances.items():
        if "dimension_table_name" in attrs:
            dimensions.append({
                "instance": instance,
                "dimension_table_name": attrs.get("dimension_table_name"),
                "join_logic": attrs.get("dimension_table_join_logic"),
                "key_column": attrs.get("dimension_table_key_column_name"),
                "output_column": attrs.get("dimension_key_output_column_name"),
                "columns_to_add": attrs.get("dimension_columns_to_add_to_fact")
            })
    
    return dimensions


def extract_union_sources(advanced_df, table_id: int) -> List[Dict]:
    """Extract union table sources from union_data configurations"""
    union_configs = advanced_df.filter(
        (advanced_df.Table_ID == table_id) & 
        (advanced_df.Configuration_Category == "data_transformation_steps") &
        (advanced_df.Configuration_Name == "union_data")
    ).collect()
    
    union_instances = {}
    for row in union_configs:
        instance = row.Configuration_Name_Instance_Number
        if instance not in union_instances:
            union_instances[instance] = {}
        union_instances[instance][row.Configuration_Attribute_Name] = row.Configuration_Attribute_Value
    
    unions = []
    for instance, attrs in union_instances.items():
        if "union_tables" in attrs:
            # union_tables can be comma-separated list of table names
            union_tables_str = attrs.get("union_tables", "")
            table_names = [t.strip() for t in union_tables_str.split(",") if t.strip()]
            for table_name in table_names:
                unions.append({
                    "instance": instance,
                    "union_table_name": table_name,
                    "union_type": attrs.get("union_type", "by_name"),
                    "deduplicate": attrs.get("deduplicate", "false")
                })
    
    return unions


def extract_transformations(advanced_df, table_id: int) -> List[str]:
    """Extract list of transformation types applied to this table"""
    transform_configs = advanced_df.filter(
        (advanced_df.Table_ID == table_id) & 
        (advanced_df.Configuration_Category == "data_transformation_steps")
    ).select("Configuration_Name").distinct().collect()
    
    return [row.Configuration_Name for row in transform_configs]


def find_root_sources(G: nx.DiGraph) -> List[str]:
    """Find all root nodes (external sources with no incoming edges)"""
    return [node for node in G.nodes() if G.in_degree(node) == 0]


def extract_custom_ingestion_function_info(config_df, table_id: int) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Extract custom ingestion function and notebook info from primary configuration (source_details).
    
    This handles:
    - custom_table_ingestion_function: For custom SQL/table extraction from databases or Delta tables
    - custom_file_ingestion_function: For custom file parsing (XML, proprietary formats, etc.)
    
    Returns:
        Tuple of (function_name, notebook_name, function_type) where function_type is 'table' or 'file'
        Returns (None, None, None) if not configured
    """
    source_configs = config_df.filter(
        (config_df.Table_ID == table_id) & 
        (config_df.Configuration_Category == "source_details")
    ).collect()
    
    function_name = None
    notebook_name = None
    function_type = None
    
    for row in source_configs:
        config_name = row.Configuration_Name
        config_value = row.Configuration_Value
        
        # Check for table ingestion function
        if config_name == "custom_table_ingestion_function":
            function_name = config_value
            function_type = "table"
        elif config_name == "custom_table_ingestion_function_notebook":
            notebook_name = config_value
        # Check for file ingestion function
        elif config_name == "custom_file_ingestion_function":
            function_name = config_value
            function_type = "file"
        elif config_name == "custom_file_ingestion_function_notebook":
            notebook_name = config_value
    
    return function_name, notebook_name, function_type


def extract_custom_transformation_function_info(advanced_df, table_id: int) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract custom transformation function and notebook info from advanced configuration.
    
    This handles custom_transformation_function which transforms data AFTER ingestion.
    Configured in data_transformation_steps category in Advanced Configuration.
    
    Returns:
        Tuple of (function_name, notebook_name) or (None, None) if not configured
    """
    transform_configs = advanced_df.filter(
        (advanced_df.Table_ID == table_id) & 
        (advanced_df.Configuration_Category == "data_transformation_steps") &
        (advanced_df.Configuration_Name == "custom_transformation_function")
    ).collect()
    
    if not transform_configs:
        return None, None
    
    # Group by instance number
    instances = {}
    for row in transform_configs:
        instance = row.Configuration_Name_Instance_Number
        if instance not in instances:
            instances[instance] = {}
        instances[instance][row.Configuration_Attribute_Name] = row.Configuration_Attribute_Value
    
    # Return first instance (typically only one custom_transformation_function per table)
    if instances:
        first_instance = instances[min(instances.keys())]
        # Attribute names match NB_Helper_Functions_1: functions_to_execute, notebooks_to_run
        function_name = first_instance.get("functions_to_execute")
        notebook_name = first_instance.get("notebooks_to_run")
        return function_name, notebook_name
    
    return None, None


def extract_tables_from_notebook(notebook_name: str) -> List[Dict[str, str]]:
    """
    Extract table references from a custom notebook using notebookutils.notebook.getDefinition.
    
    Custom ingestion functions only READ data, so we focus on read patterns.
    
    Parses the notebook code to find:
    - spark.sql() calls with SELECT/FROM clauses
    - spark.read.table() and spark.table() calls
    - spark.read.format("delta").load() with ABFSS paths
    - DeltaTable.forPath() and DeltaTable.forName() calls
    - Direct ABFSS path references in read operations
    
    Args:
        notebook_name: Name of the notebook (without .ipynb extension)
    
    Returns:
        List of dicts with table info: [{"table_name": "...", "source_type": "table|path|sql"}]
    """
    import re
    import json
    
    tables_found = []
    
    try:
        # Get notebook definition using notebookutils
        notebook_definition = notebookutils.notebook.getDefinition(notebook_name)
        
        # Parse the notebook JSON
        notebook_json = json.loads(notebook_definition)
        
        # Extract code cells
        cells = notebook_json.get("cells", [])
        
        for cell in cells:
            if cell.get("cell_type") == "code":
                source_lines = cell.get("source", [])
                code = "".join(source_lines) if isinstance(source_lines, list) else source_lines
                
                # Pattern 1: spark.sql("SELECT ... FROM table_name")
                sql_pattern = r'spark\.sql\s*\(\s*[f]?["\'](.+?)["\']'
                sql_matches = re.findall(sql_pattern, code, re.DOTALL | re.IGNORECASE)
                for sql in sql_matches:
                    # Extract table names from SQL SELECT statements
                    table_refs = extract_tables_from_sql(sql)
                    tables_found.extend(table_refs)
                
                # Pattern 2: spark.read.table("table_name") or spark.table("table_name")
                read_table_pattern = r'spark\.(?:read\.)?table\s*\(\s*[f]?["\']([^"\']+)["\']'
                read_matches = re.findall(read_table_pattern, code, re.IGNORECASE)
                for table in read_matches:
                    tables_found.append({"table_name": table, "source_type": "table"})
                
                # Pattern 3: DeltaTable.forName(spark, "table_name")
                delta_name_pattern = r'DeltaTable\.forName\s*\(\s*spark\s*,\s*[f]?["\']([^"\']+)["\']'
                delta_name_matches = re.findall(delta_name_pattern, code, re.IGNORECASE)
                for table in delta_name_matches:
                    tables_found.append({"table_name": table, "source_type": "table"})
                
                # Pattern 4: ABFSS paths - spark.read...load("abfss://...")
                abfss_pattern = r'\.load\s*\(\s*[f]?["\'](abfss://[^"\']+)["\']'
                abfss_matches = re.findall(abfss_pattern, code, re.IGNORECASE)
                for abfss_path in abfss_matches:
                    table_name = extract_table_from_abfss_path(abfss_path)
                    if table_name:
                        tables_found.append({"table_name": table_name, "source_type": "abfss_path", "full_path": abfss_path})
                
                # Pattern 5: DeltaTable.forPath(spark, "abfss://...")
                delta_path_pattern = r'DeltaTable\.forPath\s*\(\s*spark\s*,\s*[f]?["\'](abfss://[^"\']+)["\']'
                delta_path_matches = re.findall(delta_path_pattern, code, re.IGNORECASE)
                for abfss_path in delta_path_matches:
                    table_name = extract_table_from_abfss_path(abfss_path)
                    if table_name:
                        tables_found.append({"table_name": table_name, "source_type": "abfss_path", "full_path": abfss_path})
                
                # Pattern 6: Variable assignments with ABFSS paths
                abfss_var_pattern = r'["\']?(abfss://[^"\']+/Tables/[^"\']+)["\']?'
                abfss_var_matches = re.findall(abfss_var_pattern, code, re.IGNORECASE)
                for abfss_path in abfss_var_matches:
                    table_name = extract_table_from_abfss_path(abfss_path)
                    if table_name:
                        tables_found.append({"table_name": table_name, "source_type": "abfss_path", "full_path": abfss_path})
                
                # Pattern 7: Files path references (for file ingestion)
                files_pattern = r'["\']?(abfss://[^"\']+/Files/[^"\']+)["\']?'
                files_matches = re.findall(files_pattern, code, re.IGNORECASE)
                for files_path in files_matches:
                    file_name = extract_file_from_abfss_path(files_path)
                    if file_name:
                        tables_found.append({"table_name": file_name, "source_type": "file_path", "full_path": files_path})
        
        # Deduplicate
        seen = set()
        unique_tables = []
        for t in tables_found:
            key = t["table_name"]
            if key not in seen:
                seen.add(key)
                unique_tables.append(t)
        
        print(f"📖 Found {len(unique_tables)} table references in notebook '{notebook_name}'")
        return unique_tables
        
    except Exception as e:
        print(f"⚠️ Could not parse notebook '{notebook_name}': {str(e)}")
        return []


def extract_table_from_abfss_path(abfss_path: str) -> Optional[str]:
    """
    Extract table name from an ABFSS path.
    
    Handles paths like:
    - abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse.Lakehouse/Tables/schema/table
    - abfss://...@onelake.../bronze.Lakehouse/Tables/dbo/customers
    
    Returns:
        Table name in format "datastore.schema.table" or None if not a Tables path
    """
    import re
    
    if "/Tables/" not in abfss_path:
        return None
    
    # Extract lakehouse name and table path
    # Pattern: /lakehouse_name.Lakehouse/Tables/schema/table
    match = re.search(r'/([^/]+)\.Lakehouse/Tables/(.+?)(?:\?|$)', abfss_path, re.IGNORECASE)
    if match:
        lakehouse_name = match.group(1)
        table_path = match.group(2).rstrip('/')
        # table_path might be "schema/table" or just "table"
        parts = table_path.split('/')
        if len(parts) >= 2:
            return f"{lakehouse_name}.{parts[0]}.{parts[1]}"
        elif len(parts) == 1:
            return f"{lakehouse_name}.dbo.{parts[0]}"
    
    return None


def extract_file_from_abfss_path(abfss_path: str) -> Optional[str]:
    """
    Extract file reference from an ABFSS Files path.
    
    Handles paths like:
    - abfss://workspace@onelake.../bronze.Lakehouse/Files/folder/file.csv
    
    Returns:
        File path in format "Files/folder/file.csv" or None
    """
    import re
    
    if "/Files/" not in abfss_path:
        return None
    
    # Extract the Files portion
    match = re.search(r'/([^/]+)\.Lakehouse/(Files/.+?)(?:\?|$)', abfss_path, re.IGNORECASE)
    if match:
        lakehouse_name = match.group(1)
        file_path = match.group(2).rstrip('/')
        return f"{lakehouse_name}:{file_path}"
    
    return None


def extract_tables_from_sql(sql: str) -> List[Dict[str, str]]:
    """
    Extract table names from a SQL query string (READ operations only).
    
    Handles:
    - FROM clause: SELECT * FROM table_name
    - JOIN clauses: JOIN table_name ON ...
    
    Args:
        sql: SQL query string
    
    Returns:
        List of dicts with table info
    """
    import re
    
    tables = []
    
    # Pattern for FROM and JOIN - captures 3-part names (datastore.schema.table)
    from_join_pattern = r'(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*){0,2})'
    matches = re.findall(from_join_pattern, sql, re.IGNORECASE)
    
    # SQL keywords to skip
    skip_keywords = {'SELECT', 'WHERE', 'AND', 'OR', 'ON', 'AS', 'SET', 'VALUES', 
                     'GROUP', 'ORDER', 'BY', 'HAVING', 'LIMIT', 'UNION', 'CASE', 
                     'WHEN', 'THEN', 'ELSE', 'END', 'NULL', 'TRUE', 'FALSE'}
    
    for table in matches:
        if table.upper() not in skip_keywords:
            tables.append({"table_name": table, "source_type": "sql"})
    
    return tables


def calculate_lineage_depth_and_path(G: nx.DiGraph, target_node_id: str, source_node_id: str, relationship_type: str = "direct") -> Tuple[int, str]:
    """
    Calculate the lineage depth and full path from any root source to the target.
    
    For 'direct' relationships: Shows full lineage chain from ultimate root to target.
    For secondary sources (joins, dimension lookups, unions): Shows simplified path
    since those sources have their own lineage records with full chains.
    
    Returns:
        depth: Number of hops from the earliest ancestor (root) to target
        path: Path string (full chain for direct, simplified for secondary sources)
    """
    root_sources = find_root_sources(G)
    
    # For secondary sources (joins, dimension lookups, unions), show simplified path
    # These sources have their own lineage records that show their full chain
    if relationship_type and relationship_type != "direct":
        # Calculate depth through this secondary source
        if source_node_id in root_sources:
            depth = 1
        else:
            best_depth = 1
            for root in root_sources:
                try:
                    path_to_source = nx.shortest_path(G, root, source_node_id)
                    depth_candidate = len(path_to_source)
                    if depth_candidate > best_depth:
                        best_depth = depth_candidate
                except nx.NetworkXNoPath:
                    continue
            depth = best_depth
        
        # Show simplified path: just the immediate relationship
        simplified_path = f"{source_node_id} → {target_node_id}"
        return depth, simplified_path
    
    # For direct relationships, show full lineage chain
    # If the source IS a root source, depth is 1
    if source_node_id in root_sources:
        path = f"{source_node_id} → {target_node_id}"
        return 1, path
    
    # Find the shortest path from any root to the source, then add the target
    best_depth = float('inf')
    best_path = None
    
    for root in root_sources:
        try:
            # Get path from root to source
            path_to_source = nx.shortest_path(G, root, source_node_id)
            # Depth is number of edges from root to target (path length + 1 for the final hop)
            depth = len(path_to_source)  # edges from root to source, then +1 is implicit (source→target)
            
            if depth < best_depth:
                best_depth = depth
                # Build full path including target
                full_path = path_to_source + [target_node_id]
                best_path = " → ".join(full_path)
        except nx.NetworkXNoPath:
            continue
    
    if best_path is None:
        # No path found from any root, this edge starts from source directly
        return 1, f"{source_node_id} → {target_node_id}"
    
    return best_depth, best_path

print("✅ Helper functions defined")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Build Lineage Graph

# CELL ********************

def build_lineage_graph(
    orchestration_df,
    primary_config_df,
    advanced_config_df,
    table_ids: List[int] = None,
    extract_custom_transformation_function_lineage: bool = True
) -> nx.DiGraph:
    """
    Build a NetworkX directed graph representing data lineage.
    
    Args:
        orchestration_df: DataFrame with orchestration metadata
        primary_config_df: DataFrame with primary configuration
        advanced_config_df: DataFrame with advanced configuration
        table_ids: Optional list of specific Table_IDs to include
        extract_custom_transformation_function_lineage: Whether to parse custom notebooks for additional sources
    
    Returns:
        NetworkX DiGraph with nodes (entities) and edges (data flows)
    """
    G = nx.DiGraph()
    
    if table_ids:
        orch_records = orchestration_df.filter(
            orchestration_df.Table_ID.isin(table_ids)
        ).collect()
    else:
        orch_records = orchestration_df.filter(
            orchestration_df.Ingestion_Active == 1
        ).collect()
    
    for orch_row in orch_records:
        table_id = orch_row.Table_ID
        target_datastore = orch_row.Target_Datastore
        target_entity = orch_row.Target_Entity
        trigger_name_val = orch_row.Trigger_Name
        
        # Create target node
        target_medallion = derive_medallion_layer(target_datastore)
        target_node = LineageNode(
            entity_name=target_entity,
            datastore=target_datastore,
            medallion_layer=target_medallion,
            node_type="entity"
        )
        
        # Get source details
        source_entity, source_datastore, source_medallion = derive_source_entity_and_datastore(
            primary_config_df, table_id
        )
        source_system_type = derive_source_system_type(primary_config_df, table_id)
        
        if source_entity:
            source_node = LineageNode(
                entity_name=source_entity,
                datastore=source_datastore,
                medallion_layer=source_medallion,
                system_type=source_system_type,
                node_type="external_source" if source_medallion == "External" else "entity"
            )
            
            G.add_node(source_node.node_id, **source_node.to_dict())
            G.add_node(target_node.node_id, **target_node.to_dict())
            
            transformations = extract_transformations(advanced_config_df, table_id)
            transformation_str = ", ".join(transformations) if transformations else None
            
            G.add_edge(
                source_node.node_id,
                target_node.node_id,
                table_id=table_id,
                trigger_name=trigger_name_val,
                relationship_type="direct",
                transformation_applied=transformation_str
            )
        
        # Add edges for join sources
        join_sources = extract_join_sources(advanced_config_df, table_id)
        for join_info in join_sources:
            right_table = join_info["right_table_name"]
            if right_table:
                parts = right_table.split(".")
                join_datastore = parts[0] if len(parts) >= 2 else None
                join_medallion = derive_medallion_layer(join_datastore)
                
                join_node = LineageNode(
                    entity_name=right_table,
                    datastore=join_datastore,
                    medallion_layer=join_medallion,
                    node_type="entity"
                )
                
                G.add_node(join_node.node_id, **join_node.to_dict())
                G.add_edge(
                    join_node.node_id,
                    target_node.node_id,
                    table_id=table_id,
                    trigger_name=trigger_name_val,
                    relationship_type=f"join ({join_info['join_type']})",
                    transformation_applied=f"join_data: {join_info.get('join_condition', '')}"
                )
        
        # Add edges for dimension surrogate key lookups
        dim_sources = extract_dimension_surrogate_key_sources(advanced_config_df, table_id)
        for dim_info in dim_sources:
            dim_table = dim_info["dimension_table_name"]
            if dim_table:
                parts = dim_table.split(".")
                dim_datastore = parts[0] if len(parts) >= 2 else None
                dim_medallion = derive_medallion_layer(dim_datastore)
                
                dim_node = LineageNode(
                    entity_name=dim_table,
                    datastore=dim_datastore,
                    medallion_layer=dim_medallion,
                    node_type="dimension"
                )
                
                G.add_node(dim_node.node_id, **dim_node.to_dict())
                
                # Build transformation description
                transform_desc = f"attach_dimension_surrogate_key: {dim_info.get('key_column', 'surrogate_key')}"
                if dim_info.get('columns_to_add'):
                    transform_desc += f" + denormalize({dim_info['columns_to_add']})"
                
                G.add_edge(
                    dim_node.node_id,
                    target_node.node_id,
                    table_id=table_id,
                    trigger_name=trigger_name_val,
                    relationship_type="dimension_lookup",
                    transformation_applied=transform_desc
                )
        
        # Add edges for union sources
        union_sources = extract_union_sources(advanced_config_df, table_id)
        for union_info in union_sources:
            union_table = union_info["union_table_name"]
            if union_table:
                parts = union_table.split(".")
                union_datastore = parts[0] if len(parts) >= 2 else None
                union_medallion = derive_medallion_layer(union_datastore)
                
                union_node = LineageNode(
                    entity_name=union_table,
                    datastore=union_datastore,
                    medallion_layer=union_medallion,
                    node_type="entity"
                )
                
                G.add_node(union_node.node_id, **union_node.to_dict())
                
                # Build transformation description
                transform_desc = f"union_data ({union_info.get('union_type', 'by_name')})"
                if union_info.get('deduplicate') == 'true':
                    transform_desc += " + deduplicate"
                
                G.add_edge(
                    union_node.node_id,
                    target_node.node_id,
                    table_id=table_id,
                    trigger_name=trigger_name_val,
                    relationship_type="union",
                    transformation_applied=transform_desc
                )
        
        # Extract lineage from custom ingestion functions (table or file)
        if extract_custom_transformation_function_lineage:
            ingestion_func, ingestion_notebook, ingestion_type = extract_custom_ingestion_function_info(primary_config_df, table_id)
            
            if ingestion_notebook:
                func_type_label = "custom_table_ingestion_function" if ingestion_type == "table" else "custom_file_ingestion_function"
                print(f"🔍 Extracting lineage from {func_type_label} notebook: {ingestion_notebook} (Table_ID: {table_id})")
                
                # Parse the notebook to find table/path references
                notebook_tables = extract_tables_from_notebook(ingestion_notebook)
                
                for table_ref in notebook_tables:
                    ref_table_name = table_ref["table_name"]
                    source_type = table_ref.get("source_type", "unknown")
                    
                    # Derive datastore and medallion from the table name
                    parts = ref_table_name.split(".")
                    if len(parts) >= 2:
                        ref_datastore = parts[0]
                        ref_medallion = derive_medallion_layer(ref_datastore)
                    else:
                        ref_datastore = None
                        ref_medallion = "Unknown"
                    
                    # Create node for the custom ingestion source
                    custom_source_node = LineageNode(
                        entity_name=ref_table_name,
                        datastore=ref_datastore,
                        medallion_layer=ref_medallion,
                        system_type=func_type_label,
                        node_type="entity" if source_type in ("table", "sql") else "file"
                    )
                    
                    # Add node and edge
                    G.add_node(custom_source_node.node_id, **custom_source_node.to_dict())
                    G.add_node(target_node.node_id, **target_node.to_dict())
                    
                    G.add_edge(
                        custom_source_node.node_id,
                        target_node.node_id,
                        table_id=table_id,
                        trigger_name=trigger_name_val,
                        relationship_type=f"{func_type_label} ({ingestion_func or 'unknown'})",
                        transformation_applied=f"{func_type_label}: {ingestion_func}, notebook: {ingestion_notebook}"
                    )
            
            # Extract lineage from custom transformation functions (Advanced Config)
            transform_func, transform_notebook = extract_custom_transformation_function_info(advanced_config_df, table_id)
            
            if transform_notebook:
                print(f"🔍 Extracting lineage from custom_transformation_function notebook: {transform_notebook} (Table_ID: {table_id})")
                
                # Parse the notebook to find table/path references
                notebook_tables = extract_tables_from_notebook(transform_notebook)
                
                for table_ref in notebook_tables:
                    ref_table_name = table_ref["table_name"]
                    source_type = table_ref.get("source_type", "unknown")
                    
                    # Derive datastore and medallion from the table name
                    parts = ref_table_name.split(".")
                    if len(parts) >= 2:
                        ref_datastore = parts[0]
                        ref_medallion = derive_medallion_layer(ref_datastore)
                    else:
                        ref_datastore = None
                        ref_medallion = "Unknown"
                    
                    # Create node for the custom transformation source
                    custom_source_node = LineageNode(
                        entity_name=ref_table_name,
                        datastore=ref_datastore,
                        medallion_layer=ref_medallion,
                        system_type="custom_transformation_function",
                        node_type="entity" if source_type in ("table", "sql") else "file"
                    )
                    
                    # Add node and edge
                    G.add_node(custom_source_node.node_id, **custom_source_node.to_dict())
                    G.add_node(target_node.node_id, **target_node.to_dict())
                    
                    G.add_edge(
                        custom_source_node.node_id,
                        target_node.node_id,
                        table_id=table_id,
                        trigger_name=trigger_name_val,
                        relationship_type=f"custom_transformation_function ({transform_func or 'unknown'})",
                        transformation_applied=f"custom_transformation_function: {transform_func}, notebook: {transform_notebook}"
                    )
    
    return G

print("✅ build_lineage_graph function defined")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Generate Lineage Records

# CELL ********************

def generate_lineage_records(
    G: nx.DiGraph,
    logs_df,
    primary_config_df,
    orchestration_df,
    workspace_variables: dict,
    trigger_name_filter: str = None,
    execution_id_filter: str = None,
    lineage_version: int = 1
) -> List[Dict]:
    """
    Generate lineage records by combining graph structure with execution logs.
    
    Creates ONE record per unique edge (structural lineage only).
    Only outputs edges where the target table has actual run data (no "Metadata Only" records).
    Execution-specific fields (Records_Processed, timing, Log_ID) are excluded.
    
    Cross-trigger lineage is automatically detected by checking if the Lineage_Path
    spans nodes from different triggers (e.g., Oracle → bronze (TriggerA) → silver (TriggerB))
    
    Now includes:
    - Source/Target Workspace Names
    - Source/Target ABFSS Paths (for internal Fabric sources)
    - Source Connection ID (for external sources only)
    """
    lineage_records = []
    lineage_generated_at = datetime.utcnow()
    
    filtered_logs = logs_df.filter(logs_df.Ingestion_Status == "Processed")
    if trigger_name_filter:
        filtered_logs = filtered_logs.filter(filtered_logs.Trigger_Name == trigger_name_filter)
    if execution_id_filter:
        filtered_logs = filtered_logs.filter(filtered_logs.Trigger_Execution_ID == execution_id_filter)
    
    log_records = filtered_logs.collect()
    
    # Create a SET of Table_IDs that have actual log data
    # Also create lookups for Source_Type and Target_Type from logs
    tables_with_logs = set()
    source_type_lookup = {}
    target_type_lookup = {}
    for log in log_records:
        tables_with_logs.add(log.Table_ID)
        # Get Source_Type from log if available
        if hasattr(log, 'Source_Type') and log.Source_Type:
            source_type_lookup[log.Table_ID] = log.Source_Type
        # Get Target_Type from log if available
        if hasattr(log, 'Target_Type') and log.Target_Type:
            target_type_lookup[log.Table_ID] = log.Target_Type
    
    # Build a map of node_id -> trigger_name for cross-trigger detection
    node_trigger_map = {}
    for source_id, target_id, edge_data in G.edges(data=True):
        edge_trigger = edge_data.get("trigger_name")
        if edge_trigger:
            # Map both source and target to their trigger
            node_trigger_map[target_id] = edge_trigger
            # Note: source might be shared across triggers, so we don't overwrite if already set
            if source_id not in node_trigger_map:
                node_trigger_map[source_id] = edge_trigger
    
    lineage_id_counter = 1
    
    # Build lookup for orchestration rows by Table_ID for target details
    orch_lookup = {}
    for row in orchestration_df.collect():
        orch_lookup[row.Table_ID] = row
    
    for source_id, target_id, edge_data in G.edges(data=True):
        table_id = edge_data.get("table_id")
        edge_trigger_name = edge_data.get("trigger_name")
        relationship_type = edge_data.get("relationship_type", "direct")
        
        # Skip edges without actual run data
        if table_id not in tables_with_logs:
            continue
        
        source_node = G.nodes[source_id]
        target_node = G.nodes[target_id]
        
        # Calculate lineage depth and path using graph traversal
        # Pass relationship_type to show simplified path for secondary sources
        lineage_depth, lineage_path = calculate_lineage_depth_and_path(G, target_id, source_id, relationship_type)
        
        # Detect cross-trigger lineage by checking if path nodes belong to different triggers
        path_nodes = lineage_path.split(" → ") if lineage_path else []
        triggers_in_path = set()
        for node_id in path_nodes:
            if node_id in node_trigger_map:
                triggers_in_path.add(node_trigger_map[node_id])
        
        # Cross-trigger path if more than one trigger is involved
        is_cross_trigger = len(triggers_in_path) > 1
        cross_trigger_info = ", ".join(sorted(triggers_in_path)) if is_cross_trigger else None
        
        # Extract extended source details (connection_id for external, ABFSS for internal)
        # For non-direct relationships (joins, unions, dimension lookups, custom functions),
        # derive ABFSS from the source node directly since the Table_ID's primary config
        # only has the main/direct source, not secondary sources
        relationship_type = edge_data.get("relationship_type", "direct")
        
        if relationship_type != "direct":
            # Secondary source (join, union, dimension_lookup, custom function)
            # Build ABFSS from source node's datastore/entity
            source_details = build_source_details_from_node(source_node, workspace_variables)
        else:
            # Direct/primary source - use primary config
            source_details = extract_extended_source_details(primary_config_df, table_id, workspace_variables)
        
        # Extract target details (workspace name, ABFSS path)
        target_details = {"workspace_name": None, "abfss_path": None}
        if table_id in orch_lookup:
            target_details = extract_target_details(orch_lookup[table_id], workspace_variables)
        
        # Create ONE record per unique edge (structural lineage only)
        lineage_id = str(uuid.uuid4())
        date_key = int(lineage_generated_at.strftime("%Y%m%d"))
        
        record = {
            "Lineage_ID": lineage_id,
            "Trigger_Name": edge_trigger_name,
            "Table_ID": table_id,
            "Source_Entity": source_node.get("entity_name"),
            "Source_Datastore": source_node.get("datastore"),
            "Source_Medallion_Layer": source_node.get("medallion_layer"),
            "Source_Type": source_type_lookup.get(table_id),  # From Data_Pipeline_Logs
            "Source_Workspace_Name": source_details.get("workspace_name"),  # For internal sources
            "Source_ABFSS_Path": source_details.get("abfss_path"),  # For internal sources
            "Source_Connection_ID": source_details.get("connection_id"),  # For external sources only
            "Target_Entity": target_node.get("entity_name"),
            "Target_Datastore": target_node.get("datastore"),
            "Target_Medallion_Layer": target_node.get("medallion_layer"),
            "Target_Type": target_type_lookup.get(table_id),  # From Data_Pipeline_Logs
            "Target_Workspace_Name": target_details.get("workspace_name"),
            "Target_ABFSS_Path": target_details.get("abfss_path"),
            "Relationship_Type": edge_data.get("relationship_type"),
            "Transformation_Applied": edge_data.get("transformation_applied"),
            "Lineage_Depth": lineage_depth,
            "Lineage_Path": lineage_path,
            "Is_Cross_Trigger": is_cross_trigger,
            "Cross_Trigger_Dependencies": cross_trigger_info,
            "Lineage_Generated_At": lineage_generated_at,
            "Lineage_Version": lineage_version,
            "Date_Key": date_key
        }
        lineage_records.append(record)
        lineage_id_counter += 1
    
    return lineage_records


def generate_lineage_summary(record: Dict) -> str:
    """
    Generate a human-readable summary for a lineage record.
    Focuses on structural data flow context (no execution-specific data).
    Makes the lineage easy to understand at a glance.
    """
    source = record.get("Source_Entity", "Unknown")
    target = record.get("Target_Entity", "Unknown")
    source_layer = record.get("Source_Medallion_Layer", "")
    target_layer = record.get("Target_Medallion_Layer", "")
    relationship = record.get("Relationship_Type", "direct")
    depth = record.get("Lineage_Depth", 1)
    transforms = record.get("Transformation_Applied")
    is_cross_trigger = record.get("Is_Cross_Trigger", False)
    
    # Build the flow description
    if source_layer == "External":
        flow_desc = f"Ingests from {source}"
    else:
        flow_desc = f"Reads from {source} ({source_layer})"
    
    flow_desc += f" → writes to {target} ({target_layer})"
    
    # Add relationship context
    if relationship == "direct":
        rel_desc = "as primary source"
    elif "join" in relationship.lower():
        rel_desc = f"via {relationship}"
    elif relationship == "dimension_lookup":
        rel_desc = "for dimension key lookup"
    elif relationship == "union":
        rel_desc = "as union source"
    elif "custom_transformation_function" in relationship.lower():
        rel_desc = "via custom ingestion"
    else:
        rel_desc = f"({relationship})"
    
    # Add transformation info
    if transforms:
        # Simplify transformation names
        transform_summary = transforms.split(",")[0].strip()  # Take first transform
        if len(transforms.split(",")) > 1:
            transform_summary += f" +{len(transforms.split(','))-1} more"
        transform_str = f" • Transforms: {transform_summary}"
    else:
        transform_str = ""
    
    # Add depth context for multi-hop lineage
    if depth > 1:
        depth_str = f" • Depth {depth} (multi-hop)"
    else:
        depth_str = ""
    
    # Cross-trigger indicator
    cross_str = " • ⚠️ Cross-trigger" if is_cross_trigger else ""
    
    return f"{flow_desc} {rel_desc}{transform_str}{depth_str}{cross_str}"


def generate_target_lineage_narrative(records: List[Dict], target_entity: str) -> str:
    """
    Generate a consolidated, human-readable narrative for a target entity.
    Designed to be clear and natural for both humans and LLMs (Copilot).
    
    Example output:
    "gold.dbo.fact_sales is a Gold layer table that originates from Oracle SALES.ORDERS. 
     Data flows through 3 hops: Oracle → bronze.dbo.orders → silver.dbo.orders → gold.dbo.fact_sales.
     This table also joins with dim_customer (originating from Oracle CUSTOMERS) and dim_product 
     (originating from Oracle PRODUCTS) to enrich the data with dimension keys.
     Transformations applied: cleanse_data, derived_column, attach_dimension_surrogate_key."
    """
    # Filter records for this target
    target_records = [r for r in records if r.get("Target_Entity") == target_entity]
    
    if not target_records:
        return f"{target_entity}: No lineage data available."
    
    # Get target info from first record
    target_layer = target_records[0].get("Target_Medallion_Layer", "Unknown")
    target_datastore = target_records[0].get("Target_Datastore", "")
    
    # Separate primary (direct) sources from secondary sources
    primary_sources = []
    secondary_sources = []
    all_cross_triggers = set()
    all_transforms = set()
    max_depth = 0
    
    for r in target_records:
        rel_type = r.get("Relationship_Type", "direct")
        depth = r.get("Lineage_Depth", 1)
        max_depth = max(max_depth, depth)
        
        # Collect transforms
        transforms = r.get("Transformation_Applied", "")
        if transforms:
            for t in transforms.split(","):
                # Clean up transform names (remove parameters)
                t_clean = t.split(":")[0].strip()
                if t_clean:
                    all_transforms.add(t_clean)
        
        if r.get("Is_Cross_Trigger"):
            cross_deps = r.get("Cross_Trigger_Dependencies", "")
            if cross_deps:
                for t in cross_deps.split(", "):
                    all_cross_triggers.add(t.strip())
        
        if rel_type == "direct":
            primary_sources.append(r)
        else:
            secondary_sources.append(r)
    
    # Build the narrative in natural language
    parts = []
    
    # --- OPENING: What is this table? ---
    full_target = f"{target_datastore}.{target_entity}" if target_datastore and not target_entity.startswith(target_datastore) else target_entity
    parts.append(f"{full_target} is a {target_layer} layer table.")
    
    # --- PRIMARY DATA FLOW: Where does the main data come from? ---
    if primary_sources:
        r = primary_sources[0]  # Main primary source
        path = r.get("Lineage_Path", "")
        source = r.get("Source_Entity", "Unknown")
        source_layer = r.get("Source_Medallion_Layer", "")
        depth = r.get("Lineage_Depth", 1)
        
        # Find the ultimate origin (first node in path)
        if path:
            path_nodes = path.split(" → ")
            origin = path_nodes[0]
        else:
            origin = source
        
        # Describe the origin
        if source_layer == "External" or ":" in origin:
            # External source (Oracle, SQL Server, etc.)
            origin_desc = f"originates from external source {origin}"
        else:
            origin_desc = f"sources data from {origin}"
        
        parts.append(f"It {origin_desc}.")
        
        # Describe the data flow path
        if depth > 1 and path:
            parts.append(f"Data flows through {depth} hops: {path}.")
        elif depth == 1:
            parts.append(f"Data flows directly from {source}.")
    
    # --- SECONDARY SOURCES: Joins, dimensions, unions ---
    if secondary_sources:
        join_sources = []
        dim_sources = []
        union_sources = []
        other_sources = []
        
        for r in secondary_sources:
            source = r.get("Source_Entity", "Unknown")
            rel_type = r.get("Relationship_Type", "")
            
            # Find the origin of this secondary source
            source_lineage = [rec for rec in records if rec.get("Target_Entity") == source and rec.get("Relationship_Type") == "direct"]
            if source_lineage:
                best = max(source_lineage, key=lambda x: x.get("Lineage_Depth", 1))
                origin_path = best.get("Lineage_Path", "")
                if origin_path:
                    sec_origin = origin_path.split(" → ")[0]
                else:
                    sec_origin = best.get("Source_Entity", "unknown origin")
            else:
                sec_origin = None
            
            source_info = {"name": source, "origin": sec_origin, "rel_type": rel_type}
            
            if "join" in rel_type.lower():
                join_sources.append(source_info)
            elif rel_type == "dimension_lookup":
                dim_sources.append(source_info)
            elif rel_type == "union":
                union_sources.append(source_info)
            else:
                other_sources.append(source_info)
        
        # Describe dimension lookups
        if dim_sources:
            if len(dim_sources) == 1:
                d = dim_sources[0]
                origin_text = f" (originating from {d['origin']})" if d['origin'] else ""
                parts.append(f"It enriches data by looking up keys from dimension table {d['name']}{origin_text}.")
            else:
                dim_descs = []
                for d in dim_sources:
                    origin_text = f" (from {d['origin']})" if d['origin'] else ""
                    dim_descs.append(f"{d['name']}{origin_text}")
                parts.append(f"It enriches data by looking up keys from dimension tables: {', '.join(dim_descs)}.")
        
        # Describe joins
        if join_sources:
            if len(join_sources) == 1:
                j = join_sources[0]
                origin_text = f" (originating from {j['origin']})" if j['origin'] else ""
                join_type = j['rel_type'].replace("join", "").replace("(", "").replace(")", "").strip() or "inner"
                parts.append(f"It joins with {j['name']}{origin_text} using a {join_type} join.")
            else:
                join_names = [j['name'] for j in join_sources]
                parts.append(f"It joins with tables: {', '.join(join_names)}.")
        
        # Describe unions
        if union_sources:
            union_names = [u['name'] for u in union_sources]
            parts.append(f"It unions data from: {', '.join(union_names)}.")
    
    # --- TRANSFORMATIONS ---
    if all_transforms:
        transforms_list = sorted(all_transforms)
        parts.append(f"Transformations applied: {', '.join(transforms_list)}.")
    
    # --- CROSS-TRIGGER WARNING ---
    if all_cross_triggers:
        parts.append(f"⚠️ This table spans multiple triggers: {', '.join(sorted(all_cross_triggers))}.")
    
    # --- DEPTH SUMMARY ---
    if max_depth > 2:
        parts.append(f"Total lineage depth: {max_depth} hops from origin.")
    
    return " ".join(parts)


def compute_lineage_fingerprint(records: List[Dict]) -> str:
    """
    Compute a fingerprint of the lineage structure to detect changes.
    Only considers the structural elements, not execution-specific data.
    """
    import hashlib
    
    # Extract only the structural fields that define the lineage
    structural_data = []
    for r in records:
        structural_key = (
            r.get("Source_Entity", ""),
            r.get("Source_Datastore", ""),
            r.get("Source_Medallion_Layer", ""),
            r.get("Source_Workspace_Name", ""),
            r.get("Source_ABFSS_Path", ""),
            r.get("Source_Connection_ID", ""),
            r.get("Target_Entity", ""),
            r.get("Target_Datastore", ""),
            r.get("Target_Medallion_Layer", ""),
            r.get("Target_Workspace_Name", ""),
            r.get("Target_ABFSS_Path", ""),
            r.get("Relationship_Type", ""),
            r.get("Transformation_Applied", ""),
            r.get("Trigger_Name", ""),
            r.get("Table_ID", ""),
            r.get("Lineage_Path", "")
        )
        structural_data.append(structural_key)
    
    # Sort for consistent ordering
    structural_data.sort()
    
    # Create hash
    fingerprint_str = str(structural_data)
    return hashlib.md5(fingerprint_str.encode()).hexdigest()


def compute_table_fingerprints(records: List[Dict]) -> Dict[int, str]:
    """
    Compute fingerprints for each Table_ID to enable table-level change detection.
    
    Returns:
        Dict mapping Table_ID to its lineage fingerprint
    """
    import hashlib
    from collections import defaultdict
    
    # Group records by Table_ID
    records_by_table = defaultdict(list)
    for r in records:
        table_id = r.get("Table_ID")
        if table_id is not None:
            records_by_table[table_id].append(r)
    
    # Compute fingerprint for each table
    table_fingerprints = {}
    for table_id, table_records in records_by_table.items():
        structural_data = []
        for r in table_records:
            structural_key = (
                r.get("Source_Entity", ""),
                r.get("Source_Datastore", ""),
                r.get("Source_Medallion_Layer", ""),
                r.get("Source_Workspace_Name", ""),
                r.get("Source_ABFSS_Path", ""),
                r.get("Source_Connection_ID", ""),
                r.get("Target_Entity", ""),
                r.get("Target_Datastore", ""),
                r.get("Target_Medallion_Layer", ""),
                r.get("Target_Workspace_Name", ""),
                r.get("Target_ABFSS_Path", ""),
                r.get("Relationship_Type", ""),
                r.get("Transformation_Applied", ""),
                r.get("Lineage_Path", "")
            )
            structural_data.append(structural_key)
        
        # Sort for consistent ordering
        structural_data.sort()
        fingerprint_str = str(structural_data)
        table_fingerprints[table_id] = hashlib.md5(fingerprint_str.encode()).hexdigest()
    
    return table_fingerprints


def detect_table_level_changes(
    current_records: List[Dict],
    previous_records: List[Dict]
) -> Dict[str, List[int]]:
    """
    Detect which tables have changed, been added, or been removed.
    
    Returns:
        Dict with keys: 'changed', 'added', 'removed', 'unchanged'
        Each value is a list of Table_IDs
    """
    current_fingerprints = compute_table_fingerprints(current_records)
    previous_fingerprints = compute_table_fingerprints(previous_records)
    
    current_tables = set(current_fingerprints.keys())
    previous_tables = set(previous_fingerprints.keys())
    
    added = list(current_tables - previous_tables)
    removed = list(previous_tables - current_tables)
    
    # Check tables that exist in both for changes
    common_tables = current_tables & previous_tables
    changed = []
    unchanged = []
    
    for table_id in common_tables:
        if current_fingerprints[table_id] != previous_fingerprints[table_id]:
            changed.append(table_id)
        else:
            unchanged.append(table_id)
    
    return {
        'changed': sorted(changed),
        'added': sorted(added),
        'removed': sorted(removed),
        'unchanged': sorted(unchanged)
    }


print("✅ generate_lineage_records function defined")
print("✅ generate_lineage_summary function defined")
print("✅ compute_lineage_fingerprint function defined")
print("✅ compute_table_fingerprints function defined")
print("✅ detect_table_level_changes function defined")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Execute Lineage Generation

# CELL ********************

# Read metadata tables
print(f"📖 Reading metadata from {metadata_warehouse_name}...")

orchestration_df = spark.read.synapsesql(f"{metadata_warehouse_name}.dbo.Data_Pipeline_Metadata_Orchestration")
primary_config_df = spark.read.synapsesql(f"{metadata_warehouse_name}.dbo.Data_Pipeline_Metadata_Primary_Configuration")
advanced_config_df = spark.read.synapsesql(f"{metadata_warehouse_name}.dbo.Data_Pipeline_Metadata_Advanced_Configuration")
logs_df = spark.read.synapsesql(f"{metadata_warehouse_name}.dbo.Data_Pipeline_Logs")

# Load datastore configuration from Datastore_Configuration table
print("📖 Loading datastore configuration...")
datastore_config_df = spark.read.synapsesql(f"{metadata_warehouse_name}.dbo.Datastore_Configuration")
datastore_rows = datastore_config_df.collect()

# Build workspace_variables dictionary from Datastore_Configuration table
# Keys follow the pattern: {datastore_name}_datastore_{property}
workspace_variables = {}
for row in datastore_rows:
    ds_name = row.Datastore_Name.strip().lower()
    workspace_variables[f"{ds_name}_datastore_id"] = row.Datastore_ID
    workspace_variables[f"{ds_name}_datastore_workspace_id"] = row.Workspace_ID
    workspace_variables[f"{ds_name}_datastore_workspace_name"] = row.Workspace_Name
    if row.Endpoint:
        workspace_variables[f"{ds_name}_datastore_endpoint"] = row.Endpoint

print(f"   - Loaded {len(datastore_rows)} datastores from Datastore_Configuration table")
print(f"   - Built {len(workspace_variables)} workspace variable entries")

print(f"   - Orchestration records: {orchestration_df.count()}")
print(f"   - Primary config records: {primary_config_df.count()}")
print(f"   - Advanced config records: {advanced_config_df.count()}")
print(f"   - Log records: {logs_df.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# IMPORTANT: Always build the FULL lineage graph first to capture cross-trigger relationships
# This ensures depth and path calculations are accurate even when data flows across triggers
# Example: TriggerA writes to silver.dbo.customers, TriggerB reads from silver.dbo.customers
#          The path should show: Oracle → bronze → silver.dbo.customers → gold (spanning both triggers)

print("🔨 Building FULL lineage graph (all triggers) for accurate cross-trigger depth calculation...")
G_full = build_lineage_graph(
    orchestration_df,  # Full orchestration, not filtered
    primary_config_df,
    advanced_config_df,
    table_ids=None
)

print(f"   - Total nodes (entities): {G_full.number_of_nodes()}")
print(f"   - Total edges (relationships): {G_full.number_of_edges()}")

# If trigger_name is specified, we'll filter the OUTPUT records (not the graph)
# This preserves cross-trigger lineage paths while focusing output on the requested trigger
if trigger_name:
    print(f"🔍 Will filter OUTPUT to trigger: {trigger_name}")
    print(f"   (Cross-trigger lineage paths are still calculated from the full graph)")

# Use the full graph for lineage generation
G = G_full

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Generate lineage records first (with placeholder version)
lineage_records = generate_lineage_records(
    G,
    logs_df,
    primary_config_df,
    orchestration_df,
    workspace_variables,
    trigger_name_filter=trigger_name,
    execution_id_filter=execution_id,
    lineage_version=0  # Placeholder, will update after change detection
)

print(f"📊 Generated {len(lineage_records)} lineage records")

# Add human-readable summary to each record
for record in lineage_records:
    record["Lineage_Summary"] = generate_lineage_summary(record)

# Generate consolidated Target_Lineage_Narrative for each unique target
# This provides one comprehensive narrative per target entity
target_entities = set(r.get("Target_Entity") for r in lineage_records)
target_narrative_cache = {}
for target in target_entities:
    target_narrative_cache[target] = generate_target_lineage_narrative(lineage_records, target)

# Generate Source_Lineage_Narrative for each unique source entity
# This tells you where each source comes from (its own lineage)
source_entities = set(r.get("Source_Entity") for r in lineage_records)
source_narrative_cache = {}
for source in source_entities:
    # A source entity might also be a target in the lineage (intermediate tables)
    # If so, use its target narrative; otherwise, it's likely an origin/root
    if source in target_narrative_cache:
        source_narrative_cache[source] = target_narrative_cache[source]
    else:
        # This is a root/origin source (external system or file)
        # Generate a simple narrative for it
        source_records = [r for r in lineage_records if r.get("Source_Entity") == source]
        if source_records:
            r = source_records[0]
            source_layer = r.get("Source_Medallion_Layer", "Unknown")
            source_type = r.get("Source_Type", "")
            if source_layer == "External":
                source_narrative_cache[source] = f"{source} is an external data source ({source_type or 'external system'}). It is the origin point for data flowing into this pipeline."
            else:
                source_narrative_cache[source] = f"{source} is a {source_layer} layer table. No upstream lineage found - this may be a root source or its lineage is not tracked."
        else:
            source_narrative_cache[source] = f"{source}: No lineage data available."

# Add both narratives to each record
for record in lineage_records:
    target = record.get("Target_Entity")
    source = record.get("Source_Entity")
    record["Target_Lineage_Narrative"] = target_narrative_cache.get(target, "")
    record["Source_Lineage_Narrative"] = source_narrative_cache.get(source, "")

print(f"📝 Generated consolidated narratives for {len(target_entities)} target entities and {len(source_entities)} source entities")

# Check for table-level changes
# Get the latest version
existing_lineage_df = spark.read.synapsesql(f"{metadata_warehouse_name}.dbo.Data_Pipeline_Lineage")

max_version_row = existing_lineage_df.selectExpr("MAX(Lineage_Version) as max_version").collect()[0]
previous_version = max_version_row.max_version or 0

if previous_version > 0:
    # Get records from previous version
    previous_records = existing_lineage_df.filter(
        existing_lineage_df.Lineage_Version == previous_version
    ).collect()
    
    # Convert to dict format
    previous_records_dict = [row.asDict() for row in previous_records]
    
    # Detect table-level changes
    table_changes = detect_table_level_changes(lineage_records, previous_records_dict)
    
    # Summarize changes
    total_changes = len(table_changes['changed']) + len(table_changes['added']) + len(table_changes['removed'])
    
    print(f"\n📋 Table-Level Change Analysis (comparing to v{previous_version}):")
    print(f"   - Tables with changed lineage: {len(table_changes['changed'])}")
    print(f"   - New tables added: {len(table_changes['added'])}")
    print(f"   - Tables removed: {len(table_changes['removed'])}")
    print(f"   - Tables unchanged: {len(table_changes['unchanged'])}")
    
    if table_changes['changed']:
        print(f"\n   🔄 Changed Table_IDs: {table_changes['changed'][:10]}{'...' if len(table_changes['changed']) > 10 else ''}")
    if table_changes['added']:
        print(f"   ➕ Added Table_IDs: {table_changes['added'][:10]}{'...' if len(table_changes['added']) > 10 else ''}")
    if table_changes['removed']:
        print(f"   ➖ Removed Table_IDs: {table_changes['removed'][:10]}{'...' if len(table_changes['removed']) > 10 else ''}")
    
    if total_changes > 0:
        lineage_changed = True
        lineage_version = previous_version + 1
        print(f"\n🔄 Lineage CHANGED for {total_changes} table(s) - creating version {lineage_version}")
    else:
        lineage_changed = False
        lineage_version = previous_version
        print(f"\n✅ All table lineage unchanged - keeping version {lineage_version}")
else:
    lineage_changed = True
    lineage_version = 1
    table_changes = {'changed': [], 'added': [r.get('Table_ID') for r in lineage_records], 'removed': [], 'unchanged': []}
    print(f"\n📊 First lineage generation - creating version {lineage_version}")

# Update version in all records
for record in lineage_records:
    record["Lineage_Version"] = lineage_version

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Persist to table if requested AND lineage has changed
if persist_to_table and lineage_records and lineage_changed:
    print("💾 Persisting lineage records to Data_Pipeline_Lineage table...")
    
    schema = StructType([
        StructField("Lineage_ID", StringType(), False),  # NOT NULL to match table schema
        StructField("Trigger_Name", StringType(), True),
        StructField("Table_ID", IntegerType(), True),
        StructField("Source_Entity", StringType(), True),
        StructField("Source_Datastore", StringType(), True),
        StructField("Source_Medallion_Layer", StringType(), True),
        StructField("Source_Type", StringType(), True),
        StructField("Source_Workspace_Name", StringType(), True),
        StructField("Source_ABFSS_Path", StringType(), True),
        StructField("Source_Connection_ID", StringType(), True),
        StructField("Target_Entity", StringType(), True),
        StructField("Target_Datastore", StringType(), True),
        StructField("Target_Medallion_Layer", StringType(), True),
        StructField("Target_Type", StringType(), True),
        StructField("Target_Workspace_Name", StringType(), True),
        StructField("Target_ABFSS_Path", StringType(), True),
        StructField("Relationship_Type", StringType(), True),
        StructField("Transformation_Applied", StringType(), True),
        StructField("Lineage_Depth", IntegerType(), True),
        StructField("Lineage_Path", StringType(), True),
        StructField("Is_Cross_Trigger", BooleanType(), True),
        StructField("Cross_Trigger_Dependencies", StringType(), True),
        StructField("Lineage_Summary", StringType(), True),  # Per-edge summary
        StructField("Source_Lineage_Narrative", StringType(), True),  # Narrative for the source entity
        StructField("Target_Lineage_Narrative", StringType(), True),  # Consolidated narrative for target entity
        StructField("Lineage_Generated_At", TimestampType(), True),
        StructField("Lineage_Version", IntegerType(), True),
        StructField("Date_Key", IntegerType(), True)
    ])
    
    lineage_df = spark.createDataFrame(lineage_records, schema)
    
    # Append to lineage table (overwrite to replace with new version)
    lineage_df.write.mode("overwrite").synapsesql(f"{metadata_warehouse_name}.dbo.Data_Pipeline_Lineage")
    
    print(f"✅ Persisted {len(lineage_records)} lineage records (Version {lineage_version})")
    
    # Show sample summaries
    print("\n📋 Sample Lineage Summaries:")
    for i, record in enumerate(lineage_records[:5]):
        print(f"   {i+1}. {record['Lineage_Summary']}")
    if len(lineage_records) > 5:
        print(f"   ... and {len(lineage_records) - 5} more records")
        
elif persist_to_table and lineage_records and not lineage_changed:
    print("ℹ️ Skipping persistence - lineage structure unchanged from previous version")
    print(f"   Current version remains: {lineage_version}")
else:
    print("ℹ️ Skipping persistence (persist_to_table=False or no records)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
