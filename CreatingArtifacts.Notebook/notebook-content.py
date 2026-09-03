# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import requests
import json
import base64
import time
import pyodbc
import struct
import re
import pandas as pd
from sqlalchemy import create_engine, text
import urllib
import sempy.fabric as fabric

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

current_workspace_id = notebookutils.runtime.context.get("currentWorkspaceId")

# Update blank IDs with the current workspace ID
if not bronze_lakehouse_workspace_id:
    bronze_lakehouse_workspace_id = current_workspace_id

if not silver_lakehouse_workspace_id:
    silver_lakehouse_workspace_id = current_workspace_id

if not gold_lakehouse_workspace_id:
    gold_lakehouse_workspace_id = current_workspace_id

if not metadata_workspace_id:
    metadata_workspace_id = current_workspace_id

if not spark_compute_workspace_id:
    spark_compute_workspace_id = current_workspace_id

if not pipeline_workspace_id:
    pipeline_workspace_id = current_workspace_id

if not report_semantic_model_workspace_id:
    report_semantic_model_workspace_id = current_workspace_id

normalized_folder_name = folder_name.strip() if folder_name and folder_name.strip() else ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Dynamic lakehouse and warehouse deployment
# lakehouses_to_deploy and warehouses_to_deploy are provided as input dictionaries
# Expected format:
lakehouses_to_deploy = {
    "bronze": { "lakehouse_name": bronze_lakehouse_name, "workspace_id": bronze_lakehouse_workspace_id },
    "silver": { "lakehouse_name": silver_lakehouse_name, "workspace_id": silver_lakehouse_workspace_id },
    "gold": { "lakehouse_name": gold_lakehouse_name, "workspace_id": gold_lakehouse_workspace_id },
    "metadata_files": { "lakehouse_name": metadata_lakehouse_name, "workspace_id": metadata_workspace_id }
}


warehouses_to_deploy = {
    "metadata": { "warehouse_name": metadata_warehouse_name, "workspace_id": metadata_workspace_id }
}

if metadata_lakehouse_name == metadata_warehouse_name:
    raise ValueError("The metadata lakehouse and warehouse must have different names.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run ImportArtifacts_DONT_OPEN_IN_FABRIC

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

connection_ids = {}
log_analytics_workspace_id = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source_pipelines_json = json.loads(Pipelines)
source_notebooks_json = json.loads(Notebooks)
source_semantic_models_json = json.loads(SemanticModels)
source_reports_json = json.loads(Reports)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source_pipelines_and_ids = {pipeline['displayName']: pipeline['id'] for pipeline in source_pipelines_json}
source_notebooks_and_ids = {notebook['displayName']: notebook['id'] for notebook in source_notebooks_json}
source_semantic_models_and_ids = {semantic_model['displayName']: semantic_model['id'] for semantic_model in source_semantic_models_json}
source_reports_and_ids = {report['displayName']: report['id'] for report in source_reports_json}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source_connections = {}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

body = [source_semantic_model_json for source_semantic_model_json in source_semantic_models_json if source_semantic_model_json['displayName'] == "SM_Data_Pipeline_Monitoring"][0]
parts = body['definition']['parts']

for part in parts:
    path = part['path']
    if path == 'definition/expressions.tmdl':
        payload = part['payload']

semantic_model_content_str = base64.b64decode(payload).decode("utf-8")

source_metadata_warehouse_details = re.findall(r'Sql.Database\((.*?)\)', semantic_model_content_str)[0].replace('"','').split(', ')
source_warehouse_details = {}
source_warehouse_details['metadata_id'] = source_metadata_warehouse_details[1]
source_warehouse_details['metadata_endpoint'] = source_metadata_warehouse_details[0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fabric_token = notebookutils.credentials.getToken('pbi')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fabric_headers = {
            'Content-Type': "application/json",
            'Authorization': f"Bearer {fabric_token}"
        }
base_url = "https://api.fabric.microsoft.com/v1"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

workspace_folder_cache = {}

def ensure_workspace_folder(workspace_id):
    """Ensure the folder exists in the given workspace and return its ID."""
    if not normalized_folder_name:
        return None

    if workspace_id in workspace_folder_cache:
        return workspace_folder_cache[workspace_id]

    list_url = f"{base_url}/workspaces/{workspace_id}/folders"
    response = requests.get(list_url, headers=fabric_headers)
    if response.status_code >= 400:
        try:
            error_payload = response.json()
            message = error_payload.get('message', response.text)
        except ValueError:
            message = response.text
        raise Exception(f"Unable to list folders for workspace {workspace_id}: {message}")

    folders = response.json().get('value', [])
    folder_id = next((folder['id'] for folder in folders if folder.get('displayName', '').lower() == normalized_folder_name.lower()), None)

    if not folder_id:
        create_url = f"{base_url}/workspaces/{workspace_id}/folders"
        folder_body = {"displayName": normalized_folder_name}
        response = requests.post(create_url, headers=fabric_headers, json=folder_body)
        if response.status_code == 202:
            redirect_url = response.headers.get('Location')
            if redirect_url:
                percent_complete = 0
                while percent_complete != 100:
                    time.sleep(1)
                    poll_response = requests.get(redirect_url, headers=fabric_headers)
                    poll_json = poll_response.json()
                    percent_complete = poll_json.get('percentComplete', 100)
                response = poll_response

        if response.status_code >= 400:
            try:
                error_payload = response.json()
                message = error_payload.get('message', response.text)
            except ValueError:
                message = response.text
            raise Exception(f"Unable to create folder '{normalized_folder_name}' in workspace {workspace_id}: {message}")

        folder_data = response.json() if response.text else {}
        if folder_data.get('errorCode'):
            raise Exception(folder_data.get('message', f"Error creating folder '{normalized_folder_name}'"))

        folder_id = folder_data.get('id')

    workspace_folder_cache[workspace_id] = folder_id
    return folder_id

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Static URLs for each artifact type using their designated workspaces
# Note: Lakehouses and Warehouses use dynamic URLs per their individual workspace configurations
get_target_pipelines_url = f"{base_url}/workspaces/{pipeline_workspace_id}/items?type=DataPipeline"
get_target_notebooks_url = f"{base_url}/workspaces/{spark_compute_workspace_id}/items?type=Notebook" 
get_target_semantic_models_url = f"{base_url}/workspaces/{report_semantic_model_workspace_id}/items?type=SemanticModel" 
get_target_reports_url = f"{base_url}/workspaces/{report_semantic_model_workspace_id}/items?type=Report"  

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get workspace names for display purposes
target_report_semantic_model_workspace_name = fabric.resolve_workspace_name(report_semantic_model_workspace_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

target_pipelines = requests.get(get_target_pipelines_url, headers = fabric_headers)
target_pipelines_json = target_pipelines.json().get('value')
target_pipelines_names = [pipeline['displayName'] for pipeline in target_pipelines_json]
target_pipelines_and_ids = {pipeline['displayName']: pipeline['id'] for pipeline in target_pipelines_json}
pipeline_id_mapping = {key: [source_pipelines_and_ids.get(key), target_pipelines_and_ids.get(key)] for key in source_pipelines_and_ids}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

target_notebooks = requests.get(get_target_notebooks_url, headers = fabric_headers)
target_notebooks_json = target_notebooks.json().get('value')
target_notebooks_names = [notebook['displayName'] for notebook in target_notebooks_json]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

target_semantic_models = requests.get(get_target_semantic_models_url, headers = fabric_headers)
target_semantic_models_json = target_semantic_models.json().get('value')
target_semantic_models_names = [semantic_model['displayName'] for semantic_model in target_semantic_models_json]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

target_reports = requests.get(get_target_reports_url, headers = fabric_headers)
target_reports_json = target_reports.json().get('value')
target_reports_names = [report['displayName'] for report in target_reports_json]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Deploy lakehouses to their designated workspaces
for key, config in lakehouses_to_deploy.items():
    lakehouse_name = config.get('lakehouse_name', key)  # Use lakehouse_name if provided, else use key
    workspace_id = config['workspace_id']
    
    # Get list of existing lakehouses in this workspace
    get_lakehouses_url = f"{base_url}/workspaces/{workspace_id}/items?type=Lakehouse"
    lakehouses_in_workspace = requests.get(get_lakehouses_url, headers = fabric_headers)
    lakehouses_in_workspace_names = [lh['displayName'] for lh in lakehouses_in_workspace.json().get('value', [])]
    
    # Check if lakehouse exists (case-insensitive comparison)
    lakehouse_exists = any(lh.lower() == lakehouse_name.lower() for lh in lakehouses_in_workspace_names)
    
    if not lakehouse_exists:
        print(f"Creating lakehouse '{lakehouse_name}' in workspace {workspace_id}")
        create_item_url = f"{base_url}/workspaces/{workspace_id}/items"
        body = {
            "displayName": lakehouse_name,
            "type": "Lakehouse",
            "description": f"{lakehouse_name} zone of the data lake.",
            "creationPayload": {
                "enableSchemas": True
            }
        }
        folder_id = ensure_workspace_folder(workspace_id)
        if folder_id:
            body["folderId"] = folder_id
        response = requests.post(create_item_url, headers = fabric_headers, json = body)
        data = response.json()
        if data.get('errorCode'):
            raise Exception(data.get('message'))
    else:
        print(f"lakehouse '{lakehouse_name}' already exists in workspace {workspace_id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Deploy warehouses to their designated workspaces
for key, config in warehouses_to_deploy.items():
    warehouse_name = config.get('warehouse_name', key)  # Use warehouse_name if provided, else use key
    workspace_id = config['workspace_id']
    
    # Get list of existing warehouses in this workspace
    get_warehouses_url = f"{base_url}/workspaces/{workspace_id}/items?type=Warehouse"
    warehouses_in_workspace = requests.get(get_warehouses_url, headers = fabric_headers)
    warehouses_in_workspace_names = [wh['displayName'] for wh in warehouses_in_workspace.json().get('value', [])]
    
    # Check if warehouse exists (case-insensitive comparison)
    warehouse_exists = any(wh.lower() == warehouse_name.lower() for wh in warehouses_in_workspace_names)
    
    if not warehouse_exists:
        print(f"Creating warehouse '{warehouse_name}' in workspace {workspace_id}")
        
        # Set appropriate description based on warehouse name
        if warehouse_name.lower() == 'metadata':
            description = "Metadata storage and logging for metadata driven ingestion."
        else:
            description = f"{warehouse_name} warehouse."
        
        create_item_url = f"{base_url}/workspaces/{workspace_id}/items"
        body = {
            "displayName": warehouse_name,
            "type": "Warehouse",
            "description": description
        }
        folder_id = ensure_workspace_folder(workspace_id)
        if folder_id:
            body["folderId"] = folder_id
        response = requests.post(create_item_url, headers = fabric_headers, json = body)
        if response.status_code == 202:
            time.sleep(20)
            redirect_url = response.headers.get('Location')
            response = requests.get(redirect_url, headers = fabric_headers)
            
        data = response.json()

        if data.get('errorCode'):
            raise Exception(data.get('message'))
    else:
        print(f"warehouse '{warehouse_name}' already exists in workspace {workspace_id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_or_update_pipeline(create_target_item_url
                            , fabric_headers
                            , pipelineBody
                            , target_pipelines_names
                            , target_pipelines_json
                            , warehouse_mapping
                            , target_workspace_id
                            , target_pipelines_and_ids
                            , pipeline_id_mapping
                            , notebook_id_mapping
                            , lakehouse_id_mapping_ids_only
                            , source_connections
                            , target_connections
                            , lakehouses_to_deploy
                            , warehouses_to_deploy):

    item_name = pipelineBody.get('displayName')

    source_workspace_id = pipelineBody.get('workspaceId')

    pipeline_content = pipelineBody['definition']['parts'][0]['payload']

    pipeline_content_str = base64.b64decode(pipeline_content).decode("utf-8")

    for source_connection_key, source_connection_value in source_connections.items():
        if not target_connections.get(source_connection_key) and ('PL_02_Get_External_Data_' in item_name or item_name == 'PL_02_Get_External_Data_Metadata'):
            # if not using connection, mark as inactive so no errors post deployment
            pipeline_content_str = re.sub(r'"value": "'+source_connection_key+'",\s+"activities": \[\s+{'
                                        , f'"value": "{source_connection_key}", "activities": [{{"state": "Inactive", "onInactiveMarkAs": "Failed",'
                                        , pipeline_content_str) 
            pipeline_content_str = pipeline_content_str.replace(source_connection_value, "")
                                        
        elif ('PL_02_Get_External_Data_' in item_name or item_name == 'PL_02_Get_External_Data_Metadata'):
            # update connection values for those the user plans to use
            pipeline_content_str = pipeline_content_str.replace(source_connection_value, target_connections[source_connection_key])

    # Replace datastore variable references with dynamic names
    # Map source lakehouse names to target lakehouse names for datastore variables
    source_lakehouse_names_map = {
        'bronze': 'Bronze',
        'silver': 'Silver',
        'gold': 'Gold',
        'metadata_files': 'Metadata_Files'
    }
    
    for key, config in lakehouses_to_deploy.items():
        source_name = source_lakehouse_names_map.get(key.lower(), key)
        target_name = config.get('lakehouse_name', key)
        
        # Only replace if names are different
        if source_name.lower() != target_name.lower():
            # Replace datastore variable references: bronze_datastore_* -> bronze2_datastore_*
            pipeline_content_str = pipeline_content_str.replace(f"{source_name.lower()}_datastore_", f"{target_name.lower()}_datastore_")
    
    # Replace warehouse datastore variable references
    source_warehouse_names_map = {
        'metadata': 'Metadata'
    }
    
    for key, config in warehouses_to_deploy.items():
        source_name = source_warehouse_names_map.get(key.lower(), key)
        target_name = config.get('warehouse_name', key)
        
        # Only replace if names are different
        if source_name.lower() != target_name.lower():
            # Replace datastore variable references: metadata_datastore_* -> metadata2_datastore_*
            pipeline_content_str = pipeline_content_str.replace(f"{source_name.lower()}_datastore_", f"{target_name.lower()}_datastore_")

    for source_warehouse_value, target_warehouse_value in warehouse_mapping.items():
        pipeline_content_str = pipeline_content_str.replace(source_warehouse_value, target_warehouse_value)

    pipeline_id_mapping_ids_only = {value[0]: value[1] for key, value in pipeline_id_mapping.items() if value[1] != None}  
    
    for source_pipeline_id, target_pipeline_id in pipeline_id_mapping_ids_only.items():
        pipeline_content_str = pipeline_content_str.replace(source_pipeline_id, target_pipeline_id)

    notebook_id_mapping_ids_only = {value[0]: value[1] for key, value in notebook_id_mapping.items() if value[1] != None} 

    for source_notebook_id, target_notebook_id in notebook_id_mapping_ids_only.items():
        pipeline_content_str = pipeline_content_str.replace(source_notebook_id, target_notebook_id)

    for source_lakehouse_id, target_lakehouse_id in lakehouse_id_mapping_ids_only.items():
        pipeline_content_str = pipeline_content_str.replace(source_lakehouse_id, target_lakehouse_id)

    pipeline_content_str = pipeline_content_str.replace(source_workspace_id, target_workspace_id)
    
    pipeline_content_bytes = base64.b64encode(pipeline_content_str.encode('utf-8'))

    pipelineBody['definition']['parts'][0]['payload'] = pipeline_content_bytes

    pipelineBody_updated = pipelineBody.copy()

    pipelineBody_updated.pop('workspaceId')
    pipelineBody_updated.pop('id')
    folder_id = ensure_workspace_folder(target_workspace_id)
    if folder_id:
        pipelineBody_updated["folderId"] = folder_id
    
    if item_name in target_pipelines_names:
        print(f"Updating Pipeline, {item_name}")
        target_item_id = [item['id'] for item in target_pipelines_json if item['displayName'] == item_name][0]
        update_item_url = f"{base_url}/workspaces/{target_workspace_id}/items/{target_item_id}/updateDefinition"
        response = requests.post(update_item_url, headers = fabric_headers, json = pipelineBody_updated, stream = True)
        if response.status_code == 202:
            time.sleep(20)
            redirect_url = response.headers.get('Location')
            response = requests.get(redirect_url, headers = fabric_headers)
        
        if response.status_code == 200 and response.text == "":
            print("Update pipeline command accepted by server, but server returned no response")
            data = {}
        else:
            data = response.json()
    
    else:
        print(f"Creating Target Pipeline, {item_name}")
        create_payload = dict(pipelineBody_updated)
        if folder_id:
            create_payload['folderId'] = folder_id
        response = requests.post(create_target_item_url, headers = fabric_headers, json = create_payload)
        if response.status_code == 202:
            time.sleep(20)
            redirect_url = response.headers.get('Location')
            response = requests.get(redirect_url, headers = fabric_headers)
        
        data = response.json()

        if not data.get('errorCode'):
            target_pipelines_and_ids[data['displayName']] = data['id']
            pipeline_id_mapping = {key: [source_pipelines_and_ids.get(key), target_pipelines_and_ids.get(key)] for key in source_pipelines_and_ids}

    if data.get('errorCode'):
        raise Exception(data.get('message'))
        
    return data, pipeline_id_mapping

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_or_update_notebook(create_target_item_url, fabric_headers, body, target_item_names, target_items_json, target_workspace_id, target_lakehouses_and_ids):
    item_name = body.get('displayName')

    source_workspace_id = body.get('workspaceId')

    notebook_content = body['definition']['parts'][0]['payload']

    notebook_content_str = base64.b64decode(notebook_content).decode("utf-8")

    lakehouseid = re.findall(r'.*"default_lakehouse":\s+"(.*)".*', notebook_content_str)

    if lakehouseid != []:
        source_lakehouse_id = re.findall(r'.*"default_lakehouse":\s+"(.*)".*', notebook_content_str)[0]
        source_lakehouse_name = re.findall(r'.*"default_lakehouse_name":\s+"(.*)".*', notebook_content_str)[0]
        
        # Map source lakehouse (Bronze, Silver, Gold, Metadata_Files) to target based on source name
        # Source lakehouses are always named Bronze, Silver, Gold, Metadata_Files
        # Target lakehouses can have any custom name based on lakehouses_to_deploy config
        # The source name is the key in target_lakehouses_and_ids mapping
        
        lakehouse_id_mapping_ids_only = {}
        if source_lakehouse_name in target_lakehouses_and_ids:
            target_lakehouse_id = target_lakehouses_and_ids[source_lakehouse_name]['id']
            target_lakehouse_name_mapped = target_lakehouses_and_ids[source_lakehouse_name]['name']
            
            # Replace the source lakehouse ID with target lakehouse ID
            notebook_content_str = notebook_content_str.replace(source_lakehouse_id, target_lakehouse_id)
            # Also replace the source lakehouse name with target lakehouse name
            notebook_content_str = notebook_content_str.replace(f'"{source_lakehouse_name}"', f'"{target_lakehouse_name_mapped}"')
            
            # Store mapping for aggregation
            lakehouse_id_mapping_ids_only[source_lakehouse_id] = target_lakehouse_id
        else:
            lakehouse_id_mapping_ids_only = 'No Mapping'
    else:
        lakehouse_id_mapping_ids_only = 'No Mapping'

    notebook_content_str = notebook_content_str.replace(source_workspace_id, target_workspace_id)
    
    notebook_content_bytes = base64.b64encode(notebook_content_str.encode('utf-8'))

    body['definition']['parts'][0]['payload'] = notebook_content_bytes

    body_updated = body.copy()

    body_updated.pop('workspaceId')
    body_updated.pop('id')
    folder_id = ensure_workspace_folder(target_workspace_id)
    if folder_id:
        body_updated["folderId"] = folder_id

    if item_name in target_item_names:
        print(f"Updating Notebook, {item_name}")
        target_item_id = [item['id'] for item in target_items_json if item['displayName'] == item_name][0]
        update_item_url = f"{base_url}/workspaces/{target_workspace_id}/items/{target_item_id}/updateDefinition?updateMetadata=True"
        response = requests.post(update_item_url, headers = fabric_headers, json = body_updated)

    else:
        print(f"Creating New Notebook, {item_name}")
        create_payload = dict(body_updated)
        if folder_id:
            create_payload['folderId'] = folder_id
        response = requests.post(create_target_item_url, headers = fabric_headers, json = create_payload)
    
    if response.status_code == 202:
        redirect_url = response.headers.get('Location')
        percentComplete = 0
        while percentComplete != 100:
            time.sleep(1)
            response = requests.get(redirect_url, headers = fabric_headers)
            percentComplete = response.json().get('percentComplete')
    
    data = response.json()
    if data.get('errorCode'):
        raise Exception(data.get('message'))
        
    return data, lakehouse_id_mapping_ids_only


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Build comprehensive lakehouse mapping from all workspaces
# Structure: {source_name: {'id': target_lakehouse_id, 'name': target_lakehouse_name}}
# Source lakehouses always have fixed names: Bronze, Silver, Gold, Metadata_Files
# Target lakehouses can have custom names based on lakehouses_to_deploy config
# The source name is the key because that's the static value we search for in artifacts
target_lakehouses_and_ids = {}

# Define source lakehouse names (these are fixed in source workspace)
source_lakehouse_names = {
    'bronze': 'Bronze',
    'silver': 'Silver', 
    'gold': 'Gold',
    'metadata_files': 'Metadata_Files'
}

for key, config in lakehouses_to_deploy.items():
    lakehouse_name = config.get('lakehouse_name', key)  # Use lakehouse_name if provided, else use key
    workspace_id = config['workspace_id']
    
    get_lakehouses_url = f"{base_url}/workspaces/{workspace_id}/items?type=Lakehouse"
    lakehouses_in_workspace = requests.get(get_lakehouses_url, headers = fabric_headers)
    lakehouses_and_ids = {lh['displayName']: lh['id'] for lh in lakehouses_in_workspace.json().get('value', [])}
    
    # Case-insensitive lookup
    lakehouse_id = next((v for k, v in lakehouses_and_ids.items() if k.lower() == lakehouse_name.lower()), None)
    if lakehouse_id:
        # Get the source name for this config key (e.g., 'bronze' -> 'Bronze')
        source_name = source_lakehouse_names.get(key.lower(), key)
        # Map from source name to target lakehouse details
        target_lakehouses_and_ids[source_name] = {'id': lakehouse_id, 'name': lakehouse_name}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

lakehouse_id_mapping_ids_only_all = {}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for notebookBody in source_notebooks_json:
    create_target_item_url = f"{base_url}/workspaces/{spark_compute_workspace_id}/items"
    return_value, lakehouse_ids = create_or_update_notebook(create_target_item_url = create_target_item_url
                                    , fabric_headers = fabric_headers
                                    , body = notebookBody
                                    , target_item_names = target_notebooks_names
                                    , target_items_json = target_notebooks_json
                                    , target_workspace_id = spark_compute_workspace_id
                                    , target_lakehouses_and_ids = target_lakehouses_and_ids
                                    )
    if lakehouse_ids != 'No Mapping':
        lakehouse_id_mapping_ids_only_all = lakehouse_id_mapping_ids_only_all | lakehouse_ids

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

target_notebooks = requests.get(get_target_notebooks_url, headers = fabric_headers)
target_notebooks_json = target_notebooks.json().get('value')
target_notebooks_and_ids = {notebook['displayName']: notebook['id'] for notebook in target_notebooks_json}
notebook_id_mapping = {key: [source_notebooks_and_ids.get(key), target_notebooks_and_ids.get(key)] for key in source_notebooks_and_ids}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Build comprehensive warehouse details from all workspaces
# Structure: {source_name: {'id': target_warehouse_id, 'name': target_warehouse_name, 'endpoint': connection_string}}
# Source warehouses always have fixed names (e.g., 'metadata')
# Target warehouses can have custom names based on warehouses_to_deploy config
# The source name is the key because that's the static value we search for in artifacts
target_warehouse_details = {}

# Define source warehouse names (these are fixed in source workspace)
source_warehouse_names = {
    'metadata': 'metadata'
}

for key, config in warehouses_to_deploy.items():
    warehouse_name = config.get('warehouse_name', key)  # Use warehouse_name if provided, else use key
    workspace_id = config['workspace_id']
    
    get_warehouses_url = f"{base_url}/workspaces/{workspace_id}/items?type=Warehouse"
    warehouses_in_workspace = requests.get(get_warehouses_url, headers = fabric_headers)
    warehouses_in_workspace_json = warehouses_in_workspace.json().get('value', [])
    
    # Find the warehouse with case-insensitive match
    warehouse = next((wh for wh in warehouses_in_workspace_json if wh['displayName'].lower() == warehouse_name.lower()), None)
    if warehouse:
        get_warehouse_details_url = f"{base_url}/workspaces/{workspace_id}/warehouses/{warehouse['id']}" 
        get_warehouse_details = requests.get(get_warehouse_details_url, headers = fabric_headers).json()
        warehouse_endpoint = get_warehouse_details['properties']['connectionString']
        
        # Get the source name for this config key
        source_name = source_warehouse_names.get(key.lower(), key)
        # Map from source name to target warehouse details
        target_warehouse_details[source_name] = {
            'id': warehouse['id'],
            'name': warehouse_name,
            'endpoint': warehouse_endpoint
        }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Build warehouse mapping dynamically
# Map source warehouse IDs and endpoints to target warehouse IDs and endpoints
warehouse_mapping = {}

# Source warehouse is always 'metadata' (from source_warehouse_details)
# Target warehouse is in target_warehouse_details with key 'metadata'
if 'metadata' in target_warehouse_details:
    # Map source metadata ID to target metadata ID
    if 'metadata_id' in source_warehouse_details:
        warehouse_mapping[source_warehouse_details['metadata_id']] = target_warehouse_details['metadata']['id']
    
    # Map source metadata endpoint to target metadata endpoint
    if 'metadata_endpoint' in source_warehouse_details:
        warehouse_mapping[source_warehouse_details['metadata_endpoint']] = target_warehouse_details['metadata']['endpoint']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Source Connection string - use metadata warehouse from target_warehouse_details
driver = "ODBC Driver 18 for SQL Server"
if 'metadata' not in target_warehouse_details:
    raise Exception("Metadata warehouse not found in target_warehouse_details")
    
server = target_warehouse_details['metadata']['endpoint']
database = target_warehouse_details['metadata']['name']  # Use the actual warehouse name
odbc_str = f"Driver={{{driver}}};Server={server};Database={database};"

# URL encode the connection string
params = urllib.parse.quote_plus(odbc_str)

# URL encode the connection string
fabric_token = notebookutils.credentials.getToken('pbi')
token = str.encode(fabric_token) 
exptoken = b""
for i in token:
    exptoken += bytes({i})
    exptoken += bytes(1)
token_struct = struct.pack("=i", len(exptoken)) + exptoken

# Create SQLAlchemy engine with token
engine = create_engine(
 f"mssql+pyodbc:///?odbc_connect={params}",
 connect_args={"attrs_before": {1256: token_struct}} # 1256 = SQL_COPT_SS_ACCESS_TOKEN
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sql_statements = SQLArtifacts.split('GO')
print("Deploying SQL Tables and Stored Procedures to the Warehouse")
with engine.connect() as conn:
    for statement in sql_statements:
        conn.execute(text(statement))
        conn.commit()

conn.close()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Populating Datastore_Configuration Table")

# Build datastore configuration records for the Datastore_Configuration table
# This replaces the per-datastore Variable Library entries
datastore_records = []

# Map medallion layer names
medallion_layer_map = {
    'bronze': 'Bronze',
    'silver': 'Silver',
    'gold': 'Gold',
    'metadata_files': None  # metadata_files doesn't have a medallion layer
}

# Process lakehouses
for key, config in lakehouses_to_deploy.items():
    lakehouse_name = config.get('lakehouse_name', key)
    workspace_id = config['workspace_id']
    
    # Get lakehouse ID from the target workspace
    get_lakehouses_url = f"{base_url}/workspaces/{workspace_id}/items?type=Lakehouse"
    lakehouses_in_workspace = requests.get(get_lakehouses_url, headers = fabric_headers)
    lakehouses_and_ids = {lh['displayName']: lh['id'] for lh in lakehouses_in_workspace.json().get('value', [])}
    
    # Case-insensitive lookup
    lakehouse_id = next((v for k, v in lakehouses_and_ids.items() if k.lower() == lakehouse_name.lower()), None)
    
    # Get workspace name
    get_workspace_url = f"{base_url}/workspaces/{workspace_id}"
    workspace_details = requests.get(get_workspace_url, headers = fabric_headers).json()
    workspace_name = workspace_details.get('displayName', workspace_id)
    
    if lakehouse_id:
        medallion_layer = medallion_layer_map.get(key.lower())
        datastore_records.append({
            'Datastore_Name': lakehouse_name.lower(),
            'Datastore_Type': 'Lakehouse',
            'Datastore_ID': lakehouse_id,
            'Workspace_ID': workspace_id,
            'Workspace_Name': workspace_name,
            'Medallion_Layer': medallion_layer,
            'Endpoint': None,
            'Connection_ID': None
        })

# Process warehouses
for key, config in warehouses_to_deploy.items():
    warehouse_name = config.get('warehouse_name', key)
    workspace_id = config['workspace_id']
    
    # Get warehouse details from the target workspace
    get_warehouses_url = f"{base_url}/workspaces/{workspace_id}/items?type=Warehouse"
    warehouses_in_workspace = requests.get(get_warehouses_url, headers = fabric_headers)
    warehouses_in_workspace_json = warehouses_in_workspace.json().get('value', [])
    
    # Find the warehouse with case-insensitive match
    warehouse = next((wh for wh in warehouses_in_workspace_json if wh['displayName'].lower() == warehouse_name.lower()), None)
    
    if warehouse:
        # Get warehouse endpoint
        get_warehouse_details_url = f"{base_url}/workspaces/{workspace_id}/warehouses/{warehouse['id']}"
        get_warehouse_details = requests.get(get_warehouse_details_url, headers = fabric_headers).json()
        warehouse_endpoint = get_warehouse_details['properties']['connectionString']
        
        # Get workspace name
        get_workspace_url = f"{base_url}/workspaces/{workspace_id}"
        workspace_details = requests.get(get_workspace_url, headers = fabric_headers).json()
        workspace_name = workspace_details.get('displayName', workspace_id)
        
        # Metadata warehouse is typically considered part of the "Gold" layer for reporting purposes
        medallion_layer = 'Gold' if key.lower() == 'metadata' else medallion_layer_map.get(key.lower())
        
        datastore_records.append({
            'Datastore_Name': warehouse_name.lower() if key.lower() == 'metadata' else warehouse_name.lower(),
            'Datastore_Type': 'Warehouse',
            'Datastore_ID': warehouse['id'],
            'Workspace_ID': workspace_id,
            'Workspace_Name': workspace_name,
            'Medallion_Layer': medallion_layer,
            'Endpoint': warehouse_endpoint,
            'Connection_ID': None
        })

# Insert records into Datastore_Configuration table
print(f"Inserting {len(datastore_records)} datastore records into Datastore_Configuration table")

# Clear existing records first (full refresh approach)
with engine.connect() as conn:
    conn.execute(text("TRUNCATE TABLE dbo.Datastore_Configuration;"))
    conn.commit()
    
    # Insert each datastore record
    for record in datastore_records:
        endpoint_value = f"'{record['Endpoint']}'" if record['Endpoint'] else 'NULL'
        medallion_value = f"'{record['Medallion_Layer']}'" if record['Medallion_Layer'] else 'NULL'
        connection_value = f"'{record['Connection_ID']}'" if record['Connection_ID'] else 'NULL'
        
        insert_sql = f"""
        INSERT INTO dbo.Datastore_Configuration 
            (Datastore_Name, Datastore_Type, Datastore_ID, Workspace_ID, Workspace_Name, Medallion_Layer, Endpoint, Connection_ID)
        VALUES
            ('{record['Datastore_Name']}', '{record['Datastore_Type']}', 
             '{record['Datastore_ID']}', '{record['Workspace_ID']}', '{record['Workspace_Name']}', 
             {medallion_value}, {endpoint_value}, {connection_value});
        """
        conn.execute(text(insert_sql))
        conn.commit()
        print(f"  ✓ Inserted: {record['Datastore_Name']} ({record['Datastore_Type']})")

conn.close()
print("✅ Datastore configuration complete")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# Create Default Spark Environment (ENV_Default)
# =============================================================================
# Creates an empty Fabric Environment that can be customized with:
#   - Custom Spark cluster sizes
#   - External Python/R libraries
#   - Spark configuration properties
# The environment ID is stored in the Variable Library for pipeline use.
# =============================================================================

print("Creating Default Spark Environment (ENV_Default)...")

env_name = "ENV_Default"
env_workspace_id = spark_compute_workspace_id

# Check if environment already exists
get_envs_url = f"{base_url}/workspaces/{env_workspace_id}/environments"
envs_response = requests.get(get_envs_url, headers=fabric_headers)
existing_envs = {env['displayName']: env['id'] for env in envs_response.json().get('value', [])}

if env_name in existing_envs:
    default_spark_environment_id = existing_envs[env_name]
    print(f"  Environment '{env_name}' already exists (ID: {default_spark_environment_id})")
else:
    # Create the environment
    env_body = {
        "displayName": env_name,
        "description": "Default Spark environment for the Data Platform Accelerator. Customize cluster size and libraries as needed."
    }

    response = requests.post(get_envs_url, headers=fabric_headers, json=env_body)

    if response.status_code == 201:
        # Direct success
        response_data = response.json()
        default_spark_environment_id = response_data.get('id', '')
        print(f"  ✓ Environment '{env_name}' created (ID: {default_spark_environment_id})")
    elif response.status_code == 202:
        # Long-running operation - poll for completion
        redirect_url = response.headers.get('Location')
        if redirect_url:
            max_retries = 60
            retry_count = 0
            while retry_count < max_retries:
                time.sleep(2)
                retry_count += 1
                poll_response = requests.get(redirect_url, headers=fabric_headers)
                poll_json = poll_response.json()
                status = poll_json.get('status')
                if status == 'Succeeded':
                    break
                elif status == 'Failed':
                    error_info = poll_json.get('error', {})
                    print(f"  ✗ Operation failed: {error_info}")
                    break
                percent_complete = poll_json.get('percentComplete', 0)
                if percent_complete >= 100:
                    break
            else:
                print(f"  ⚠️ Timeout waiting for environment creation")

        # Re-fetch to get the environment ID
        envs_response = requests.get(get_envs_url, headers=fabric_headers)
        existing_envs = {env['displayName']: env['id'] for env in envs_response.json().get('value', [])}
        default_spark_environment_id = existing_envs.get(env_name, '')
        if default_spark_environment_id:
            print(f"  ✓ Environment '{env_name}' created (ID: {default_spark_environment_id})")
        else:
            print(f"  ⚠️ Environment created but could not retrieve ID")
    else:
        error_msg = response.json().get('message', response.text) if response.text else 'Unknown error'
        print(f"  ⚠️ Could not create environment: {error_msg}")
        default_spark_environment_id = ""

print(f"✅ Default Spark Environment ready: {default_spark_environment_id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Creating Variable Library (Pipeline Runtime Variables)")

# Build variables for pipeline runtime (non-datastore variables only)
# Datastore configuration is now stored in dbo.Datastore_Configuration table
variables_list = []

# Get metadata warehouse details for logging/metadata variables
metadata_workspace_id_var = warehouses_to_deploy.get('metadata', {}).get('workspace_id', report_semantic_model_workspace_id)
metadata_files_workspace_id = lakehouses_to_deploy.get('metadata_files', {}).get('workspace_id', spark_compute_workspace_id)

# Get metadata warehouse details
metadata_warehouse_name_for_lookup = warehouses_to_deploy.get('metadata', {}).get('warehouse_name', 'metadata')
get_warehouses_url = f"{base_url}/workspaces/{metadata_workspace_id_var}/items?type=Warehouse"
warehouses_in_workspace = requests.get(get_warehouses_url, headers = fabric_headers)
warehouses_in_workspace_json = warehouses_in_workspace.json().get('value', [])

metadata_warehouse = next((wh for wh in warehouses_in_workspace_json if wh['displayName'].lower() == metadata_warehouse_name_for_lookup.lower()), None)
if metadata_warehouse:
    get_warehouse_details_url = f"{base_url}/workspaces/{metadata_workspace_id_var}/warehouses/{metadata_warehouse['id']}"
    get_warehouse_details = requests.get(get_warehouse_details_url, headers = fabric_headers).json()
    metadata_endpoint = get_warehouse_details['properties']['connectionString']
    
    # Metadata warehouse variables (needed for pipeline orchestration)
    variables_list.append(f"  {{'name': 'metadata_workspace_id', 'note': '', 'type': 'String', 'value': '{metadata_workspace_id_var}'}}")
    variables_list.append(f"  {{'name': 'metadata_warehouse_id', 'note': '', 'type': 'String', 'value': '{metadata_warehouse['id']}'}}")
    variables_list.append(f"  {{'name': 'metadata_warehouse_endpoint', 'note': '', 'type': 'String', 'value': '{metadata_endpoint}'}}")
    
    # Logging variables (same as metadata - for backward compatibility)
    variables_list.append(f"  {{'name': 'logging_workspace_id', 'note': '', 'type': 'String', 'value': '{metadata_workspace_id_var}'}}")
    variables_list.append(f"  {{'name': 'logging_warehouse_id', 'note': '', 'type': 'String', 'value': '{metadata_warehouse['id']}'}}")
    variables_list.append(f"  {{'name': 'logging_warehouse_endpoint', 'note': '', 'type': 'String', 'value': '{metadata_endpoint}'}}")

# Get metadata_files lakehouse details
metadata_files_lakehouse_name = lakehouses_to_deploy.get('metadata_files', {}).get('lakehouse_name', 'metadata_files')
get_lakehouses_url = f"{base_url}/workspaces/{metadata_files_workspace_id}/items?type=Lakehouse"
lakehouses_in_workspace = requests.get(get_lakehouses_url, headers = fabric_headers)
lakehouses_and_ids = {lh['displayName']: lh['id'] for lh in lakehouses_in_workspace.json().get('value', [])}
metadata_files_id = next((v for k, v in lakehouses_and_ids.items() if k.lower() == metadata_files_lakehouse_name.lower()), None)

if metadata_files_id:
    variables_list.append(f"  {{'name': 'metadata_lakehouse_id', 'note': '', 'type': 'String', 'value': '{metadata_files_id}'}}")
    variables_list.append(f"  {{'name': 'metadata_lakehouse_name', 'note': '', 'type': 'String', 'value': '{metadata_files_lakehouse_name}'}}")

# Spark compute workspace ID
variables_list.append(f"  {{'name': 'spark_compute_workspace_id', 'note': '', 'type': 'String', 'value': '{spark_compute_workspace_id}'}}")

# Notebook ID variables
variables_list.append(f"  {{'name': 'notebook_id_batch_processing', 'note': '', 'type': 'String', 'value': '{target_notebooks_and_ids['NB_Batch_Processing']}'}}")
variables_list.append(f"  {{'name': 'notebook_id_exploratory_data_analysis', 'note': '', 'type': 'String', 'value': '{target_notebooks_and_ids['NB_Run_Exploratory_Data_Analysis']}'}}")
variables_list.append(f"  {{'name': 'notebook_id_extract_error_logs_in_html', 'note': '', 'type': 'String', 'value': '{target_notebooks_and_ids['NB_Extract_Error_Logs_In_Html']}'}}")

# Default Spark Environment ID
variables_list.append(f"  {{'name': 'default_spark_environment_id', 'note': '', 'type': 'String', 'value': '{default_spark_environment_id}'}}")

# Join all variables with commas
variables_str = ",\n".join(variables_list)

payload = f'''{{'$schema': 'https://developer.microsoft.com/json-schemas/fabric/item/variableLibrary/definition/variables/1.0.0/schema.json',
 'variables': [
{variables_str}
  ]}}
   '''

payload_bytes = base64.b64encode(payload.encode('utf-8'))

body = {
  "displayName": "VL_Workspace_Variables",
  "description": "Pipeline runtime variables. Datastore configuration is stored in dbo.Datastore_Configuration table.",
  'definition': {'parts': [{'path': 'variables.json',
    'payload': payload_bytes,
    'payloadType': 'InlineBase64'},
   {'path': 'settings.json',
    'payload': 'ew0KICAiJHNjaGVtYSI6ICJodHRwczovL2RldmVsb3Blci5taWNyb3NvZnQuY29tL2pzb24tc2NoZW1hcy9mYWJyaWMvaXRlbS92YXJpYWJsZUxpYnJhcnkvZGVmaW5pdGlvbi9zZXR0aW5ncy8xLjAuMC9zY2hlbWEuanNvbiIsDQogICJ2YWx1ZVNldHNPcmRlciI6IFtdDQp9',
    'payloadType': 'InlineBase64'},
   {'path': 'valueSets/FeatureWorkspaceConfig.json',
    'payload': 'ew0KICAiJHNjaGVtYSI6ICJodHRwczovL2RldmVsb3Blci5taWNyb3NvZnQuY29tL2pzb24tc2NoZW1hcy9mYWJyaWMvaXRlbS92YXJpYWJsZUxpYnJhcnkvZGVmaW5pdGlvbi92YWx1ZVNldC8xLjAuMC9zY2hlbWEuanNvbiIsDQogICJuYW1lIjogIkZlYXR1cmVXb3Jrc3BhY2VDb25maWciLA0KICAidmFyaWFibGVPdmVycmlkZXMiOiBbXQ0KfQ==',
    'payloadType': 'InlineBase64'},
   {'path': '.platform',
    'payload': 'ewogICIkc2NoZW1hIjogImh0dHBzOi8vZGV2ZWxvcGVyLm1pY3Jvc29mdC5jb20vanNvbi1zY2hlbWFzL2ZhYnJpYy9naXRJbnRlZ3JhdGlvbi9wbGF0Zm9ybVByb3BlcnRpZXMvMi4wLjAvc2NoZW1hLmpzb24iLAogICJtZXRhZGF0YSI6IHsKICAgICJ0eXBlIjogIlZhcmlhYmxlTGlicmFyeSIsCiAgICAiZGlzcGxheU5hbWUiOiAiV29ya3NwYWNlVmFyaWFibGVzIiwKICAgICJkZXNjcmlwdGlvbiI6ICIiCiAgfSwKICAiY29uZmlnIjogewogICAgInZlcnNpb24iOiAiMi4wIiwKICAgICJsb2dpY2FsSWQiOiAiMDAwMDAwMDAtMDAwMC0wMDAwLTAwMDAtMDAwMDAwMDAwMDAwIgogIH0KfQ==',
    'payloadType': 'InlineBase64'}]}}

folder_id = ensure_workspace_folder(pipeline_workspace_id)
if folder_id:
    body["folderId"] = folder_id
# Variable Library goes to the pipelines workspace
url = f'https://api.fabric.microsoft.com/v1/workspaces/{pipeline_workspace_id}/VariableLibraries'

response = requests.post(url, headers = fabric_headers, json = body)

if response.json():
    errorCode = response.json().get('errorCode') 
    if errorCode == 'ItemDisplayNameAlreadyInUse':
        # update variable library if it already exists
        print("Variable Library already exists, updating")
        response = requests.get(url, headers = fabric_headers)
        vl_id = [vl['id'] for vl in response.json()['value'] if vl['displayName'] == 'VL_Workspace_Variables'][0]
        update_url = f'https://api.fabric.microsoft.com/v1/workspaces/{pipeline_workspace_id}/VariableLibraries/{vl_id}/updateDefinition'
        response = requests.post(update_url, headers = fabric_headers, json = body)
    elif errorCode and errorCode != 'ItemDisplayNameAlreadyInUse':
        raise Exception(response.json().get('message'))

print("✅ Variable Library created/updated")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# Create Datastores and Metadata Folders in Fabric Workspace
# =============================================================================
# This creates:
#   1. A "datastores" folder containing a dynamically-generated datastore notebook
#   2. An empty "metadata" folder for users to add their metadata SQL files
# =============================================================================

print("Creating 'datastores' and 'metadata' folders with datastore configuration notebook...")

# Always use "DEV" as the environment name for deployment
# Users can create additional environment-specific notebooks (datastore_PROD, etc.) manually or via CI/CD
environment_name = "DEV"

notebook_name = f"datastore_{environment_name}"
print(f"  Environment: {environment_name}")
print(f"  Datastore notebook name: {notebook_name}")

# Generate the current date for documentation
from datetime import datetime
current_date = datetime.now().strftime("%Y-%m-%d")

# Build the INSERT VALUES for all deployed datastores
insert_values = []

# Add lakehouses
for key, config in lakehouses_to_deploy.items():
    lakehouse_name = config.get('lakehouse_name', key)
    workspace_id = config['workspace_id']
    
    # Get lakehouse ID 
    get_lakehouses_url = f"{base_url}/workspaces/{workspace_id}/items?type=Lakehouse"
    lakehouses_in_workspace = requests.get(get_lakehouses_url, headers=fabric_headers)
    lakehouses_and_ids = {lh['displayName']: lh['id'] for lh in lakehouses_in_workspace.json().get('value', [])}
    lakehouse_id = next((v for k, v in lakehouses_and_ids.items() if k.lower() == lakehouse_name.lower()), None)
    
    # Get workspace name
    get_workspace_url = f"{base_url}/workspaces/{workspace_id}"
    workspace_details = requests.get(get_workspace_url, headers=fabric_headers).json()
    workspace_name = workspace_details.get('displayName', workspace_id)
    
    if lakehouse_id:
        medallion_layer = medallion_layer_map.get(key.lower())
        medallion_str = f"'{medallion_layer}'" if medallion_layer else 'NULL'
        insert_values.append(
            f"('{lakehouse_name.lower()}', 'Lakehouse', '{lakehouse_id}', '{workspace_id}', '{workspace_name}', {medallion_str}, NULL, NULL)"
        )

# Add warehouses
for key, config in warehouses_to_deploy.items():
    warehouse_name = config.get('warehouse_name', key)
    workspace_id = config['workspace_id']
    
    # Get warehouse details
    get_warehouses_url = f"{base_url}/workspaces/{workspace_id}/items?type=Warehouse"
    warehouses_in_workspace = requests.get(get_warehouses_url, headers=fabric_headers)
    warehouses_in_workspace_json = warehouses_in_workspace.json().get('value', [])
    warehouse = next((wh for wh in warehouses_in_workspace_json if wh['displayName'].lower() == warehouse_name.lower()), None)
    
    if warehouse:
        get_warehouse_details_url = f"{base_url}/workspaces/{workspace_id}/warehouses/{warehouse['id']}"
        get_warehouse_details = requests.get(get_warehouse_details_url, headers=fabric_headers).json()
        warehouse_endpoint = get_warehouse_details['properties']['connectionString']
        
        # Get workspace name
        get_workspace_url = f"{base_url}/workspaces/{workspace_id}"
        workspace_details = requests.get(get_workspace_url, headers=fabric_headers).json()
        workspace_name = workspace_details.get('displayName', workspace_id)
        
        medallion_layer = 'Gold' if key.lower() == 'metadata' else medallion_layer_map.get(key.lower())
        medallion_str = f"'{medallion_layer}'" if medallion_layer else 'NULL'
        insert_values.append(
            f"('{warehouse_name.lower()}', 'Warehouse', '{warehouse['id']}', '{workspace_id}', '{workspace_name}', {medallion_str}, '{warehouse_endpoint}', NULL)"
        )

# Build the SQL notebook content
insert_values_str = ",\n".join(insert_values)

# Get metadata warehouse ID for notebook dependency
metadata_warehouse_id = target_warehouse_details.get('metadata', {}).get('id', '')

notebook_sql_content = f'''-- Fabric notebook source

-- METADATA ********************

-- META {{
-- META   "kernel_info": {{
-- META     "name": "sqldatawarehouse"
-- META   }},
-- META   "dependencies": {{
-- META     "warehouse": {{
-- META       "default_warehouse": "{metadata_warehouse_id}",
-- META       "known_warehouses": [
-- META         {{
-- META           "id": "{metadata_warehouse_id}",
-- META           "type": "Datawarehouse"
-- META         }}
-- META       ]
-- META     }}
-- META   }}
-- META }}

-- CELL ********************

-- =====================================================================
-- Datastore Configuration: {environment_name} Environment
-- Environment: {environment_name}
-- Generated: {current_date}
-- =====================================================================
-- Purpose:
--   Register all Fabric datastores (Lakehouses, Warehouses) for the {environment_name} environment.
--   This replaces Variable Library entries for datastore details.
--
-- CI/CD Notes:
--   - Create separate notebooks for each environment (datastore_DEV, datastore_PROD, etc.)
--   - Each notebook contains environment-specific GUIDs
--   - Deploy the appropriate notebook as part of your ADO pipeline
--
-- To Add a New Datastore:
--   1. Create the Lakehouse/Warehouse in Fabric
--   2. Copy the artifact GUID and workspace GUID from Fabric
--   3. Add an INSERT statement below
--   4. Run this cell or deploy via CI/CD
-- =====================================================================

-- METADATA ********************

-- META {{
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }}

-- CELL ********************

-- =====================================================================
-- STEP 1: Clear existing datastore configuration (full refresh)
-- =====================================================================
TRUNCATE TABLE dbo.Datastore_Configuration;

-- METADATA ********************

-- META {{
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }}

-- CELL ********************

-- =====================================================================
-- STEP 2: Insert {environment_name} environment datastore configuration
-- =====================================================================

INSERT INTO [dbo].[Datastore_Configuration] 
    (Datastore_Name, Datastore_Type, Datastore_ID, Workspace_ID, Workspace_Name, Medallion_Layer, Endpoint, Connection_ID)
VALUES
-- Core Medallion Architecture Datastores
{insert_values_str};

-- =====================================================================
-- Add additional datastores below as needed:
-- =====================================================================
-- Example: Adding a new domain-specific lakehouse
-- INSERT INTO [dbo].[Datastore_Configuration] 
--     (Datastore_Name, Datastore_Type, Datastore_ID, Workspace_ID, Workspace_Name, Medallion_Layer, Endpoint, Connection_ID)
-- VALUES
-- ('sales_bronze', 'Lakehouse', '<lakehouse-guid>', '<workspace-guid>', '{environment_name.lower()}', 'Bronze', NULL, NULL);

-- METADATA ********************

-- META {{
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }}
'''

# Generate unique logical ID for the notebook
import uuid
notebook_logical_id = str(uuid.uuid4())

# Create the .platform file content
platform_content = json.dumps({
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
    "metadata": {
        "type": "Notebook",
        "displayName": notebook_name
    },
    "config": {
        "version": "2.0",
        "logicalId": notebook_logical_id
    }
}, indent=2)

# Base64 encode the notebook content
notebook_payload = base64.b64encode(notebook_sql_content.encode('utf-8')).decode('utf-8')
platform_payload = base64.b64encode(platform_content.encode('utf-8')).decode('utf-8')

# Create the notebook via Fabric API
notebook_body = {
    "displayName": notebook_name,
    "type": "Notebook",
    "description": f"Datastore configuration for {environment_name} environment. Run this notebook to register all datastores in dbo.Datastore_Configuration.",
    "definition": {
        "parts": [
            {
                "path": "notebook-content.sql",
                "payload": notebook_payload,
                "payloadType": "InlineBase64"
            },
            {
                "path": ".platform",
                "payload": platform_payload,
                "payloadType": "InlineBase64"
            }
        ]
    }
}

# === Use METADATA workspace for datastores folder and notebook ===
metadata_ws_id = warehouses_to_deploy.get('metadata', {}).get('workspace_id', metadata_workspace_id)
parent_folder_id = ensure_workspace_folder(metadata_ws_id) if normalized_folder_name else None
parent_folder_display = f"'{normalized_folder_name}'" if normalized_folder_name else "workspace root"
print(f"  Target workspace: metadata workspace ({metadata_ws_id})")
print(f"  Parent folder: {parent_folder_display}")

# Helper function to create a subfolder (optionally within parent folder)
def create_subfolder(workspace_id, folder_name, parent_id=None):
    """Create a folder, optionally as a child of another folder."""
    # List all folders to check if it exists
    list_url = f"{base_url}/workspaces/{workspace_id}/folders"
    response = requests.get(list_url, headers=fabric_headers)
    if response.status_code >= 400:
        return None, f"Could not list folders: {response.text}"
    
    folders = response.json().get('value', [])
    
    # Check if folder already exists (with matching parent if specified)
    for f in folders:
        if f.get('displayName', '').lower() == folder_name.lower():
            # Check if parent matches (if parent_id specified)
            if parent_id:
                if f.get('parentFolderId') == parent_id:
                    return f['id'], None  # Found matching folder with correct parent
            else:
                # No parent specified, check it's at root (no parentFolderId)
                if not f.get('parentFolderId'):
                    return f['id'], None
    
    # Folder doesn't exist, create it
    create_url = f"{base_url}/workspaces/{workspace_id}/folders"
    folder_body = {"displayName": folder_name}
    if parent_id:
        folder_body["parentFolderId"] = parent_id
    
    response = requests.post(create_url, headers=fabric_headers, json=folder_body)
    
    if response.status_code == 202:
        redirect_url = response.headers.get('Location')
        if redirect_url:
            percent_complete = 0
            while percent_complete != 100:
                time.sleep(1)
                poll_response = requests.get(redirect_url, headers=fabric_headers)
                poll_json = poll_response.json()
                percent_complete = poll_json.get('percentComplete', 100)
            response = poll_response
    
    if response.status_code < 400:
        folder_data = response.json() if response.text else {}
        return folder_data.get('id'), None
    else:
        return None, response.text

# === Create 'datastores' folder in METADATA workspace ===
print("  Creating 'datastores' folder...")
datastores_folder_id, error = create_subfolder(metadata_ws_id, "datastores", parent_folder_id)
if datastores_folder_id:
    print(f"  ✓ 'datastores' folder ready")
else:
    print(f"  ⚠️ Could not create 'datastores' folder: {error}")

# Add folder ID to notebook body if folder was created
if datastores_folder_id:
    notebook_body["folderId"] = datastores_folder_id

# Check if notebook already exists in METADATA workspace
get_notebooks_url = f"{base_url}/workspaces/{metadata_ws_id}/items?type=Notebook"
notebooks_response = requests.get(get_notebooks_url, headers=fabric_headers)
existing_notebooks = {nb['displayName']: nb['id'] for nb in notebooks_response.json().get('value', [])}

if notebook_name in existing_notebooks:
    print(f"  Updating existing notebook '{notebook_name}'...")
    existing_id = existing_notebooks[notebook_name]
    update_url = f"{base_url}/workspaces/{metadata_ws_id}/items/{existing_id}/updateDefinition?updateMetadata=True"
    response = requests.post(update_url, headers=fabric_headers, json=notebook_body)
else:
    print(f"  Creating notebook '{notebook_name}'...")
    create_url = f"{base_url}/workspaces/{metadata_ws_id}/items"
    response = requests.post(create_url, headers=fabric_headers, json=notebook_body)

# Debug: Print initial response
print(f"    Initial response: {response.status_code}")
if response.status_code >= 400:
    print(f"    Error response body: {response.text}")

if response.status_code == 202:
    redirect_url = response.headers.get('Location')
    if redirect_url:
        max_retries = 60  # Timeout after 60 seconds
        retry_count = 0
        while retry_count < max_retries:
            time.sleep(2)
            retry_count += 1
            poll_response = requests.get(redirect_url, headers=fabric_headers)
            poll_json = poll_response.json()
            percent_complete = poll_json.get('percentComplete')
            status = poll_json.get('status')
            # Debug: Print poll status
            if retry_count % 5 == 0 or status:
                print(f"    Poll {retry_count}: status={status}, percentComplete={percent_complete}")
            # Exit if completed, failed, or percentComplete is 100
            if status == 'Succeeded':
                response = poll_response
                print(f"    ✓ Operation succeeded")
                break
            elif status == 'Failed':
                error_info = poll_json.get('error', {})
                print(f"    ✗ Operation failed: {error_info}")
                response = poll_response
                break
            elif percent_complete is not None and percent_complete >= 100:
                response = poll_response
                break
        else:
            print(f"  ⚠️ Timeout waiting for notebook creation after {max_retries * 2}s")
    else:
        print(f"  ⚠️ No redirect URL in 202 response")
elif response.status_code in [200, 201]:
    # Direct success without polling needed
    print(f"    ✓ Direct success (no polling needed)")

# Check final status
response_data = {}
try:
    if response.text:
        response_data = response.json()
except:
    pass

if response.status_code < 400 and not response_data.get('errorCode'):
    print(f"  ✓ Datastore notebook '{notebook_name}' created/updated in 'datastores' folder")
else:
    error_msg = response_data.get('message') or response_data.get('error', {}).get('message') or response.text or 'Unknown error'
    print(f"  ⚠️ Could not create datastore notebook: {error_msg}")

# === Create empty 'metadata' folder in METADATA workspace ===
print("  Creating 'metadata' folder...")
metadata_folder_id, error = create_subfolder(metadata_ws_id, "metadata", parent_folder_id)
if metadata_folder_id:
    print(f"  ✓ 'metadata' folder ready")
else:
    print(f"  ⚠️ Could not create 'metadata' folder: {error}")

print("✅ Datastores and metadata folders setup complete")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for pipelineBody in source_pipelines_json:
    create_target_item_url = f"{base_url}/workspaces/{pipeline_workspace_id}/items"
    return_value, pipeline_id_mapping = create_or_update_pipeline(create_target_item_url = create_target_item_url
                                    , fabric_headers = fabric_headers
                                    , pipelineBody = pipelineBody
                                    , target_pipelines_names = target_pipelines_names
                                    , target_pipelines_json = target_pipelines_json
                                    , warehouse_mapping = warehouse_mapping
                                    , target_workspace_id = pipeline_workspace_id
                                    , target_pipelines_and_ids = target_pipelines_and_ids
                                    , pipeline_id_mapping = pipeline_id_mapping
                                    , notebook_id_mapping = notebook_id_mapping
                                    , lakehouse_id_mapping_ids_only = lakehouse_id_mapping_ids_only_all
                                    , source_connections = source_connections
                                    , target_connections = connection_ids
                                    , lakehouses_to_deploy = lakehouses_to_deploy
                                    , warehouses_to_deploy = warehouses_to_deploy)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_or_update_semantic_model(create_target_item_url
                            , fabric_headers
                            , body
                            , target_item_names
                            , target_items_json
                            , target_workspace_id
                            , warehouse_mapping
                            , lakehouse_id_mapping_ids_only
                            , log_analytics_workspace_id):

    item_name = body.get('displayName')

    source_workspace_id = body.get('workspaceId')
    target_workspace_id = create_target_item_url.split('/workspaces/')[1].split('/items')[0]
    folder_id = ensure_workspace_folder(target_workspace_id)

    parts = body['definition']['parts']

    for part in parts:
        path = part['path']
        if path == 'definition/expressions.tmdl':
            payload = part['payload']
            semantic_model_content_str = base64.b64decode(payload).decode("utf-8")

            for source_warehouse_value, target_warehouse_value in warehouse_mapping.items():
                if '.datawarehouse.fabric.microsoft.com' in target_warehouse_value:
                    target_warehouse_value = target_warehouse_value.upper().replace('.DATAWAREHOUSE.FABRIC.MICROSOFT.COM', '.datawarehouse.fabric.microsoft.com')
                semantic_model_content_str = re.sub(source_warehouse_value, target_warehouse_value, semantic_model_content_str, flags = re.I)

            for source_lakehouse_id, target_lakehouse_id in lakehouse_id_mapping_ids_only.items():
                semantic_model_content_str = semantic_model_content_str.replace(source_lakehouse_id, target_lakehouse_id)

            semantic_model_content_str = semantic_model_content_str.replace(source_workspace_id, target_workspace_id)

            if item_name == "SM_PBIReportPerformance":
                semantic_model_content_str = semantic_model_content_str.replace("11815e68-02e7-4170-8634-a67b5766af08", log_analytics_workspace_id)
            
            semantic_model_content_bytes = base64.b64encode(semantic_model_content_str.encode('utf-8'))

            part['payload'] = semantic_model_content_bytes


    body_updated = body.copy()
    body_updated.pop('workspaceId')
    body_updated.pop('id')
    if folder_id:
        body_updated["folderId"] = folder_id

    if item_name in target_item_names:
        print(f"Updating SemanticModel, {item_name}")
        target_item_id = [item['id'] for item in target_items_json if item['displayName'] == item_name][0]
        update_target_item_url = f"{base_url}/workspaces/{target_workspace_id}/items/{target_item_id}/updateDefinition?updateMetadata=True"
        response = requests.post(update_target_item_url, headers = fabric_headers, json = body_updated)

    else:
        print(f"Creating New SemanticModel, {item_name}")
        create_target_item_url = f"{base_url}/workspaces/{target_workspace_id}/items"
        create_payload = dict(body_updated)
        if folder_id:
            create_payload['folderId'] = folder_id
        response = requests.post(create_target_item_url, headers = fabric_headers, json = create_payload)

    if response.status_code == 202:
        redirect_url = response.headers.get('Location')
        percentComplete = 0
        while percentComplete != 100:
            time.sleep(4)
            response = requests.get(redirect_url, headers = fabric_headers)
            percentComplete = response.json().get('percentComplete')

    data = response.json()
    if data.get('errorCode'):
        raise Exception(data.get('message'))

    if data.get('status') == "Failed":
        if data['error']['errorCode'] == "Dataset_Import_FailedToImportDataset":
            print("One of more data tables underlying the semantic model doesn't exist yet in the target environment. Please try redeploying when all tables exist.")
        else:
            raise Exception(data['error']['message'])
        
    return data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


def create_or_update_report(create_target_item_url
                            , fabric_headers
                            , body
                            , target_item_names
                            , target_items_json
                            , target_workspace_name
                            , semantic_models_id_mapping_ids_only):

    item_name = body.get('displayName')

    source_workspace_id = body.get('workspaceId')
    target_workspace_id = create_target_item_url.split('/workspaces/')[1].split('/items')[0]
    folder_id = ensure_workspace_folder(target_workspace_id)

    parts = body['definition']['parts']

    for part in parts:
        path = part['path']
        if path == 'definition.pbir':
            payload = part['payload']
            report_content_str = base64.b64decode(payload).decode("utf-8")

            source_semantic_model = json.loads(report_content_str)['datasetReference']['byConnection']['connectionString']

            source_semantic_model_id_match = re.search(r'(?:^|;)\s*semanticmodelid\s*=\s*([^\s;]+)', source_semantic_model, flags=re.IGNORECASE)
            source_semantic_model_id = source_semantic_model_id_match.group(1) if source_semantic_model_id_match else None
            
            if not source_semantic_model_id:
                raise Exception(f"❌ Failed to extract semantic model ID from report '{item_name}'. The connection string does not contain a valid 'semanticmodelid' value. Connection string: {source_semantic_model}")

            target_semantic_model_id = semantic_models_id_mapping_ids_only.get(source_semantic_model_id)

            report_content_str = report_content_str.replace(source_semantic_model_id, target_semantic_model_id)

            report_content_str = re.sub(r'powerbi://api.powerbi.com/v1.0/myorg/(.*?);', f"powerbi://api.powerbi.com/v1.0/myorg/{target_workspace_name};", report_content_str)
                        
            report_content_bytes = base64.b64encode(report_content_str.encode('utf-8'))

            part['payload'] = report_content_bytes

    body_updated = body.copy()
    body_updated.pop('workspaceId')
    body_updated.pop('id')
    if folder_id:
        body_updated["folderId"] = folder_id

    if item_name in target_item_names:
        print(f"Updating Report: '{item_name}'")
        target_item_id = [item['id'] for item in target_items_json if item['displayName'] == item_name][0]
        update_target_item_url = f"{base_url}/workspaces/{target_workspace_id}/items/{target_item_id}/updateDefinition?updateMetadata=True"
        response = requests.post(update_target_item_url, headers = fabric_headers, json = body_updated)
    else:
        print(f"Creating Report: '{item_name}'")
        create_payload = dict(body_updated)
        if folder_id:
            create_payload['folderId'] = folder_id
        response = requests.post(create_target_item_url, headers = fabric_headers, json = create_payload)

    if response.status_code == 202:
        redirect_url = response.headers.get('Location')
        percentComplete = 0
        while percentComplete != 100:
            time.sleep(2)
            response = requests.get(redirect_url, headers = fabric_headers)
            percentComplete = response.json().get('percentComplete')

    data = response.json()
    if data.get('errorCode'):
        raise Exception(data.get('message'))

    if data.get('status') == "Failed":
        raise Exception(data['error']['message'])
        
    return data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for semantic_model_body in source_semantic_models_json:
    if not log_analytics_workspace_id and semantic_model_body.get('displayName') == 'SM_PBIReportPerformance':
        continue
    
    create_target_item_url = f"{base_url}/workspaces/{report_semantic_model_workspace_id}/items"
    return_value = create_or_update_semantic_model(create_target_item_url = create_target_item_url
                                    , fabric_headers = fabric_headers
                                    , body = semantic_model_body
                                    , target_item_names = target_semantic_models_names
                                    , target_items_json = target_semantic_models_json
                                    , target_workspace_id = report_semantic_model_workspace_id
                                    , warehouse_mapping = warehouse_mapping
                                    , lakehouse_id_mapping_ids_only = lakehouse_id_mapping_ids_only_all
                                    , log_analytics_workspace_id = log_analytics_workspace_id
                                    )
                                

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

target_semantic_models = requests.get(get_target_semantic_models_url, headers = fabric_headers)
target_semantic_models_json = target_semantic_models.json().get('value')
target_semantic_models_and_ids = {lakehouse['displayName']: lakehouse['id'] for lakehouse in target_semantic_models_json}
semantic_models_id_mapping = {key: [source_semantic_models_and_ids.get(key), target_semantic_models_and_ids.get(key)] for key in source_semantic_models_and_ids}
semantic_models_id_mapping_ids_only = {value[0]: value[1] for key, value in semantic_models_id_mapping.items()} 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for report_body in source_reports_json:
    if not log_analytics_workspace_id and report_body.get('displayName') == 'RP_PBIReportPerformance':
        continue

    create_target_item_url = f"{base_url}/workspaces/{report_semantic_model_workspace_id}/items"
    return_value = create_or_update_report(create_target_item_url = create_target_item_url
                                    , fabric_headers = fabric_headers
                                    , body = report_body
                                    , target_item_names = target_reports_names
                                    , target_items_json = target_reports_json
                                    , target_workspace_name = target_report_semantic_model_workspace_name                             
                                    , semantic_models_id_mapping_ids_only = semantic_models_id_mapping_ids_only
                                    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Refresh all semantic models AFTER reports are deployed
# This ensures reports have their connections properly established before data flows through
print("Refreshing all semantic models to ensure reports display data correctly...")

for semantic_model_body in source_semantic_models_json:
    semantic_model_name = semantic_model_body.get('displayName')
    
    try:
        print(f"  Refreshing: {semantic_model_name}")
        fabric.refresh_dataset(
            workspace=target_report_semantic_model_workspace_name, 
            dataset=semantic_model_name, 
            refresh_type="full"
        )
    except Exception as e:
        print(f"  ⚠️ Warning: Could not refresh {semantic_model_name}: {str(e)}")
        print(f"     You may need to manually refresh this semantic model in Power BI.")

print("✅ Semantic model refresh complete")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Insert Data into Date Dimension
print("Inserting Dates into Date Dimension Table")
from pyspark.sql import functions as f
import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants
from datetime import datetime, date
import json
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import (
    col, expr, concat_ws, year, month, dayofmonth, date_format,
    quarter, dayofweek, weekofyear, lit, lpad, substring
)
from pyspark.sql.types import DateType
import datetime

# Get metadata warehouse workspace ID and name for cross-workspace access
metadata_workspace_id = warehouses_to_deploy.get('metadata', {}).get('workspace_id', report_semantic_model_workspace_id)
metadata_warehouse_name = warehouses_to_deploy.get('metadata', {}).get('warehouse_name', 'metadata')

date_dimension_records = spark.read.option(Constants.WorkspaceId, metadata_workspace_id).synapsesql(f"{metadata_warehouse_name}.dbo.Date_Dimension").count()

if date_dimension_records == 0:
    # Define start and end dates
    start_date = datetime.date(2025, 3, 26)
    end_date = datetime.date(2035, 12, 31)
    
    # Generate a DataFrame with all dates in range
    date_df = spark.createDataFrame(
        [(start_date + datetime.timedelta(days=i),)
        for i in range((end_date - start_date).days + 1)],
        ["date"]
    )
    
    # Transform into Date Dimension format
    date_dim_df = date_df.withColumn("Date_Key", date_format("date", "yyyyMMdd").cast("int")) \
        .withColumn("Date", col("date")) \
        .withColumn("Date_Text", date_format("date", "yyyy-MM-dd")) \
        .withColumn("Year", year("date")) \
        .withColumn("Quarter", quarter("date")) \
        .withColumn("Month", month("date")) \
        .withColumn("Month_Name", date_format("date", "MMMM")) \
        .withColumn("Month_Name_Abbrev", date_format("date", "MMM")) \
        .withColumn("Day", dayofmonth("date")) \
        .withColumn("Day_Name", date_format("date", "EEEE")) \
        .withColumn("Day_Of_Week", dayofweek("date")) \
        .withColumn("Week_Of_Year", weekofyear("date")) \
        .withColumn("Is_Weekend", expr("CASE WHEN dayofweek(date) IN (1, 7) THEN TRUE ELSE FALSE END")) \
        .withColumn("Month_Year", concat_ws(", ", date_format("date", "MMM"), year("date"))) \
        .withColumn("Sort_Year", -year("date")) \
        .withColumn("Sort_Quarter", -quarter("date")) \
        .withColumn("Sort_Month", -month("date")) \
        .withColumn("Sort_Day", -date_format("date", "yyyyMMdd").cast("int")) \
        .withColumn("Sort_Day_Of_Week", -dayofweek("date")) \
        .withColumn("Sort_Week_Of_Year", -weekofyear("date")) \
        .withColumn("Sort_Year_Month", (year("date") * 100 + month("date")) * -1)

    def set_all_columns_nullable(schema):
        new_fields = [StructField(field.name, field.dataType, True) for field in schema.fields]
        return StructType(new_fields)

    # Update schema to set all columns as nullable
    updated_schema = set_all_columns_nullable(date_dim_df.schema)

    # Apply the updated schema to the DataFrame
    date_dim_df = spark.createDataFrame(date_dim_df.rdd, schema=updated_schema)

    date_dim_df.write.mode("append").option(Constants.WorkspaceId, metadata_workspace_id).synapsesql(f"{metadata_warehouse_name}.dbo.Date_Dimension")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Deleted notebook, ImportArtifacts_DONT_OPEN_IN_FABRIC, to avoid someone opening it by mistake.")
notebookutils.notebook.delete("ImportArtifacts_DONT_OPEN_IN_FABRIC")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
