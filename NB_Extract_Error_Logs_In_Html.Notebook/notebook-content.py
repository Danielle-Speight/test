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

# CELL ********************

import pandas as pd
import requests
from datetime import date
import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

data_movement_failures = "[]"
data_quality_notifications = "[]"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 1: Convert JSON string to Python dict
data_movement_failures = json.loads(data_movement_failures)

# Step 2: Convert dict to Pandas DataFrame
data_movement_failures = pd.DataFrame(data_movement_failures)

# Step 1: Convert JSON string to Python dict
data_quality_notifications = json.loads(data_quality_notifications)

# Step 2: Convert dict to Pandas DataFrame
data_quality_notifications = pd.DataFrame(data_quality_notifications)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data_movement_failures_html_table = data_movement_failures.to_html(index=False, border=1, classes='dataframe')
data_movement_failures_html_template = f"""
<html>
<head>
    <style>
        body {{
            font-family: Arial, sans-serif;
        }}
        table {{
            border-collapse: collapse;
            width: 100%;
            font-size: 14px;
        }}
        th {{
            border: 1px solid #dddddd;
            background-color: #f2f2f2;
            font-weight: bold;
            padding: 8px;
            text-align: left;
            font-size: 16px;
        }}
        td {{
            border: 1px solid #dddddd;
            padding: 8px;
            text-align: left;
            font-size: 13px;
        }}
        h2 {{
            font-size: 18px;
        }}
    </style>
</head>
<body>
    <h2> Errors in Latest Data Load</h2>
    {data_movement_failures_html_table}
</body>
</html>
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data_quality_notifications_html_table = data_quality_notifications.to_html(index=False, border=1, classes='dataframe')
data_quality_notifications_html_template = f"""
<html>
<head>
    <style>
        body {{
            font-family: Arial, sans-serif;
        }}
        table {{
            border-collapse: collapse;
            width: 100%;
            font-size: 14px;
        }}
        th {{
            border: 1px solid #dddddd;
            background-color: #f2f2f2;
            font-weight: bold;
            padding: 8px;
            text-align: left;
            font-size: 16px;
        }}
        td {{
            border: 1px solid #dddddd;
            padding: 8px;
            text-align: left;
            font-size: 13px;
        }}
        h2 {{
            font-size: 18px;
        }}
    </style>
</head>
<body>
    <h2> Data Quality Notifications in Latest Data Load</h2>
    {data_quality_notifications_html_table}
</body>
</html>
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if len(data_movement_failures) == 0:
    data_movement_failures_html_template = ""
    
if len(data_quality_notifications) == 0:
    data_quality_notifications_html_template = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.notebook.exit({'data_quality_notifications_html_template': data_quality_notifications_html_template, 'data_movement_failures_html_template': data_movement_failures_html_template})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
