-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "sqldatawarehouse"
-- META   },
-- META   "dependencies": {
-- META     "warehouse": {
-- META       "default_warehouse": "473c5949-7c50-a434-4bbb-62c22a8bb212",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "473c5949-7c50-a434-4bbb-62c22a8bb212",
-- META           "type": "Datawarehouse"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

-- =====================================================================
-- Datastore Configuration: DEV Environment
-- Environment: DEV
-- Generated: 2026-02-12
-- =====================================================================
-- Purpose:
--   Register all Fabric datastores (Lakehouses, Warehouses) for the DEV environment.
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

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

-- =====================================================================
-- STEP 1: Clear existing datastore configuration (full refresh)
-- =====================================================================
TRUNCATE TABLE dbo.Datastore_Configuration;

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

-- =====================================================================
-- STEP 2: Insert DEV environment datastore configuration
-- =====================================================================


INSERT INTO [dbo].[Datastore_Configuration] 
    (Datastore_Name, Datastore_Type, Datastore_ID, Workspace_ID, Workspace_Name, Medallion_Layer, Endpoint, Connection_ID)
VALUES
-- Core Medallion Architecture Datastores
('LearningPlatforms_Bronze', 'Lakehouse', 'a2201eb1-3966-489f-acfc-fd0147d4b2e6', '0f45f4f1-888e-4db4-9132-46179563e4e4', 'Ingest_Dev_LearningPlatforms', 'Bronze', NULL, NULL),
('metadata_lakehouse', 'Lakehouse', '23e58db4-bc94-4fea-ac48-0686234ae11d', '0f45f4f1-888e-4db4-9132-46179563e4e4', 'Ingest_Dev_LearningPlatforms', NULL, NULL, NULL),
('metadata_warehouse', 'Warehouse', '2a8bb212-62c2-4bbb-a434-7c50473c5949', '0f45f4f1-888e-4db4-9132-46179563e4e4', 'Ingest_Dev_LearningPlatforms', 'Gold', 'gbf3pplisvlerpn7az2zo6h4xq-6h2ekd4orc2e3ejsiylzky7e4q.datawarehouse.fabric.microsoft.com', NULL),
('Blackboard - Prod', 'ExternalDatabase', 'N/A', '0f45f4f1-888e-4db4-9132-46179563e4e4', 'Ingest_Dev_LearningPlatforms', NULL, NULL, '41339987-9b8d-4d15-8ff0-adc8bbdbdd5f')

-- =====================================================================
-- Add additional datastores below as needed:
-- =====================================================================
-- Example: Adding a new domain-specific lakehouse
-- INSERT INTO [dbo].[Datastore_Configuration] 
--     (Datastore_Name, Datastore_Type, Datastore_ID, Workspace_ID, Workspace_Name, Medallion_Layer, Endpoint, Connection_ID)
-- VALUES
-- ('sales_bronze', 'Lakehouse', '<lakehouse-guid>', '<workspace-guid>', 'dev', 'Bronze', NULL, NULL);

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }
