-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
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
-- Metadata Configuration: Blackboard Ingestion - Bronze Layers
-- Trigger Name: Blackboard_Ingestion
-- Generated: 2020-04-16
-- Author: Data Engineering Team
-- =====================================================================
--
-- Purpose:
--   Configure data pipelines for Cardiff University Blackboard student data
--   ingestion from Blackboard Snowflake database to Microsoft Fabric Lakehouse (Bronze layer)
--
-- Source: Blackboard database (6 source tables)
-- Target: bronze.Blackboard.*
--
-- Table_ID Scheme:
--   Bronze: 101-106 (6 Blackboard source tables)
--TRUNCATE TABLE [metadata_warehouse].[dbo].[Data_Pipeline_Logs]
--TRUNCATE TABLE [metadata_warehouse].[dbo].[Data_Pipeline_Metadata_Advanced_Configuration]
--TRUNCATE TABLE [metadata_warehouse].[dbo].[Data_Pipeline_Metadata_Orchestration]
--TRUNCATE TABLE [metadata_warehouse].[dbo].[Data_Pipeline_Metadata_Primary_Configuration]
--
-- Execution Order (Optimized for F16 SKU - 4 Orders, down from 8):
--   Order 1: Bronze small/medium (<10K rows) - 8 tables in parallel
--   Order 2: Bronze large (395K-9.3M rows) - 6 tables in parallel--

-- Loading Strategy:
--   Bronze: Full extract from Blackboard → MERGE on PK → CDF enabled

-- Prerequisites:
--   - Blackboard snowflake connection configured with connection_id GUID
--   - bronze lakehouses created with CDM_LMS schemas
--
-- =====================================================================

-- =============================================================================
-- SECTION 0: IDEMPOTENT DELETE BLOCK
-- =============================================================================
DELETE FROM dbo.Data_Pipeline_Metadata_Advanced_Configuration
WHERE Table_ID IN (
    SELECT Table_ID
    FROM dbo.Data_Pipeline_Metadata_Orchestration
    WHERE Trigger_Name = 'CDM_LMS_Blackboard_Ingestion'
)

DELETE FROM dbo.Data_Pipeline_Metadata_Primary_Configuration
WHERE Table_ID IN (
    SELECT Table_ID
    FROM dbo.Data_Pipeline_Metadata_Orchestration
    WHERE Trigger_Name = 'CDM_LMS_Blackboard_Ingestion'
)

DELETE FROM dbo.Data_Pipeline_Metadata_Orchestration
WHERE Trigger_Name = 'CDM_LMS_Blackboard_Ingestion'

-- =============================================================================
-- SECTION 1: ORCHESTRATION METADATA (ALL TABLE_IDS)
-- =============================================================================
-- Optimized for F16 Fabric SKU: 4 Orders (down from 8)
--
-- Bronze Layer: 101-114 (6 Blackboard source replicas) - 2 groups
--   Order 1: Small/medium tables (<10K rows) - 8 tables (parallel, fast finish)
--   Order 2: Large tables (395K-9.3M rows) - 6 tables (full F16 capacity)

-- =============================================================================
INSERT INTO dbo.Data_Pipeline_Metadata_Orchestration ([Trigger_Name],[Order_Of_Operations],[Table_ID],[Target_Datastore],[Target_Entity],[Primary_Keys],[Processing_Method],[Ingestion_Active])
VALUES
-- =====================================================================
-- BRONZE ORDER 1: Small/medium reference tables - 11 tables, <10K rows each
-- =====================================================================
('CDM_LMS_Blackboard_Ingestion', 1, 101, 'LearningPlatforms_Bronze', 'blackboard.term', 'id', 'batch_with_staging', 1),
('CDM_LMS_Blackboard_Ingestion', 1, 102, 'LearningPlatforms_Bronze', 'blackboard.person_course', 'id', 'batch_with_staging', 1),
('CDM_LMS_Blackboard_Ingestion', 1, 103, 'LearningPlatforms_Bronze', 'blackboard.institution_hierarchy', 'id', 'batch_with_staging', 1),
('CDM_LMS_Blackboard_Ingestion', 1, 104, 'LearningPlatforms_Bronze', 'blackboard.institution_hierarchy_course', 'id', 'batch_with_staging', 1),
('CDM_LMS_Blackboard_Ingestion', 1, 105, 'LearningPlatforms_Bronze', 'blackboard.course', 'id', 'batch_with_staging', 1),
('CDM_LMS_Blackboard_Ingestion', 1, 106, 'LearningPlatforms_Bronze', 'blackboard.person', 'id', 'batch_with_staging', 1),
('CDM_LMS_Blackboard_Ingestion', 1, 107, 'LearningPlatforms_Bronze', 'blackboard.gradebook', 'id', 'batch_with_staging', 1),
('CDM_LMS_Blackboard_Ingestion', 1, 108, 'LearningPlatforms_Bronze', 'blackboard.course_item', 'id', 'batch_with_staging', 1),
('CDM_LMS_Blackboard_Ingestion', 1, 109, 'LearningPlatforms_Bronze', 'blackboard.evaluable_item', 'id', 'batch_with_staging', 1),
('CDM_LMS_Blackboard_Ingestion', 1, 110, 'LearningPlatforms_Bronze', 'blackboard.grade', 'id', 'batch_with_staging', 1),
('CDM_LMS_Blackboard_Ingestion', 1, 111, 'LearningPlatforms_Bronze', 'blackboard.course_activity', 'id', 'batch_with_staging', 1)





-- =============================================================================
-- SECTION 2: PRIMARY CONFIGURATION (ALL TABLE_IDS)
-- =============================================================================
INSERT INTO [dbo].[Data_Pipeline_Metadata_Primary_Configuration] ([Table_ID],[Configuration_Category],[Configuration_Name],[Configuration_Value])
VALUES
-- =====================================================================
-- BRONZE LAYER CONFIGURATIONS (101-111) - 11 Oracle Source Tables
-- Full extract from Oracle, MERGE to Bronze, CDF enabled for Silver incremental
-- NOTE: Bronze layer just copies data as-is from Blackboard (no transformations)
-- =====================================================================

(101, 'source_details', 'source', 'snowflake'),
(101, 'source_details', 'datastore_name', 'Blackboard - Prod'),
(101, 'source_details', 'schema_name', 'Blackboards'),
(101, 'source_details', 'table_name', 'cdm_lms.term'),
(101, 'WatermarkDetails', 'column_name', 'MODIFIED_TIME'),
(101, 'WatermarkDetails', 'DataType', 'Datetime'),
(101, 'source_details', 'query', 'SELECT * FROM cdm_lms.term WHERE MODIFIED_TIME > ''{WATERMARKVALUE}'''),
(101, 'source_details', 'staging_lakehouse_name', 'LearningPlatforms_Bronze'),
(101, 'source_details', 'staging_folder_path', 'Blackboards/CDM_LMS/term/'),
(101, 'target_details', 'merge_type', 'append'),
(101, 'target_details', 'enable_change_data_feed', 'true'),

(102, 'source_details', 'source', 'snowflake'),
(102, 'source_details', 'datastore_name', 'Blackboard - Prod'),
(102, 'source_details', 'schema_name', 'Blackboards'),
(102, 'source_details', 'table_name', 'cdm_lms.person_course'),
(102, 'WatermarkDetails', 'column_name', 'MODIFIED_TIME'),
(102, 'WatermarkDetails', 'DataType', 'Datetime'),
(102, 'source_details', 'query', 'SELECT * FROM cdm_lms.person_course WHERE MODIFIED_TIME > ''{WATERMARKVALUE}'''),
(102, 'source_details', 'staging_lakehouse_name', 'LearningPlatforms_Bronze'),
(102, 'source_details', 'staging_folder_path', 'Blackboards/CDM_LMS/term/'),
(102, 'target_details', 'merge_type', 'append'),
(102, 'target_details', 'enable_change_data_feed', 'true'),

(103, 'source_details', 'source', 'snowflake'),
(103, 'source_details', 'datastore_name', 'Blackboard - Prod'),
(103, 'source_details', 'schema_name', 'Blackboards'),
(103, 'source_details', 'table_name', 'cdm_lms.institution_hierarchy'),
(103, 'WatermarkDetails', 'column_name', 'ROW_UPDATED_TIME'),
(103, 'WatermarkDetails', 'DataType', 'Datetime'),
(103, 'source_details', 'query', 'SELECT * FROM cdm_lms.institution_hierarchy WHERE ROW_UPDATED_TIME > ''{WATERMARKVALUE}'''),
(103, 'source_details', 'staging_lakehouse_name', 'LearningPlatforms_Bronze'),
(103, 'source_details', 'staging_folder_path', 'Blackboards/CDM_LMS/institution_hierarchy/'),
(103, 'target_details', 'merge_type', 'append'),
(103, 'target_details', 'enable_change_data_feed', 'true'),

(104, 'source_details', 'source', 'snowflake'),
(104, 'source_details', 'datastore_name', 'Blackboard - Prod'),
(104, 'source_details', 'schema_name', 'Blackboards'),
(104, 'source_details', 'table_name', 'cdm_lms.institution_hierarchy_course'),
(104, 'WatermarkDetails', 'column_name', 'ROW_UPDATED_TIME'),
(104, 'WatermarkDetails', 'DataType', 'Datetime'),
(104, 'source_details', 'query', 'SELECT * FROM cdm_lms.institution_hierarchy_course WHERE ROW_UPDATED_TIME > ''{WATERMARKVALUE}'''),
(104, 'source_details', 'staging_lakehouse_name', 'LearningPlatforms_Bronze'),
(104, 'source_details', 'staging_folder_path', 'Blackboards/CDM_LMS/institution_hierarchy_course/'),
(104, 'target_details', 'merge_type', 'append'),
(104, 'target_details', 'enable_change_data_feed', 'true'),

(105, 'source_details', 'source', 'snowflake'),
(105, 'source_details', 'datastore_name', 'Blackboard - Prod'),
(105, 'source_details', 'schema_name', 'Blackboards'),
(105, 'source_details', 'table_name', 'cdm_lms.course'),
(105, 'WatermarkDetails', 'column_name', 'modified_time'),
(105, 'WatermarkDetails', 'DataType', 'Datetime'),
(105, 'source_details', 'query', 'SELECT *, Stage:service_level AS service_level FROM cdm_lms.course WHERE modified_time > ''{WATERMARKVALUE}'''),
(105, 'source_details', 'staging_lakehouse_name', 'LearningPlatforms_Bronze'),
(105, 'source_details', 'staging_folder_path', 'Blackboards/CDM_LMS/course/'),
(105, 'target_details', 'merge_type', 'append'),
(105, 'target_details', 'enable_change_data_feed', 'true'),
(105, 'target_details', 'if_duplicate_primary_keys', 'warn'), 

(106, 'source_details', 'source', 'snowflake'),
(106, 'source_details', 'datastore_name', 'Blackboard - Prod'),
(106, 'source_details', 'schema_name', 'Blackboards'),
(106, 'source_details', 'table_name', 'cdm_lms.person'),
(106, 'WatermarkDetails', 'column_name', 'modified_time'),
(106, 'WatermarkDetails', 'DataType', 'Datetime'),
(106, 'source_details', 'query', 'SELECT * FROM cdm_lms.person WHERE modified_time > ''{WATERMARKVALUE}'''),
(106, 'source_details', 'staging_lakehouse_name', 'LearningPlatforms_Bronze'),
(106, 'source_details', 'staging_folder_path', 'Blackboards/CDM_LMS/person/'),
(106, 'target_details', 'merge_type', 'append'),
(106, 'target_details', 'enable_change_data_feed', 'true'),
(106, 'target_details', 'if_duplicate_primary_keys', 'warn'), 


(107, 'source_details', 'source', 'snowflake'),
(107, 'source_details', 'datastore_name', 'Blackboard - Prod'),
(107, 'source_details', 'schema_name', 'Blackboards'),
(107, 'source_details', 'table_name', 'cdm_lms.gradebook'),
(107, 'WatermarkDetails', 'column_name', 'modified_time'),
(107, 'WatermarkDetails', 'DataType', 'Datetime'),
(107, 'source_details', 'query', 'SELECT ID, INSTANCE_ID, COURSE_ID, COURSE_ITEM_ID, SOURCE_ID, NAME, GRADEBOOK_TYPE, ALLOWED_ATTEMPTS_CNT, POSSIBLE_SCORE, AGGREGATION_MODEL, AGGREGATION_MODEL_SOURCE_CODE, AGGREGATION_MODEL_SOURCE_DESC, CALCULATION_TYPE, CALCULATION_TYPE_SOURCE_CODE, CALCULATION_TYPE_SOURCE_DESC, DELETED_IND, FINAL_GRADE_IND, MULTIPLE_ATTEMPTS_IND, USED_IN_CALCULATIONS_IND, VISIBLE_IND, STAGE, ROW_INSERTED_TIME, ROW_UPDATED_TIME, ROW_DELETED_TIME, MODIFIED_TIME, CREATED_TIME, DUE_TIME, GRADES_RELEASED_IND FROM cdm_lms.gradebook WHERE ROW_UPDATED_TIME > TIMESTAMP ''{WATERMARKVALUE}'''),
(107, 'source_details', 'staging_lakehouse_name', 'LearningPlatforms_Bronze'),
(107, 'source_details', 'staging_folder_path', 'Blackboards/CDM_LMS/gradebook/'),
(107, 'target_details', 'merge_type', 'append'),
(107, 'target_details', 'enable_change_data_feed', 'true'),
(107, 'target_details', 'if_duplicate_primary_keys', 'warn'), 



(108, 'source_details', 'source', 'snowflake'),
(108, 'source_details', 'datastore_name', 'Blackboard - Prod'),
(108, 'source_details', 'schema_name', 'Blackboards'),
(108, 'source_details', 'table_name', 'cdm_lms.course_item'),
(108, 'WatermarkDetails', 'column_name', 'modified_time'),
(108, 'WatermarkDetails', 'DataType', 'Datetime'),
(108, 'source_details', 'query', 'SELECT * FROM cdm_lms.course_item WHERE ROW_UPDATED_TIME > TIMESTAMP ''{WATERMARKVALUE}'''),
(108, 'source_details', 'staging_lakehouse_name', 'LearningPlatforms_Bronze'),
(108, 'source_details', 'staging_folder_path', 'Blackboards/CDM_LMS/course_item/'),
(108, 'target_details', 'merge_type', 'append'),
(108, 'target_details', 'enable_change_data_feed', 'true'),
(108, 'target_details', 'if_duplicate_primary_keys', 'warn'), 


(109, 'source_details', 'source', 'snowflake'),
(109, 'source_details', 'datastore_name', 'Blackboard - Prod'),
(109, 'source_details', 'schema_name', 'Blackboards'),
(109, 'source_details', 'table_name', 'cdm_lms.evaluable_item'),
(109, 'WatermarkDetails', 'column_name', 'modified_time'),
(109, 'WatermarkDetails', 'DataType', 'Datetime'),
(109, 'source_details', 'query', 'SELECT * FROM cdm_lms.evaluable_item WHERE ROW_UPDATED_TIME > TIMESTAMP ''{WATERMARKVALUE}'''),
(109, 'source_details', 'staging_lakehouse_name', 'LearningPlatforms_Bronze'),
(109, 'source_details', 'staging_folder_path', 'Blackboards/CDM_LMS/evaluable_item/'),
(109, 'target_details', 'merge_type', 'append'),
(109, 'target_details', 'enable_change_data_feed', 'true'),
(109, 'target_details', 'if_duplicate_primary_keys', 'warn'), 


(110, 'source_details', 'source', 'snowflake'),
(110, 'source_details', 'datastore_name', 'Blackboard - Prod'),
(110, 'source_details', 'schema_name', 'Blackboards'),
(110, 'source_details', 'table_name', 'cdm_lms.grade'),
(110, 'WatermarkDetails', 'column_name', 'modified_time'),
(110, 'WatermarkDetails', 'DataType', 'Datetime'),
(110, 'source_details', 'query', 'SELECT * FROM cdm_lms.grade WHERE ROW_UPDATED_TIME > TIMESTAMP ''{WATERMARKVALUE}'''),
(110, 'source_details', 'staging_lakehouse_name', 'LearningPlatforms_Bronze'),
(110, 'source_details', 'staging_folder_path', 'Blackboards/CDM_LMS/grade/'),
(110, 'target_details', 'merge_type', 'append'),
(110, 'target_details', 'enable_change_data_feed', 'true'),
(110, 'target_details', 'if_duplicate_primary_keys', 'warn'), 



(111, 'source_details', 'source', 'snowflake'),
(111, 'source_details', 'datastore_name', 'Blackboard - Prod'),
(111, 'source_details', 'schema_name', 'Blackboards'),
(111, 'source_details', 'table_name', 'cdm_lms.course_activity'),
(111, 'WatermarkDetails', 'column_name', 'ROW_UPDATED_TIME'),
(111, 'WatermarkDetails', 'DataType', 'Datetime'),
(111, 'source_details', 'query', 'SELECT * FROM cdm_lms.course_activity WHERE ROW_UPDATED_TIME > TIMESTAMP ''{WATERMARKVALUE}'''),
(111, 'source_details', 'staging_lakehouse_name', 'LearningPlatforms_Bronze'),
(111, 'source_details', 'staging_folder_path', 'Blackboards/CDM_LMS/course_activity/'),
(111, 'target_details', 'merge_type', 'append'),
(111, 'target_details', 'enable_change_data_feed', 'true'),
(111, 'target_details', 'if_duplicate_primary_keys', 'warn')



-- =============================================================================
-- DATA FLOW SUMMARY (Medallion Architecture)
-- =============================================================================
-- 
-- BRONZE (Oracle → Fabric):
--   • Full extract from Oracle (no watermark - same as SSIS)
--   • MERGE on primary keys (only writes actual changes)
--   • CDF enabled → tracks INSERT/UPDATE/DELETE for Silver
--   • NO transformations - raw data copy with original column names
--
--
-- =============================================================================

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
