CREATE TABLE [dbo].[Data_Quality_Notifications] (

	[Log_ID] varchar(4000) NULL, 
	[Datastore_Name] varchar(255) NULL, 
	[Table_ID] int NULL, 
	[Table_Name] varchar(255) NULL, 
	[Data_Quality_Category] varchar(255) NULL, 
	[Data_Quality_Result] varchar(255) NULL, 
	[Data_Quality_Message] varchar(8000) NULL, 
	[Rows_Impacted] int NULL, 
	[Data_Quarantined] varchar(255) NULL, 
	[Rows_Quarantined] int NULL, 
	[Ingestion_Start_Time] datetime2(6) NULL, 
	[Ingestion_End_Time] datetime2(6) NULL, 
	[Fabric_Monitor_URL] varchar(500) NULL, 
	[Date_Key] int NULL
);