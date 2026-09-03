CREATE TABLE [dbo].[Data_Pipeline_Metadata_Orchestration] (

	[Trigger_Name] varchar(4000) NULL, 
	[Order_Of_Operations] int NULL, 
	[Table_ID] int NULL, 
	[Target_Datastore] varchar(4000) NULL, 
	[Target_Entity] varchar(4000) NULL, 
	[Primary_Keys] varchar(4000) NULL, 
	[Processing_Method] varchar(200) NULL, 
	[Ingestion_Active] int NULL
);