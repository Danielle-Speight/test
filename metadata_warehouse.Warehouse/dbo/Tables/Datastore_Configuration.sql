CREATE TABLE [dbo].[Datastore_Configuration] (

	[Datastore_Name] varchar(100) NOT NULL, 
	[Datastore_Type] varchar(50) NOT NULL, 
	[Datastore_ID] varchar(100) NULL, 
	[Workspace_ID] varchar(100) NULL, 
	[Workspace_Name] varchar(500) NULL, 
	[Medallion_Layer] varchar(50) NULL, 
	[Endpoint] varchar(500) NULL, 
	[Connection_ID] varchar(100) NULL
);