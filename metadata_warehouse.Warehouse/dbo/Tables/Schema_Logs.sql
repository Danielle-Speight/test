CREATE TABLE [dbo].[Schema_Logs] (

	[Table_ID] int NULL, 
	[Datastore_Name] varchar(255) NULL, 
	[Table_Name] varchar(4000) NULL, 
	[Schema_ID] varchar(4000) NULL, 
	[Schema_Details] varchar(max) NULL, 
	[Schema_Arrival_Time] datetime2(6) NULL, 
	[Fabric_Monitor_URL] varchar(4000) NULL, 
	[Date_Key] int NULL
);