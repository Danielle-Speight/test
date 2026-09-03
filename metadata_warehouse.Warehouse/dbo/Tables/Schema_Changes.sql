CREATE TABLE [dbo].[Schema_Changes] (

	[Table_ID] int NULL, 
	[Datastore_Name] varchar(255) NULL, 
	[Table_Name] varchar(4000) NULL, 
	[Change_Type] varchar(255) NULL, 
	[Column_Name] varchar(255) NULL, 
	[Data_Type_Details] varchar(255) NULL, 
	[Schema_Arrival_Time] datetime2(6) NULL, 
	[Fabric_Monitor_URL] varchar(4000) NULL, 
	[Date_Key] int NULL
);