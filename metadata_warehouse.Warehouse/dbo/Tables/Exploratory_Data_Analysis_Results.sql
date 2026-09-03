CREATE TABLE [dbo].[Exploratory_Data_Analysis_Results] (

	[Table_ID] int NULL, 
	[Datastore_Name] varchar(255) NULL, 
	[Target_Type] varchar(100) NULL, 
	[Target_Medallion_Layer] varchar(50) NULL, 
	[Table_Name] varchar(4000) NULL, 
	[Table_Last_Modified_Time] datetime2(6) NULL, 
	[Column_Name] varchar(255) NULL, 
	[Data_Type] varchar(255) NULL, 
	[Total_Rows] int NULL, 
	[Total_Columns] int NULL, 
	[Approx_Distinct_Values] int NULL, 
	[Null_Count] int NULL, 
	[Null_Percent] decimal(20,4) NULL, 
	[Mean] decimal(20,4) NULL, 
	[Std_Dev] decimal(20,4) NULL, 
	[Min] varchar(255) NULL, 
	[Max] varchar(255) NULL, 
	[Data_Profile_Execution_Time] datetime2(6) NOT NULL, 
	[Date_Key] int NULL
);