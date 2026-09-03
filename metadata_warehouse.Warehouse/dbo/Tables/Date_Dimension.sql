CREATE TABLE [dbo].[Date_Dimension] (

	[Date_Key] int NULL, 
	[Date] date NULL, 
	[Date_Text] varchar(255) NULL, 
	[Year] int NULL, 
	[Quarter] int NULL, 
	[Month] int NULL, 
	[Month_Name] varchar(255) NULL, 
	[Month_Name_Abbrev] varchar(255) NULL, 
	[Day] int NULL, 
	[Day_Name] varchar(255) NULL, 
	[Day_Of_Week] int NULL, 
	[Week_Of_Year] int NULL, 
	[Is_Weekend] bit NULL, 
	[Month_Year] varchar(255) NULL, 
	[Sort_Year] int NULL, 
	[Sort_Quarter] int NULL, 
	[Sort_Month] int NULL, 
	[Sort_Day] int NULL, 
	[Sort_Day_Of_Week] int NULL, 
	[Sort_Week_Of_Year] int NULL, 
	[Sort_Year_Month] int NULL
);