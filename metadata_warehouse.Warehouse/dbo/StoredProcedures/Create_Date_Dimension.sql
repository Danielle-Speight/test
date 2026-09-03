CREATE     PROCEDURE [dbo].[Create_Date_Dimension]
AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON
    
    IF NOT EXISTS (SELECT 1 FROM dbo.Date_Dimension)
    BEGIN
    
		DECLARE @StartDate DATE = '2025-03-26';
		DECLARE @EndDate DATE = '2035-12-31';
		
		WHILE @StartDate <= @EndDate
		BEGIN
			INSERT INTO dbo.Date_Dimension
			VALUES (
				CONVERT(INT, CONVERT(VARCHAR, @StartDate, 112)),
				@StartDate,
				CONVERT(VARCHAR, @StartDate, 23),
				YEAR(@StartDate),
				DATEPART(QUARTER, @StartDate),
				MONTH(@StartDate),
				DATENAME(month, @StartDate),
				LEFT(DATENAME(month, @StartDate),3),
				DAY(@StartDate),
				DATENAME(weekday, @StartDate),
				DATEPART(WEEKDAY, @StartDate),
				DATEPART(WEEK, @StartDate),
				CASE WHEN DATEPART(WEEKDAY, @StartDate) IN (1, 7) THEN 1 ELSE 0 END,
				CONCAT(LEFT(DATENAME(month, @StartDate),3), ', ', YEAR(@StartDate)),
				YEAR(@StartDate)*-1,
				DATEPART(QUARTER, @StartDate)*-1,
				MONTH(@StartDate)*-1,
				CONVERT(INT, CONVERT(VARCHAR, @StartDate, 112))*-1,
				DATEPART(WEEKDAY, @StartDate)*-1,
				DATEPART(WEEK, @StartDate)*-1,
				CAST(CONCAT(YEAR(@StartDate), Month(@StartDate)) AS INT) * -1 
			);
		
			SET @StartDate = DATEADD(DAY, 1, @StartDate);
		END;

    END

END