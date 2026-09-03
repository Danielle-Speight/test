CREATE     PROCEDURE [dbo].[Get_Watermark_Value]
(
    -- Add the parameters for the stored procedure here
    @table_id int = null,
	@target_datastore varchar(255) = null,
	@processing_phase varchar(50) = 'Batch'
)
AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON

    -- Insert statements for procedure here
    DECLARE @watermark_value varchar(255)

	-- Determine number of records in logging table for control table record id
	DECLARE @matchedrecords int

	SET @matchedrecords = (
		SELECT	COUNT(*)
		FROM	[dbo].[Data_Pipeline_Logs]
		WHERE	[Table_ID] = @table_id
		AND 	[Target_Datastore] = @target_datastore
		AND		[Processing_Phase] = @processing_phase
		AND		[Ingestion_Status] = 'Processed'
	)

	--Determine data type of watermark field
	DECLARE @watermark_field_data_type varchar(255)
	
	SET @watermark_field_data_type = (
		SELECT	[Configuration_Value]
		FROM	[dbo].[Data_Pipeline_Metadata_Primary_Configuration]
		WHERE	[Table_ID] = @table_id
		AND		[Configuration_Category] = 'WatermarkDetails'
        AND		[Configuration_Name] = 'DataType'
	)

	--default watermark value is datetime
	IF @watermark_field_data_type IS NULL
	BEGIN
		SET @watermark_field_data_type = 'Datetime'
	END


	--If no matches records, set watermark value to lowest value for dataset type
	IF @matchedrecords = 0
	BEGIN
		IF @watermark_field_data_type = 'Datetime'
		BEGIN
			SET @watermark_value = '1900-01-01 00:00:00'
		END
		ELSE
		BEGIN
			SET @watermark_value = '-1'
		END

		IF @watermark_field_data_type IS NULL
		BEGIN
			SET @watermark_value = ''
		END
	END

	--Otherwise, set it to latest watermark value
	IF @matchedrecords > 0
	BEGIN
		DECLARE @max_watermark_value varchar(255)
		IF @watermark_field_data_type = 'Datetime'
		BEGIN
			SET @max_watermark_value = (
				SELECT		TOP 1 CASE
									WHEN [Watermark_Value] = 'FULL_RELOAD' 
										THEN '1900-01-01 14:44:44' 
									-- required when data is moved from bronze_files as watermark is integer value of folderpath
									WHEN ISNUMERIC([Watermark_Value]) = 1
                                        THEN [Watermark_Value]
									-- sql server throws an error on datetime 6 string values on putting date in single quotes
									WHEN @processing_phase = 'Staging'
                                        THEN CAST(CAST([Watermark_Value] AS DATETIME2(3)) AS varchar(255))
									ELSE CAST(CAST([Watermark_Value] AS DATETIME2(6)) AS varchar(255))
								  END
				FROM		[dbo].[Data_Pipeline_Logs]
				WHERE		[Table_ID] = @table_id
				AND 		[Target_Datastore] = @target_datastore
				AND			[Processing_Phase] = @processing_phase
				AND			[Ingestion_Status] = 'Processed'
				ORDER BY	[Ingestion_End_Time] DESC
			)
		END
		ELSE
		BEGIN
			SET @max_watermark_value = (
				SELECT		TOP 1 CASE
									WHEN [Watermark_Value] = 'FULL_RELOAD' 
										THEN '-2'
									ELSE [Watermark_Value]
								  END
				FROM		[dbo].[Data_Pipeline_Logs]
				WHERE		[Table_ID] = @table_id
				AND 		[Target_Datastore] = @target_datastore
				AND			[Processing_Phase] = @processing_phase
				AND			[Ingestion_Status] = 'Processed'
				ORDER BY	[Ingestion_End_Time] DESC
			)
		END

		SET @watermark_value = @max_watermark_value
	END

	DECLARE @full_reload varchar(255)
	IF @watermark_value = '1900-01-01 14:44:44' or @watermark_value = '-2'
	BEGIN
		SET @full_reload = 'Yes'
	END
	
	-- if watermark value empty or null, set @watermark_value to default
	IF @watermark_value = '' or @watermark_value IS NULL
	BEGIN
		IF @watermark_field_data_type = 'Datetime'
		BEGIN
			SET @watermark_value = '1900-01-01 00:00:00'
		END
		ELSE
		BEGIN
			SET @watermark_value = '-1'
		END

		IF @watermark_field_data_type IS NULL
		BEGIN
			SET @watermark_value = ''
		END
	END

	--return values for downstream processing
	SELECT @watermark_value [watermark_value], @matchedrecords [matched_records], @full_reload [full_reload]
    RETURN

END