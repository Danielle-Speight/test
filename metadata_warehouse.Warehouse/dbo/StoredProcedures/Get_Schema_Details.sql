CREATE         PROCEDURE [dbo].[Get_Schema_Details]
(
    -- Add the parameters for the stored procedure here
    @table_id int
)
AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON
	 
	-- Use subquery to ensure TOP 1 respects ORDER BY before UNION
	SELECT Schema_ID, Schema_Details, Schema_Arrival_Time
	FROM (
		SELECT TOP 1 Schema_ID, Schema_Details, Schema_Arrival_Time
		FROM [dbo].[Schema_Logs]
		WHERE Table_ID = @table_id
		ORDER BY Schema_Arrival_Time DESC
	) AS LatestSchema
	UNION ALL
	SELECT 'NoData', 'NoData', NULL
	WHERE NOT EXISTS (
		SELECT 1 FROM [dbo].[Schema_Logs] WHERE Table_ID = @table_id
	)

	RETURN

END