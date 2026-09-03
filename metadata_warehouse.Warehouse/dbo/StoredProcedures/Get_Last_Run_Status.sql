CREATE     PROCEDURE [dbo].[Get_Last_Run_Status]
(
    @table_id int
)
AS
BEGIN
    SET NOCOUNT ON

    SELECT      COALESCE(MIN(Ingestion_Status), 'Failed') AS Last_Ingestion_Status
    FROM        [dbo].[Data_Pipeline_Logs]
    WHERE       Trigger_Execution_Start_Time = (
                    SELECT      MAX(Trigger_Execution_Start_Time)
                    FROM        [dbo].[Data_Pipeline_Logs]
                    WHERE       Table_ID = @table_id
                )
    AND         Table_ID = @table_id
    AND         Processing_Phase = 'Batch'

END