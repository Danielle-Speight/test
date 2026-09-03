CREATE     PROCEDURE [dbo].[Get_Pipeline_Tables]
(
    @trigger_name varchar(255),
    @start_with_step int = 0,
    @table_ids varchar(4000) = '0'
)
AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON

    -- Returns the tables to process for a given trigger, grouped by Order_Of_Operations.
    -- Each row represents one processing step containing a comma-separated list of
    -- table details in the format: 'Table_ID:Processing_Method:Target_Datastore:Target_Entity:Source_Datastore'
    -- Source_Datastore is included from Primary Configuration if it exists, otherwise empty.

    SELECT      o.[Trigger_Name],
                o.[Order_Of_Operations],
                STRING_AGG(
                    CONCAT(
                        '''',
                        o.[Table_ID], ':',
                        o.[Processing_Method], ':',
                        o.[Target_Datastore], ':',
                        o.[Target_Entity], ':',
                        ISNULL(pc.[Configuration_Value], ''),
                        ''''
                    ),
                    ','
                ) AS [Pipeline_Table_Details]
    FROM        [dbo].[Data_Pipeline_Metadata_Orchestration] o
    LEFT JOIN   [dbo].[Data_Pipeline_Metadata_Primary_Configuration] pc
        ON      pc.[Table_ID] = o.[Table_ID]
        AND     pc.[Configuration_Category] = 'source_details'
        AND     pc.[Configuration_Name] = 'datastore_name'
    WHERE       o.[Trigger_Name] = @trigger_name
    AND         o.[Order_Of_Operations] >= @start_with_step
    AND         (o.[Table_ID] IN (SELECT CAST(TRIM(value) AS int) FROM STRING_SPLIT(@table_ids, ','))
    OR          @table_ids = '0')
    AND         o.[Ingestion_Active] = 1
    GROUP BY    o.[Trigger_Name], o.[Order_Of_Operations]
    ORDER BY    o.[Order_Of_Operations]
END