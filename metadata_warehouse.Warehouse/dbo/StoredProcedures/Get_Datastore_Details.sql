CREATE     PROCEDURE [dbo].[Get_Datastore_Details]
(
    @datastore_name varchar(100)
)
AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON

    -- Resolve a datastore_name to its full Datastore_Configuration row.
    -- Used by PL_02 pipelines to look up the environment-specific connection details
    -- at runtime, so that metadata SQL stays environment-agnostic (no raw GUIDs).

    IF NOT EXISTS (
        SELECT  1
        FROM    [dbo].[Datastore_Configuration]
        WHERE   [Datastore_Name] = @datastore_name
    )
    BEGIN
        DECLARE @available_datastores varchar(4000)
        SELECT  @available_datastores = STRING_AGG([Datastore_Name], ', ')
        FROM    [dbo].[Datastore_Configuration]

        DECLARE @error_message varchar(8000) = CONCAT(
            'Datastore ''', @datastore_name, ''' not found in Datastore_Configuration. Available datastores: ', @available_datastores
        );
        THROW 50001, @error_message, 1;
    END

    SELECT  [Datastore_Name],
            [Datastore_Type],
            [Datastore_ID],
            [Workspace_ID],
            [Workspace_Name],
            [Medallion_Layer],
            [Endpoint],
            [Connection_ID]
    FROM    [dbo].[Datastore_Configuration]
    WHERE   [Datastore_Name] = @datastore_name
END