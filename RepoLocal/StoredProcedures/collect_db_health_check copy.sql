USE [DBAClient]
GO

-- Create the stored procedure
CREATE OR ALTER PROCEDURE [dbo].[collect_db_health_check]
AS
BEGIN
    -- Variable to hold the linked server name
    DECLARE @LinkedServerName NVARCHAR(128);
    
    -- Cursor to loop through each linked server
    DECLARE linked_server_cursor CURSOR FOR
    SELECT name
    FROM sys.servers
    WHERE is_linked = 1;

    OPEN linked_server_cursor;

    FETCH NEXT FROM linked_server_cursor INTO @LinkedServerName;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Check if the view exists on the linked server
        DECLARE @checkView NVARCHAR(MAX);
        SET @checkView = '
            IF EXISTS (
                SELECT * 
                FROM OPENQUERY([' + @LinkedServerName + '], ''SELECT * FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = ''dbo.DBhealth_check'')
            )
            BEGIN
                INSERT INTO dbo.DBhealth_check (instance_name, database_name, database_size_mb, recovery_model, last_full_backup, last_diff_backup, last_log_backup, AG_Name, Replica_Status, query_store_on, TimeStamp)
                SELECT 
                    ''' + @LinkedServerName + ''' AS instance_name,
                    database_name,
                    database_size_mb,
                    recovery_model,
                    last_full_backup,
                    last_diff_backup,
                    last_log_backup,
                    AG_Name,
                    Replica_Status,
                    query_store_on,
                    TimeStamp
                FROM OPENQUERY([' + @LinkedServerName + '], ''SELECT * FROM dbo.DBhealth_check'');
            END
        ';

        BEGIN TRY
            -- Execute the dynamic SQL
            EXEC sp_executesql @checkView;
        END TRY
        BEGIN CATCH
            PRINT 'Error processing linked server: ' + @LinkedServerName + '. Error message: ' + ERROR_MESSAGE();
        END CATCH;

        -- Fetch the next linked server
        FETCH NEXT FROM linked_server_cursor INTO @LinkedServerName;
    END

    CLOSE linked_server_cursor;
    DEALLOCATE linked_server_cursor;
END
