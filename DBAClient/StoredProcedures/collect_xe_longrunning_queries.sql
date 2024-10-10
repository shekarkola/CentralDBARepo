USE [DBAClient]
GO

-- DROP PROCEDURE [InsertRegressedQueries]
-- GO 

CREATE OR ALTER PROCEDURE [dbo].collect_xe_logrunning_queries
AS
BEGIN
    SET NOCOUNT ON;

    -- Drop the temporary table if it exists
    IF OBJECT_ID('tempdb..#capture_waits_data') IS NOT NULL
        DROP TABLE #capture_waits_data;

    -- Create the temporary table and populate data
    SELECT CAST(target_data AS XML) AS targetdata
    INTO #capture_waits_data
    FROM sys.dm_xe_session_targets xet
    JOIN sys.dm_xe_sessions xes ON xes.address = xet.event_session_address
    WHERE xes.name = 'Long_Running_queries'
        AND xet.target_name = 'ring_buffer';

    -- Insert data into RegressedQueries table
    INSERT INTO [DBAClient].dbo.[RegressedQueries] (ServerName, Name, Package, [Timestamp], CPU_Time, Duration, physical_reads, batch_text, [Statement])
    SELECT
        @@SERVERNAME AS ServerName,
        targetdata.value('(RingBufferTarget/event/@name)[1]', 'varchar(50)') AS Name,
        targetdata.value('(RingBufferTarget/event/@package)[1]', 'varchar(50)') AS Package,
        targetdata.value('(RingBufferTarget/event/@timestamp)[1]', 'datetime') AS [Timestamp],
        targetdata.value('(RingBufferTarget/event/data[@name="cpu_time"]/value)[1]', 'bigint') AS CPU_Time,
        targetdata.value('(RingBufferTarget/event/data[@name="duration"]/value)[1]', 'bigint') AS Duration,
        targetdata.value('(RingBufferTarget/event/data[@name="physical_reads"]/value)[1]', 'bigint') AS Physical_Reads,
        targetdata.value('(RingBufferTarget/event/data[@name="batch_text"]/value)[1]', 'nvarchar(150)') AS batch_text,
        targetdata.value('(RingBufferTarget/event/data[@name="Statement"]/value)[1]', 'nvarchar(150)') AS [Statement]
    FROM
        #capture_waits_data
    WHERE
        targetdata.value('(RingBufferTarget/event/@timestamp)[1]', 'datetime') >= GETDATE() - 1
        AND targetdata.value('(RingBufferTarget/event/data[@name="batch_text"]/value)[1]', 'nvarchar(150)') IS NOT NULL;
END;
