USE [DBAClient]
GO


CREATE OR ALTER PROCEDURE [dbo].[collect_sysprocesses_blocking_tree] 
    @collected_by NVARCHAR (500) = ''

AS

SET NOCOUNT ON ;

begin

---Tables Creation----------------------------------------------------------------------------------------------------
IF (SELECT OBJECT_ID('sysprocesses_blocking_tree')) IS NULL 
BEGIN 

CREATE TABLE [dbo].[sysprocesses_blocking_tree]
(
	sample_time [datetime] NULL,
	[blocking_tree] [nvarchar](max) NULL,
	[dbname] [nvarchar](128) NULL,
	[spid] [smallint] NULL,
	[blocked] [smallint] NULL,
	[lastwaittype] [nchar](32) NULL,
	[cpu] [int] NULL,
	[physical_io] [bigint] NULL,
	[memory_usage] [int] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
END 


IF (SELECT OBJECT_ID('active_processes')) IS NULL 

	BEGIN 
	exec sp_execute 
		'CREATE VIEW [active_processes] as 

		select  DB_NAME(sysproc.dbid) as dbname
				,sysproc.spid
				,sysproc.status
				,c.encrypt_option
				,sysproc.kpid
				,sysproc.blocked
				,sysproc.lastwaittype
				,sysproc.waittime
				,sysproc.cpu
				,sysproc.physical_io
				,sysproc.memusage as memory_usage
				,sysproc.login_time
				,sysproc.last_batch
				,sysproc.open_tran
				,SUBSTRING(CAST(c.protocol_version AS BINARY(4)),1,1 ) as client_protocol_bin
				,CASE SUBSTRING(CAST(c.protocol_version AS BINARY(4)), 1,1)
					WHEN 0x70 THEN ''SQL Server 7.0''
					WHEN 0x71 THEN ''SQL Server 2000''
					WHEN 0x72 THEN ''SQL Server 2005''
					WHEN 0x73 THEN ''SQL Server 2008''
					WHEN 0x74 THEN ''SQL Server 2012+''
					ELSE ''Unknown driver''
				END as client_protocol_version
				,sysproc.[program_name]
				,sysproc.hostname
				,sysproc.hostprocess
				,sysproc.loginame
				,c.client_net_address
				,c.client_tcp_port
				,sysproc.stmt_start
				,sysproc.stmt_end
				,sysproc.cmd as command_type
				,q.text as sql_text
		from sys.sysprocesses as sysproc
		join sys.dm_exec_connections as c on sysproc.spid = c.session_id 
		cross apply sys.dm_exec_sql_text(sysproc.sql_handle) as q;'
	END 

DECLARE @CurrentTime DATETIME = GETDATE();

SELECT dbname, spid, blocked, lastwaittype,cpu, physical_io, memory_usage, REPLACE(REPLACE(sql_text, CHAR(10), ' '), CHAR(13), ' ') AS batch, @CurrentTime AS Timestamp
INTO #T
FROM DBAClient.dbo.active_processes;

WITH blocking_queires (dbname, spid, blocked, lastwaittype, cpu, physical_io, memory_usage, level, batch, timestamp)
AS (
    SELECT dbname, spid, blocked, lastwaittype, cpu, physical_io, memory_usage,
           cast(replicate('0', 4-len(cast(spid as varchar))) + cast(spid as varchar) as varchar(1000)) as level,
           batch,
           timestamp 
    FROM #T R
    WHERE (BLOCKED = 0 OR BLOCKED = SPID)
      AND EXISTS (SELECT * FROM #T R2 WHERE R2.blocked = R.spid AND R2.blocked <> R2.spid)
    UNION ALL
    SELECT R.dbname, R.spid, R.blocked, R.lastwaittype, R.cpu, R.physical_io, R.memory_usage,
           CAST(B.LEVEL + RIGHT(CAST((1000 + R.SPID) AS VARCHAR(100)), 4) AS VARCHAR(1000)) AS level,
           R.batch,
           R.timestamp
    FROM #T AS R
    INNER JOIN blocking_queires B ON R.blocked = B.spid
    WHERE R.blocked > 0 AND R.blocked <> R.spid
)
INSERT INTO [dbo].[sysprocesses_blocking_tree] (sample_time, [blocking_tree], [dbname], [spid], [blocked], [lastwaittype], [cpu], [physical_io], [memory_usage])
SELECT timestamp,
		N'    ' + REPLICATE(N'|         ', LEN(LEVEL)/4 - 1) +
       CASE WHEN (LEN(LEVEL)/4 - 1) = 0 THEN 'HEAD -  ' ELSE '|------  ' END
       + CAST(spid AS NVARCHAR(10)) + N' ' + batch AS blocking_tree,
	   dbname, spid, blocked, lastwaittype, cpu, physical_io, memory_usage
-- into sysprocesses_blocking_tree
FROM blocking_queires
ORDER BY LEVEL ASC;

DROP TABLE #T;

---Delete sample data as per retention period----------------------------------------------------------------------------------------------------

Declare @older_than datetime = DATEADD (DAY,-90, GETDATE());

delete from [sysprocesses_blocking_tree] where sample_time <= @older_than;
delete from who_is_active_log where collection_time <= @older_than;

END 
go

