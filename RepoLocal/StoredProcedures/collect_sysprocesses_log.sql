USE [DBAClient]
GO

/*-- ========================================================================================================================================================================
-- Author:			Shekar Kola
-- Create date:		2020-07-07
-- Description:		Collecting currently active session into table, so that it can be triggered by any alert/job to collect snapshot of active sessions 
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Versions:
    20230419
        - New parameter added, this will collect the source name which triggering the snapshot. default value is ''
    20200908:
        - Initial Version

============================================================================================================================================================================*/

CREATE OR ALTER proc [dbo].[collect_sysprocesses_log] 
    @collected_by NVARCHAR (500) = ''

as 
set nocount on ;

begin

---Tables Creation----------------------------------------------------------------------------------------------------

IF (select OBJECT_ID ('sysprocesses_log')) IS NULL 
BEGIN 
CREATE TABLE [dbo].[sysprocesses_log](
	[sample_time] [datetime] NOT NULL,
	[databasename] [nvarchar](128) NULL,
	[spid] [smallint] NOT NULL,
	wait_type varchar(200),
	wait_time_ms int,
	blocked_by int,
	[hostname] [nchar](128) NOT NULL,
	[program_name] [nchar](128) NOT NULL,
	[loginame] [nchar](128) NOT NULL,
	[user_name] [nchar](128) NOT NULL,
	[login_time] [datetime] NOT NULL,
	[last_batch] [datetime] NOT NULL,
	[cpu] [int] NOT NULL,
	[physical_io] [bigint] NOT NULL,
	[open_tran] [smallint] NOT NULL,
	[status] [nchar](30) NOT NULL,
	[cmd] [nchar](16) NOT NULL,
	[text] [nvarchar](max) NULL,
	[memory_usage_mb] [numeric](10, 2) NULL,
	client_interface_name nvarchar(256),
	client_int_version float,
	isolation_level varchar(128),
    collected_by nvarchar (500),
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

END
---Tables Creation end ----------------------------------------------------------------------------------------------------
--begin 
--	insert into who_is_active_log
--	exec DBAClient.dbo.sp_WhoIsActive;
--end 

---Insert New Sample data----------------------------------------------------------------------------------------------------
insert into [sysprocesses_log]
([sample_time], [databasename], [spid], wait_type, wait_time_ms, blocked_by, [hostname], [program_name], [loginame], [user_name], [login_time], [last_batch]
, [cpu], [physical_io],memory_usage_mb, [open_tran], [status], [cmd], [text], client_interface_name, client_int_version, isolation_level)
select	GETDATE() as sample_time
		,DB_NAME(sp.[dbid]) as databasename,
		sp.spid, 
		lastwaittype,
		waittime,
		blocked, hostname, sp.[program_name], loginame, sp.nt_username,
		sp.login_time, last_batch, 
		cpu, physical_io, (memusage*8.0)/1024 as mem_usage_mb,
		open_tran, sp.status, 
		cmd,
		t.text,
		s.client_interface_name, s.client_version
		,case s.transaction_isolation_level
		when 0 THEN  'Unspecified'
		when 1 THEN  'ReadUncommitted'
		when 2 THEN  'ReadCommitted'
		when 3 THEN  'RepeatableRead'
		when 4 THEN  'Serializable'
		when 5 THEN  'Snapshot' end
        ,@collected_by
from sys.sysprocesses as sp
cross apply sys.dm_exec_sql_text(sp.sql_handle) t
inner join sys.dm_exec_sessions as s on sp.spid = s.session_id
where sp.status <> 'background';
---Insert New Sample data end----------------------------------------------------------------------------------------------------


---Delete sample data as per retention period----------------------------------------------------------------------------------------------------

Declare @older_than datetime = DATEADD (DAY,-90, GETDATE());

delete from sysprocesses_log where sample_time <= @older_than;
delete from who_is_active_log where collection_time <= @older_than;

END 
GO


-- USE DBAClient; ALTER TABLE sysprocesses_log add collected_by nvarchar (500);