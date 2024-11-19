USE [DBAClient]
GO

CREATE OR ALTER PROCEDURE [dbo].[collect_performance_counters]
	
AS
BEGIN

	SET NOCOUNT ON;

IF OBJECT_ID('os_performance_counters') IS NULL 
BEGIN 
	CREATE TABLE [dbo].[os_performance_counters](
		[recid] [int] IDENTITY(1,1) NOT NULL,
		[instance_fullname] nvarchar(128) NULL,
		[sample_time] smalldatetime NULL,
		[object_name] nvarchar(128) NULL,
		[counter_name] nvarchar(128) NOT NULL,
		[instance_name] nvarchar(128) NULL,
		[cntr_value] bigint NOT NULL
	);

CREATE CLUSTERED COLUMNSTORE INDEX [cci_performancecntr] ON [dbo].[os_performance_counters] ON [PRIMARY]
END

insert into os_performance_counters ([instance_fullname], [sample_time], [object_name], [counter_name], [instance_name], [cntr_value])
select	@@SERVERNAME as instance_fullname, 
		CAST(GETDATE() as smalldatetime) as sample_time,
		RTRIM([object_name]) as object_name
		,RTRIM(counter_name), instance_name, cntr_value 
from sys.dm_os_performance_counters
where	(RTRIM([object_name]) like '%Buffer Manager'
		or RTRIM([object_name]) like '%General Statistics'
		or RTRIM([object_name]) like '%Locks'
		--or RTRIM([object_name]) like '%Databases'
		or RTRIM([object_name]) like '%SQL Statistics'
		or RTRIM([object_name]) like '%Memory Manager'
		or RTRIM([object_name]) like '%Wait Statistics'
		) or (RTRIM([object_name]) like '%Databases' and RTRIM(counter_name) in ('Log Bytes Flushed/sec', 'Transactions/sec'))
		and cntr_value <> 0
		;

delete from os_performance_counters where sample_time <= dateadd(day, -30, getdate());
END
GO