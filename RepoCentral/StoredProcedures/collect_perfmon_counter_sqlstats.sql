USE [DBA]
GO

/*--------------------------------------------------------------------------
-- Author:			SHEKAR KOLA
-- Create date:		2020-09-01
-- Description:		Collecting required performance counters into central repo...

-------------------------------------------------------------------------*/
CREATE OR ALTER PROCEDURE [dbo].[collect_perfmon_counter_sqlstats]

AS
BEGIN

SET NOCOUNT ON;
-------------------------------------------------------------------------------------------------------------------------------------------------
-- Validate and run if it's only primary replica 
Declare @IsHADR bit;
select @IsHADR = Cast(SERVERPROPERTY ('Ishadrenabled') as int);
if @IsHADR = 0 or exists (	select r.replica_id 
							from sys.dm_hadr_availability_replica_states r
							join sys.availability_groups ag on r.group_id = ag.group_id
							join sys.availability_databases_cluster agc on ag.group_id = agc.group_id and database_name = DB_NAME()
							where is_local =1 and role = 1)
-------------------------------------------------------------------------------------------------------------------------------------------------
BEGIN
		DECLARE @OPENQUERY nvarchar(4000), 
				@TSQL_LinkServer nvarchar(4000), 
				@TSQL_Local nvarchar(4000), 
				@LinkedServer nvarchar(50);

		Declare @TargetServers table (ID int, LinkServer nvarchar(50));

IF (select OBJECT_ID ('tempdb..#performance_counters')) is null
BEGIN 
CREATE TABLE #performance_counters
(
	[instance_fullname] [nvarchar](128) NULL,
	[sample_time] datetime2(2) NULL,
	[counter_name] [nvarchar](250) NOT NULL,
	[cntr_value] [bigint] NULL
)
END 

IF (select OBJECT_ID ('perfmon_counter_sqlstats')) is null
BEGIN 
CREATE TABLE perfmon_counter_sqlstats
(	recid bigint identity (1,1), 
	[instance_fullname] [nvarchar](128) NULL,
	[sample_time] datetime2(2) NULL,
	[counter_name] [nvarchar](250) NOT NULL,
	[cntr_value] [bigint] NULL
);
CREATE CLUSTERED COLUMNSTORE INDEX [cci_perfmon_sqlstats] ON [dbo].perfmon_counter_sqlstats ON [PRIMARY]
END 

PRINT FORMAT(GETDATE(), 'yyyy-MMM-dd HH:mm:ss') + ' TempTable Created; ';

		SET @TSQL_LinkServer = 	'''' + 
									'select * from DBAClient.dbo.perfmon_counter_sqlstats_view'
							+ ''')' ;

		SET @TSQL_Local = 'select * from DBAClient.dbo.perfmon_counter_sqlstats_view';
---------------------------------------------------------------------------------------------------------------------------------
		 --Inserting Local Server data into TempTable 
		Insert into [#performance_counters] ([instance_fullname], [sample_time], [counter_name], [cntr_value])
		Exec (@TSQL_Local)
		Print FORMAT(GETDATE(), 'yyyy-MMM-dd HH:mm:ss') +  'Local Server data loaded into TempTable; ';
----------------------------------------------------------------------------------------------------------------------------------------------------------
-- Calling linked server procedure to process "Indexe usage info" into central temp table from all linked servers
----------------------------------------------------------------------------------------------------------------------------------------------------------

		Insert into @TargetServers select server_id, name from sys.servers where is_linked = 1  and (provider = 'SQLNCLI' or provider = 'MSOLEDBSQL');

		-- begin Loop to prepare staging data from all linked servers 
		While exists (select * from @TargetServers) 
			BEGIN
				SET @LinkedServer = (select top 1 LinkServer from @TargetServers order by ID)
				SET @OPENQUERY = 'SELECT * FROM OPENQUERY(['+ @LinkedServer + '],'

				-- Inserting linked Server data into Temp Table 
				Insert into [#performance_counters] ([instance_fullname], [sample_time], [counter_name], [cntr_value_cumulative], [sample_interval_sec], [cntr_value])

				EXEC (@OPENQUERY + @TSQL_LinkServer);
				Print FORMAT(GETDATE(), 'yyyy-MMM-dd HH:mm:ss') +  ' Linked Server "'+ @LinkedServer +' " data loaded into TempTable; ';

				Delete from @TargetServers where LinkServer = @LinkedServer
			END
---------------------------------------------------------------------------------------------------------------------------------
			BEGIN
			-- Inserting OS_Stats from staging table, if wait type not existed
					Insert into perfmon_counter_sqlstats
								([instance_fullname], [sample_time], [counter_name],  [cntr_value])
					select	S.[instance_fullname], [sample_time], [counter_name], [cntr_value]
					from	[#performance_counters] as s 
					Where not exists (	select 1 from perfmon_counter_sqlstats as d 
										where s.instance_fullname = d.instance_fullname 
												and s.sample_time = d.sample_time
												and s.counter_name = d.counter_name
									)
			END
			Print FORMAT(GETDATE(), 'yyyy-MMM-dd HH:mm:ss') +  ' Inserting into Actual Table - Completed; ';

---------------------------------------------------------------------------------------------------------------------------------
		Drop table #performance_counters;
		PRINT FORMAT(GETDATE(), 'yyyy-MMM-dd HH:mm:ss') + ' TempTable Droped; ';


----- Update date column for already collected counters via Windows level PerfMon ----------------------
--IF(SELECT OBJECT_ID ('CounterData')) is not null 
--	begin
--		update c 
--		set CounterDate = LEFT(CounterDateTime, 10)
--		from [dbo].[CounterData]  as c 
--		where CounterDate is null 
--	end

	END
END
