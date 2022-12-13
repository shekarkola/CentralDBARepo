/*------------------------------------------------------------------------------------------------------------------------------------------------
-- Author:			Shekar Kola
-- Create date:		2021-10-19
-- Description:		Standard Configuration rollout for Query Store 

Version: 20211019
	-	Initial Version 
-------------------------------------------------------------------------------------------------------------------------------------------------*/
ALTER   PROC [dbo].[query_store_configuration] 

AS
BEGIN
Declare @qs_query nvarchar(4000);
Declare @DatabaseName varchar(256);
Declare @IsHADR bit;
Declare @Version int;

SET NOCOUNT ON;

select @Version = CAST(SERVERPROPERTY('ProductMajorVersion') as int);

drop table if exists #qs_dbs; 
create table #qs_dbs (DatabaseName varchar(256));

insert into #qs_dbs 
select name from sys.databases
where is_query_store_on = 1 and is_read_only = 0 and state = 0;

--select * from #qs_dbs

while exists (select 1 from #qs_dbs)
BEGIN 
	SELECT top 1 @DatabaseName = DatabaseName from #qs_dbs;
	--- For 2016 Databases
	IF @Version < 13
		BEGIN
			SET @qs_query = 'SELECT ';
		END 
	IF @Version = 13
		BEGIN
		SELECT @qs_query = --'ALTER DATABASE ' + QUOTENAME(@DatabaseName) + ' SET QUERY_STORE (INTERVAL_LENGTH_MINUTES = 60);';
							' ALTER DATABASE ' + QUOTENAME(@DatabaseName) +
							' SET QUERY_STORE
								(
									OPERATION_MODE = READ_WRITE,
									CLEANUP_POLICY = ( STALE_QUERY_THRESHOLD_DAYS = 45 ),
									DATA_FLUSH_INTERVAL_SECONDS = 900,
									QUERY_CAPTURE_MODE = AUTO,
									-- MAX_STORAGE_SIZE_MB = 256,
									INTERVAL_LENGTH_MINUTES = 60
								);'
		END 

	IF @Version > 13	--- For 2019 Databases
	BEGIN 
	SELECT @qs_query = --'ALTER DATABASE ' + QUOTENAME(@DatabaseName) + ' SET QUERY_STORE (INTERVAL_LENGTH_MINUTES = 60);';
						' ALTER DATABASE ' + QUOTENAME(@DatabaseName) +
						' SET QUERY_STORE
							(
								OPERATION_MODE = READ_WRITE,
								CLEANUP_POLICY = ( STALE_QUERY_THRESHOLD_DAYS = 45 ),
								DATA_FLUSH_INTERVAL_SECONDS = 900,
								QUERY_CAPTURE_MODE = AUTO,
								-- MAX_STORAGE_SIZE_MB = 256,
								INTERVAL_LENGTH_MINUTES = 60,
								WAIT_STATS_CAPTURE_MODE = ON
							);'
	END
	----------------------------------------------------------------------------------------------------------------------------------------
	-- Validate and run if it's only primary replica 
		select @IsHADR = Cast(SERVERPROPERTY ('Ishadrenabled') as int);
		if @IsHADR = 0 or exists (
								select r.replica_id 
								from sys.dm_hadr_availability_replica_states r
								join sys.availability_groups ag on r.group_id = ag.group_id
								join sys.availability_databases_cluster agc on ag.group_id = agc.group_id and database_name = @DatabaseName
								where is_local =1 and role = 1)
	----------------------------------------------------------------------------------------------------------------------------------------
		BEGIN
			Print 'Configuration applied for '+ QUOTENAME(@DatabaseName);
			exec sp_executesql @qs_query;
			DELETE FROM #qs_dbs where DatabaseName = @DatabaseName;
		END 
	ELSE BEGIN 
		Print 'The Database '+ QUOTENAME(@DatabaseName) + ' is current ready-only secondary, change not applicable here...'
		DELETE FROM #qs_dbs where DatabaseName = @DatabaseName;
	END
END 

	drop table #qs_dbs;
END
GO
