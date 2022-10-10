USE [DBAClient]
GO

/*-- ========================================================================================================================================================================
-- Author:			Shekar Kola
-- Create date:		2020-07-07
-- Description:		Extended properties for database classification and description 
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Versions:
    20200707:
        - Initial Version

============================================================================================================================================================================*/
CREATE OR ALTER PROCEDURE [dbo].[process_db_extended_property]

	--@PropertyName nvarchar(128)
AS
BEGIN

SET NOCOUNT ON;

if (select object_id('database_properties')) is null 
	begin 
		create table database_properties
		(RECID int identity (1,1),
			[INSTANCEFULLNAME] [nvarchar](128) NULL,
			[INSTANCENAME] [nvarchar](128) NULL,
			[DATABASE_ID] [int] NOT NULL,
			[DATABASENAME] [sysname] NOT NULL,
			[CREATE_DATE] [datetime] NOT NULL,
			[COMPATIBILITY_LEVEL] [tinyint] NOT NULL,
			[COLLATION_NAME] [sysname] NULL,
			[RECOVERY_MODEL] [nvarchar](60) NULL,
			[IS_AUTO_CREATE_STATS_ON] [bit] NULL,
			[IS_AUTO_UPDATE_STATS_ON] [bit] NULL,
			[IS_FULLTEXT_ENABLED] [bit] NULL,
			[IS_TRUSTWORTHY_ON] [bit] NULL,
			[IS_ENCRYPTED] [bit] NULL,
			[IS_QUERY_STORE_ON] [bit] NULL,
			[IS_PUBLISHED] [bit] NOT NULL,
			[IS_SUBSCRIBED] [bit] NOT NULL,
			[IS_MERGE_PUBLISHED] [bit] NOT NULL,
			[IS_DISTRIBUTOR] [bit] NOT NULL,
			[LOG_REUSE_WAIT] [nvarchar](60) NULL,
			[IS_JOINED_AVAILABILITYGROUPS] [int] NOT NULL,
			[TARGET_RECOVERY_SECONDS] [int] NULL,
			[CONTAINMENT] [nvarchar](60) NULL,
			[AGNAME] [sysname] NULL,
			[IS_AG_PRIMARY] [bit] NULL,
			[IS_BACKUP_SCHEDULED] [int] NOT NULL,
			[IS_INDEXMAINTAIN_SCHEDULED] [int] NOT NULL,
			DB_STATE smallint,
			APPLICATION_NAME nvarchar(250),
			DB_DESCRIPTION nvarchar(500),
			IS_IN_STANDBY bit
		);
	end

if (select object_id('tempdb..#TargetDBs')) is null 
	begin 
		create table #TargetDBs
		([database_name] nvarchar(500));
	end

if (select object_id('tempdb..#XProperties')) is null 
	begin 
		create table #XProperties
		(InstanceFullName nvarchar(128), database_id int, [database_name] nvarchar(500), property_name nvarchar(250), property_value nvarchar(500));
	end

insert into #TargetDBs
select name from sys.databases where state = 0 and database_id > 4;

UPDATE STATISTICS #TargetDBs; ---- To avoid Temp Table Caching Issue

Declare @dbname nvarchar(500);
Declare @cmd1 nvarchar(4000);
Declare @cmd2 nvarchar(4000);

while exists (select 1 from #TargetDBs)
begin 
	print 'Loop started..'
	set @dbname = (select top 1 [database_name] from #TargetDBs);
	select @cmd1 = 'use [' + @dbname + ']; ' +
	'select db_id(), db_name() as [database_name], cast([name] as  nvarchar(250)) as name, cast(value as nvarchar(250)) as value
	 from sys.extended_properties where class = 0 ';

	print @cmd1;
	insert into #XProperties (database_id, [database_name], property_name, property_value)
	exec(@cmd1);
	Print 'Extended Property "Application" processed in temp for the DB ' + @dbname;

	DELETE FROM #TargetDBs where database_name = @dbname;
end 

--- Drop temporary tables 
drop table #TargetDBs;

BEGIN
	TRUNCATE TABLE database_properties;

	insert into database_properties ([INSTANCEFULLNAME], [INSTANCENAME], [DATABASE_ID], [DATABASENAME], [CREATE_DATE], [COMPATIBILITY_LEVEL], [COLLATION_NAME], [RECOVERY_MODEL], [IS_AUTO_CREATE_STATS_ON], [IS_AUTO_UPDATE_STATS_ON], [IS_FULLTEXT_ENABLED], [IS_TRUSTWORTHY_ON], [IS_ENCRYPTED], [IS_QUERY_STORE_ON], [IS_PUBLISHED], [IS_SUBSCRIBED], [IS_MERGE_PUBLISHED], [IS_DISTRIBUTOR], [LOG_REUSE_WAIT], [IS_JOINED_AVAILABILITYGROUPS], [TARGET_RECOVERY_SECONDS], [CONTAINMENT], [AGNAME], [IS_AG_PRIMARY], [IS_BACKUP_SCHEDULED], [IS_INDEXMAINTAIN_SCHEDULED], [DB_State], [APPLICATION_NAME], [DB_DESCRIPTION], IS_IN_STANDBY)
	select  @@SERVERNAME INSTANCEFULLNAME, @@SERVERNAME as INSTANCENAME,
			d.database_id,
			d.name as databasename,
			create_date,
			compatibility_level,
			collation_name,
			recovery_model_desc as recovery_model,
			is_auto_create_stats_on,
			is_auto_update_stats_on,
			is_fulltext_enabled,
			is_trustworthy_on,
			is_encrypted,
			is_query_store_on,
			is_published,
			is_subscribed,
			is_merge_published,
			is_distributor,
			log_reuse_wait_desc as log_reuse_wait,
			iif(d.group_database_id is null, 0, 1) as is_joined_availabilitygroups,
			target_recovery_time_in_seconds as target_recovery_seconds,
			containment_desc as containment,
			ag.name as agname,
			agdb.is_primary_replica as is_ag_primary,
			0 as is_backup_scheduled,
			0 as is_indexmaintain_scheduled,
			state,
			ep.property_value as  application_name,
			ep2.property_value as description,
			d.is_in_standby
	from sys.databases as d 
	left outer join master.sys.dm_hadr_database_replica_states as agdb on d.group_database_id = agdb.group_database_id and agdb.is_local = 1
	left outer join master.sys.availability_groups as ag on agdb.group_id = ag.group_id
	left outer join #XProperties as ep on d.database_id = ep.database_id and ep.property_name = 'Application'
	left outer join #XProperties as ep2 on d.database_id = ep2.database_id and ep2.property_name = 'Description'
	where d.database_id > 4
		and not exists (
					select 1 
					from database_properties as t
					where d.database_id = t.database_id 
					 );

	update t  set t.APPLICATION_NAME = s.property_value
	from database_properties as t
	join #XProperties as s on t.InstanceFullName = s.InstanceFullName and t.database_id = s.database_id and s.property_name = 'Application';

	update t  set t.DB_DESCRIPTION = s.property_value
	from database_properties as t
	join #XProperties as s on t.InstanceFullName = s.InstanceFullName and t.database_id = s.database_id and s.property_name = 'Description';


END 

-- select * from database_properties;

END
