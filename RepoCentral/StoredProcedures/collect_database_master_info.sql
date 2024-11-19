USE [DBA]
GO

/*-----------------------------------------------------------------------------------------------------------------------------------------------------
	Author:			SHEKAR KOLA
	Create date:	2024-01-07
	Description:	Central database master 

	Version: 20240107
	-	Database size details added based on sys.master_files

	Version: 20220829
	-	Along with database master info, the DB Instances will be stored in separate table
	-	DB Instance information used for grouping Instances into Environments 
	-	Renamed table name from "DatabaseMaster" to database_master

	Version: 20220207
	-	Remote procedure call disabled 

	Version: 20220207
		-	Remote procedure call disabled 

	Version: 20210707
		-	Changed primary key of DatabaseMaster table to RECID (Identity Column) to avoid error when multiple linked servers (AG Name and Hostname) active 
	
	Version: 20210120
		-	Extended properties collection has been removed, planning to add as separate procedure for that
		-	Based above change central procedure changed to skip extended properties columns update/insert 
		-	Procedure name changed to align with standard naming convenion (all small) 
				Old name: [ProcessDatabaseMaster]
				New name: [collect_database_master_info]

	Version: 20190721

-----------------------------------------------------------------------------------------------------------------------------------------------------------*/

CREATE OR ALTER PROCEDURE [dbo].[collect_database_master_info]
	-- Add the parameters for the stored procedure here
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
SET NOCOUNT ON;

		DECLARE @OPENQUERY nvarchar(4000), 
				@OPENQUERY2 nvarchar(4000), 
				@TSQL_LinkServer nvarchar(4000), 
				@TSQL_LinkServer2 nvarchar(4000),
				@TSQL_Local nvarchar(4000), 
				@TSQL_Local2 nvarchar(4000), 
				@LinkedServer nvarchar(250);

		Declare @IsHADR bit;
	IF (Select OBJECT_ID ('tempdb..#tempdbmaster_TargetServers')) is null 
	BEGIN
		Create table #tempdbmaster_TargetServers (ID int, LinkServer nvarchar(50));
	END

	IF (Select OBJECT_ID ('tempdb..#tempdbmaster_TargetServers2')) is null 
	BEGIN
		Create table #tempdbmaster_TargetServers2 (ID int, LinkServer nvarchar(50));
	END

-- Validate and run if it's only primary replica ------------------------------------------------------------------------------------
	select @IsHADR = Cast(SERVERPROPERTY ('Ishadrenabled') as int);
if @IsHADR = 0 or exists (	select r.replica_id 
							from sys.dm_hadr_availability_replica_states r
							join sys.availability_groups ag on r.group_id = ag.group_id
							join sys.availability_databases_cluster agc on ag.group_id = agc.group_id and database_name = DB_NAME()
							where is_local =1 and role = 1)
-- Validate and run if it's only primary replica ------------------------------------------------------------------------------------

BEGIN 
	IF (select OBJECT_ID ('tempdb..#TempDBMaster')) is null
	BEGIN 
		CREATE TABLE #TempDBMaster 
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
			IS_IN_STANDBY bit,
			ROW_DATA_MB int,
			LOG_DATA_MB int,
			FILESTREAM_DATA_MB int,
			FULLTEXT_DATA_MB int,
			UNKNOWN_DATA_MB int
			);
		CREATE CLUSTERED INDEX CI_TEMPDBAMSTER ON #TempDBMaster (RECID);
	END

	Print Format (getdate(), 'yyyy-MMM-dd HH:mm:ss')+ ' TempTable Created; ';

		SET @TSQL_LinkServer = 	'''' + 
									'select [INSTANCEFULLNAME], [INSTANCENAME], [DATABASE_ID], [DATABASENAME], [CREATE_DATE], [COMPATIBILITY_LEVEL], [COLLATION_NAME], [RECOVERY_MODEL], [IS_AUTO_CREATE_STATS_ON], [IS_AUTO_UPDATE_STATS_ON], [IS_FULLTEXT_ENABLED], [IS_TRUSTWORTHY_ON], [IS_ENCRYPTED], [IS_QUERY_STORE_ON], [IS_PUBLISHED], [IS_SUBSCRIBED], [IS_MERGE_PUBLISHED], [IS_DISTRIBUTOR], [LOG_REUSE_WAIT], [IS_JOINED_AVAILABILITYGROUPS], [TARGET_RECOVERY_SECONDS], [CONTAINMENT], [AGNAME], [IS_AG_PRIMARY], [IS_BACKUP_SCHEDULED], [IS_INDEXMAINTAIN_SCHEDULED], [DB_State], [APPLICATION_NAME], [DB_DESCRIPTION], IS_IN_STANDBY
 , ROW_DATA_MB
 , LOG_DATA_MB
 , FILESTREAM_DATA_MB
 , FULLTEXT_DATA_MB
 , UNKNOWN_DATA_MB
 from DBAClient.dbo.database_properties' 
							+ ''')' ;

	-------------------------------------------------
		SET @TSQL_Local = 	
									'select [INSTANCEFULLNAME], [INSTANCENAME], [DATABASE_ID], [DATABASENAME], [CREATE_DATE], [COMPATIBILITY_LEVEL], [COLLATION_NAME], [RECOVERY_MODEL], [IS_AUTO_CREATE_STATS_ON], [IS_AUTO_UPDATE_STATS_ON], [IS_FULLTEXT_ENABLED], [IS_TRUSTWORTHY_ON], [IS_ENCRYPTED], [IS_QUERY_STORE_ON], [IS_PUBLISHED], [IS_SUBSCRIBED], [IS_MERGE_PUBLISHED], [IS_DISTRIBUTOR], [LOG_REUSE_WAIT], [IS_JOINED_AVAILABILITYGROUPS], [TARGET_RECOVERY_SECONDS], [CONTAINMENT], [AGNAME], [IS_AG_PRIMARY], [IS_BACKUP_SCHEDULED], [IS_INDEXMAINTAIN_SCHEDULED], [DB_State], [APPLICATION_NAME], [DB_DESCRIPTION], IS_IN_STANDBY
 , ROW_DATA_MB
 , LOG_DATA_MB
 , FILESTREAM_DATA_MB
 , FULLTEXT_DATA_MB
 , UNKNOWN_DATA_MB
 from DBAClient.dbo.database_properties'  ;

		
		--Inserting Local Server data into TempTable 
		 Insert into #TempDBMaster 
						([INSTANCEFULLNAME] ,[INSTANCENAME], [DATABASE_ID], [DATABASENAME] ,[CREATE_DATE] ,[COMPATIBILITY_LEVEL],[COLLATION_NAME],
						 [RECOVERY_MODEL], 
						 [IS_AUTO_CREATE_STATS_ON], 
						 [IS_AUTO_UPDATE_STATS_ON], 
						 [IS_FULLTEXT_ENABLED], 
						 [IS_TRUSTWORTHY_ON], 
						 [IS_ENCRYPTED] ,
						 [IS_QUERY_STORE_ON] ,
						 [IS_PUBLISHED] ,
						 [IS_SUBSCRIBED] ,
						 [IS_MERGE_PUBLISHED] ,
						 [IS_DISTRIBUTOR], 
						 [LOG_REUSE_WAIT] ,
						 [IS_JOINED_AVAILABILITYGROUPS] ,
						 [TARGET_RECOVERY_SECONDS] ,
						 [CONTAINMENT] ,
						 [AGNAME] ,
						 [IS_AG_PRIMARY], 
						 [IS_BACKUP_SCHEDULED] ,
						 [IS_INDEXMAINTAIN_SCHEDULED]
						 , DB_STATE
						 , APPLICATION_NAME
						 , DB_DESCRIPTION
						 , IS_IN_STANDBY
						 , ROW_DATA_MB
						 , LOG_DATA_MB
						 , FILESTREAM_DATA_MB
						 , FULLTEXT_DATA_MB
						 , UNKNOWN_DATA_MB
						)
		Exec (@TSQL_Local);
		

		Print Format (getdate(), 'yyyy-MMM-dd HH:mm:ss')+ ' Local Server data loaded into TempTable; ';

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Calling linked server procedure to process "extended events data" locally on every linked server  
--- Following remote procedure call has been disabled as of 2022-02-07 due to security related (remote access) errors, enabled client side job to run hourly to collect properties locally
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	--Insert into #tempdbmaster_TargetServers2 select server_id, name from sys.servers where is_linked = 1  and (provider = 'SQLNCLI' or provider = 'MSOLEDBSQL');
	
	--While exists (select * from #tempdbmaster_TargetServers2) 
	--	BEGIN
	--		BEGIN TRY 
	--		SET @LinkedServer = (select top 1 LinkServer from #tempdbmaster_TargetServers2 order by ID);
	--		SET @OPENQUERY2 =  'Exec [' +@LinkedServer+ '].DBAClient.dbo.process_db_extended_property;';

	--		Print FORMAT(GETDATE(), 'yyyy-MMM-dd HH:mm:ss') +  ' Calling remote server client procedure at [' + @LinkedServer + ']; ';
	--		Print @OPENQUERY2;

	--		exec (@OPENQUERY2);

	--		IF (@@ERROR <> 0)
	--				begin
	--					Print	FORMAT(GETDATE(), 'yyyy-MMM-dd HH:mm:ss') +  
	--							' Error  occurred at ['+ @LinkedServer +'], Error Message: ' 
	--							+ Cast (ERROR_MESSAGE () as nvarchar (500)) +'; ';

	--					Delete from #tempdbmaster_TargetServers2 where LinkServer = @LinkedServer;
	--				end
	--		Delete from #tempdbmaster_TargetServers2 where LinkServer = @LinkedServer;
	--		END TRY 
	--		BEGIN CATCH
	--			Print FORMAT(GETDATE(), 'yyyy-MMM-dd HH:mm:ss') +  ' ERRO While reading data from Linked Server ['+ @LinkedServer +'] ; ';				
	--			INSERT INTO error_log_dc (process_name,object_ref, error_msg)
	--			SELECT CAST(OBJECT_NAME(@@PROCID) AS nvarchar(200)) + ' - Remote Proc', @LinkedServer, ERROR_MESSAGE();

	--			Delete from #tempdbmaster_TargetServers2 where LinkServer = @LinkedServer;
	--		END CATCH 
	--	END
----------------------------------------------------------------------------------------------------------------------------------------------------------
-- COMPLETED: Calling linked server procedure to process "extended events data" locally on every linked server  
----------------------------------------------------------------------------------------------------------------------------------------------------------

		Insert into #tempdbmaster_TargetServers select server_id, name from sys.servers where is_linked = 1  and (provider = 'SQLNCLI' or provider = 'MSOLEDBSQL');

		-- begin Loop to prepare staging data from all linked servers 
		While exists (select * from #tempdbmaster_TargetServers) 
			BEGIN
				BEGIN TRY 
				SET @LinkedServer = (select top 1 LinkServer from #tempdbmaster_TargetServers order by ID);
				--SET @LinkedServer = QUOTENAME (@LinkedServer);
				 
				SET @OPENQUERY = 'SELECT * FROM OPENQUERY(['+ @LinkedServer + '],';

				-- Inserting linked Server data into Temp Table 
						Insert into #TempDBMaster 
						([INSTANCEFULLNAME] ,[INSTANCENAME],
						 [DATABASE_ID],
						 [DATABASENAME] ,
						 [CREATE_DATE] ,
						 [COMPATIBILITY_LEVEL],
						 [COLLATION_NAME],
						 [RECOVERY_MODEL], 
						 [IS_AUTO_CREATE_STATS_ON], 
						 [IS_AUTO_UPDATE_STATS_ON], 
						 [IS_FULLTEXT_ENABLED], 
						 [IS_TRUSTWORTHY_ON], 
						 [IS_ENCRYPTED] ,
						 [IS_QUERY_STORE_ON] ,
						 [IS_PUBLISHED] ,
						 [IS_SUBSCRIBED] ,
						 [IS_MERGE_PUBLISHED] ,
						 [IS_DISTRIBUTOR], 
						 [LOG_REUSE_WAIT] ,
						 [IS_JOINED_AVAILABILITYGROUPS] ,
						 [TARGET_RECOVERY_SECONDS] ,
						 [CONTAINMENT] ,
						 [AGNAME] ,
						 [IS_AG_PRIMARY], 
						 [IS_BACKUP_SCHEDULED] ,
						 [IS_INDEXMAINTAIN_SCHEDULED], DB_STATE, APPLICATION_NAME, DB_DESCRIPTION, IS_IN_STANDBY
						 ,ROW_DATA_MB , LOG_DATA_MB , FILESTREAM_DATA_MB , FULLTEXT_DATA_MB , UNKNOWN_DATA_MB
						)
						EXEC (@OPENQUERY+@TSQL_LinkServer);
				---print @OPENQUERY+@TSQL_LinkServer;
				Print Format (getdate(), 'yyyy-MMM-dd HH:mm:ss')+ ' Linked Server ['+ @LinkedServer +'] data loaded into TempTable; ';
				Delete from #tempdbmaster_TargetServers where LinkServer = @LinkedServer;

				END TRY
				BEGIN CATCH
					Print FORMAT(GETDATE(), 'yyyy-MMM-dd HH:mm:ss') +  ' ERRO While reading data from Linked Server ['+ @LinkedServer +'] ; ';				
					INSERT INTO error_log_dc (process_name,object_ref, error_msg)
					SELECT CAST(OBJECT_NAME(@@PROCID) AS nvarchar(200)), @LinkedServer, ERROR_MESSAGE();

					Delete from #tempdbmaster_TargetServers where LinkServer = @LinkedServer;
				END CATCH 
			END

--------------------------------------------------------------------------------------------------------------------------------------
				-- Inserting DatabaseMaster from staging table
					BEGIN
					Print Format (getdate(), 'yyyy-MMM-dd HH:mm:ss')+ ' Inserting into Actual Table - Starting; ';

						INSERT INTO database_master 
							([INSTANCEFULLNAME], [INSTANCENAME],
								[DATABASE_ID],[DATABASENAME],
								[CREATE_DATE],[COMPATIBILITY_LEVEL],
								[COLLATION_NAME],[RECOVERY_MODEL], 
								[IS_AUTO_CREATE_STATS_ON], [IS_AUTO_UPDATE_STATS_ON], 
								[IS_FULLTEXT_ENABLED], [IS_TRUSTWORTHY_ON], 
								[IS_ENCRYPTED] ,[IS_QUERY_STORE_ON],
								[IS_PUBLISHED] ,[IS_SUBSCRIBED],
								[IS_MERGE_PUBLISHED] ,[IS_DISTRIBUTOR], 
								[LOG_REUSE_WAIT], [IS_JOINED_AVAILABILITYGROUPS],
								[TARGET_RECOVERY_SECONDS] ,[CONTAINMENT],
								[AGNAME], [IS_AG_PRIMARY], 
								[IS_BACKUP_SCHEDULED],[IS_INDEXMAINTAIN_SCHEDULED], 
								CREATED_ON, DB_STATE, APPLICATION_NAME, DB_DESCRIPTION, IS_IN_STANDBY
								,ROW_DATA_MB , LOG_DATA_MB , FILESTREAM_DATA_MB , FULLTEXT_DATA_MB , UNKNOWN_DATA_MB
							 )
						SELECT 	S.[INSTANCEFULLNAME] ,S.[INSTANCENAME],
								S.[DATABASE_ID] ,S.[DATABASENAME] ,
								S.[CREATE_DATE] ,S.[COMPATIBILITY_LEVEL],
								S.[COLLATION_NAME],S.[RECOVERY_MODEL], 
								S.[IS_AUTO_CREATE_STATS_ON], S.[IS_AUTO_UPDATE_STATS_ON], 
								S.[IS_FULLTEXT_ENABLED], S.[IS_TRUSTWORTHY_ON], 
								S.[IS_ENCRYPTED] ,S.[IS_QUERY_STORE_ON] ,
								S.[IS_PUBLISHED] ,S.[IS_SUBSCRIBED] ,
								S.[IS_MERGE_PUBLISHED] ,S.[IS_DISTRIBUTOR], 
								S.[LOG_REUSE_WAIT] ,S.[IS_JOINED_AVAILABILITYGROUPS] ,
								S.[TARGET_RECOVERY_SECONDS] ,S.[CONTAINMENT] ,
								S.[AGNAME] ,S.[IS_AG_PRIMARY], S.[IS_BACKUP_SCHEDULED] ,
								S.[IS_INDEXMAINTAIN_SCHEDULED], GETDATE (), DB_STATE, APPLICATION_NAME, DB_DESCRIPTION, IS_IN_STANDBY
								,ROW_DATA_MB , LOG_DATA_MB , FILESTREAM_DATA_MB , FULLTEXT_DATA_MB , UNKNOWN_DATA_MB
					FROM #TempDBMaster as S
						Where NOT EXISTS 
								(SELECT 1 
								 FROM database_master as D 
								 WHERE s.[INSTANCEFULLNAME] = d.[INSTANCEFULLNAME] and s.DATABASENAME = d.DATABASENAME
								);
						SELECT @@ROWCOUNT as TotalInserted;
					END
					
--------------------------------------------------------------------------------------------------------------------------------------
					-- Updating DatabaseStats  from staging table
					begin	
					Print Format (getdate(), 'yyyy-MMM-dd HH:mm:ss')+ ' Updating Actual Table - Starting; '
						UPDATE D set	[DatabaseName] = s.DatabaseName,
										[recovery_model] = s.[recovery_model],
										[Collation_name] = s.collation_name, 
										[IS_AUTO_CREATE_STATS_ON]  = s.[IS_AUTO_CREATE_STATS_ON], 
										[IS_AUTO_UPDATE_STATS_ON] = s.[IS_AUTO_UPDATE_STATS_ON], 
										[IS_FULLTEXT_ENABLED] = s.[IS_FULLTEXT_ENABLED], 
										[IS_TRUSTWORTHY_ON] = s.[IS_TRUSTWORTHY_ON], 
										[IS_ENCRYPTED] = s.[IS_ENCRYPTED], 
										[IS_QUERY_STORE_ON] = s.[IS_QUERY_STORE_ON],
										[IS_PUBLISHED] = s.[IS_PUBLISHED],
										[IS_SUBSCRIBED] = s.[IS_SUBSCRIBED],
										[IS_MERGE_PUBLISHED] = s.[IS_MERGE_PUBLISHED] ,
										[IS_DISTRIBUTOR] = s.[IS_DISTRIBUTOR],
										[LOG_REUSE_WAIT] = s.[LOG_REUSE_WAIT],
										[IS_JOINED_AVAILABILITYGROUPS] = s.[IS_JOINED_AVAILABILITYGROUPS],
										[TARGET_RECOVERY_SECONDS] = s.[TARGET_RECOVERY_SECONDS],
										[CONTAINMENT] = s.[CONTAINMENT],
										[AGNAME] = s.[AGNAME],
										[IS_AG_PRIMARY] = s.[IS_AG_PRIMARY],
										[IS_BACKUP_SCHEDULED] = s.[IS_BACKUP_SCHEDULED],
										[IS_INDEXMAINTAIN_SCHEDULED] = s.[IS_INDEXMAINTAIN_SCHEDULED],
										MODIFIED_ON = GETDATE (), 
										DB_STATE = S.DB_STATE, 
										APPLICATION_NAME = S.APPLICATION_NAME, 
										DB_DESCRIPTION = S.DB_DESCRIPTION,
										IS_IN_STANDBY = S.IS_IN_STANDBY,
										ROW_DATA_MB = S.ROW_DATA_MB, 
										LOG_DATA_MB = S.LOG_DATA_MB, 
										FILESTREAM_DATA_MB = S.FILESTREAM_DATA_MB, 
										FULLTEXT_DATA_MB = S.FULLTEXT_DATA_MB, 
										UNKNOWN_DATA_MB = S.UNKNOWN_DATA_MB
						FROM database_master as D
						left outer join #TempDBMaster as S
								on S.[INSTANCEFULLNAME] = d.[INSTANCEFULLNAME] and S.DATABASENAME = d.DATABASENAME
						Where S.database_id is not null;
						SELECT @@ROWCOUNT as TotalUpdated;
					end

----Updated DROPPED Databases----------------------------------------------------------------------------------------------------------------------------------
					begin	
					Print Format (getdate(), 'yyyy-MMM-dd HH:mm:ss')+ ' Updating Actual Table (Dropped Tables) - Starting; '
						UPDATE D set	IS_DROPPED = 1,
										MODIFIED_ON = GETDATE ()
						FROM database_master as D
						Where NOT EXISTS 
								(SELECT 1 
								 FROM #TempDBMaster as S
								 WHERE s.[INSTANCEFULLNAME] = d.[INSTANCEFULLNAME] and s.DATABASENAME = d.DATABASENAME
								);
							--and (NOT S.[IS_JOINED_AVAILABILITYGROUPS] = 0);
						SELECT @@ROWCOUNT as TotalUpdated_Dropped;
					end
------Updated DROPPED Databases--------------------------------------------------------------------------------------------------------------------------------

------Updated DB Instance Info--------------------------------------------------------------------------------------------------------------------------------
;with db_inst as (
SELECT	Distinct 
		(CASE	WHEN charindex ('\', INSTANCEFULLNAME, 1) = 0 THEN INSTANCEFULLNAME 
				ELSE SUBSTRING(INSTANCEFULLNAME, 1, charindex ('\', INSTANCEFULLNAME, 1)-1) 
		END) as hostname,
		INSTANCEFULLNAME as instance_fullname,
		(CASE	WHEN charindex ('\', INSTANCEFULLNAME, 1) = 0 THEN 'Default' 
				ELSE SUBSTRING(INSTANCEFULLNAME, charindex ('\', INSTANCEFULLNAME, 1)+1, 50) 
				END) as instance_name
FROM database_master
WHERE IS_DROPPED = 0
)

Insert into db_instances (instance_full_name, instance_host, instance_name)
select instance_fullname, hostname, instance_name
from db_inst as s 
where instance_fullname not in (select instance_full_name from db_instances);
------Updated DB Instance Info--------------------------------------------------------------------------------------------------------------------------------


	END
	ELSE PRINT 'This is NOT a Primary replica!'
END
GO



