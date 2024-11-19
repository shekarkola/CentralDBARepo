USE DBA
GO

-- =========================================================
-- Author:			Shekar Kola
-- Create date:		2024-11-13
-- Modified date:	2024-11-15
-- Description:		Data collection for Data Catalog
-- =========================================================
CREATE OR ALTER PROCEDURE [dbo].[collect_catalogdetails]  
	@ServerName sysname = null, 
	@UpdateType tinyint = null
		
AS
BEGIN

	SET NOCOUNT ON;

DECLARE @OPENQUERY nvarchar(4000), 
		@TSQL_LinkServer nvarchar(4000), 
		@TSQL_Local nvarchar(4000), 
		@LinkedServer nvarchar(128);

IF @UpdateType IS NULL 
	BEGIN 
	print 'This procedure developed to collect the column level details including data classificaiton information for the data-catalog preparation, use following details and examples to execute it.
	
	Parameters: 
		- @ServerName:	Accepts the servername (Linked servers created locally), by deault it targets all the SQL Linked servers created within local instance. 
						Multiple server names can be passed as comma (,) separated values
		- @UpdateType: It is Mandatory, accepts integer value only, following are the acceptable values 
				1 = UPDATE ONLY CLASSIFICATIONS
				2 = UPDATE CLASSIFICATIONS + SCHEMA Details
	Example: 
		exec [dbo].[collect_catalogdetails] @UpdateType = 1; ---> All Linked servers, updates classifications only 
		exec [dbo].[collect_catalogdetails] @UpdateType = 2; ---> All Linked servers, updates classifications only 
		exec [dbo].[collect_catalogdetails] @ServerName = ''G42PR-SQLAGERP'', @UpdateType = 2; ---> One Linked servers, updates all details
		'
	END

ELSE 
BEGIN 

-- Validate and run if it's only primary replica ---------------------------------------------------------------------------------------
Declare @IsHADR bit;
	select @IsHADR = Cast(SERVERPROPERTY ('Ishadrenabled') as int);
	if @IsHADR = 0 or exists (	select r.replica_id 
								from sys.dm_hadr_availability_replica_states r
								join sys.availability_groups ag on r.group_id = ag.group_id
								join sys.availability_databases_cluster agc on ag.group_id = agc.group_id and database_name = DB_NAME()
								where is_local =1 and role = 1)
-----------------------------------------------------------------------------------------------------------------------------------------
BEGIN 

	IF (Select OBJECT_ID ('tempdb..#TargetServers')) is null 
	BEGIN
		Create table #TargetServers (ID int, LinkServer nvarchar(128));
	END

	--- Output table temp
	IF (SELECT OBJECT_ID ('tempdb..#data_catalog') ) IS NULL 
		BEGIN 
		CREATE TABLE #data_catalog(
			[instancename] [nvarchar](128) NULL,
			[databasename] [nvarchar](128) NULL,
			[objectid] [int] NULL,
			[table_schema] [nvarchar](128) NULL,
			[table_name] [sysname] NOT NULL,
			[column_name] [sysname] NULL,
			[ordinal_position] [int] NULL,
			[data_type] [nvarchar](128) NULL,
			[is_nullable] [varchar](3) NULL,
			[column_default] [nvarchar](4000) NULL,
			[length] [int] NULL,
			[precision] [smallint] NULL,
			[collation_name] [sysname] NULL,
			[classify_info_type] nvarchar(4000) NULL,
			[classify_label] nvarchar(4000) NULL,
			[classify_rank] [varchar](8) NULL,
			created_on datetime2(2) DEFAULT GETDATE(),
			modified_on datetime2(2) DEFAULT GETDATE(),
			is_deleted bit
		) ;
		END 

	--- Output table persistant
	IF (SELECT OBJECT_ID ('data_catalog') ) IS NULL 
		BEGIN 
		CREATE TABLE [dbo].[data_catalog](
			[instancename] [nvarchar](128) NULL,
			[databasename] [nvarchar](128) NULL,
			[objectid] [int] NULL,
			[table_schema] [nvarchar](128) NULL,
			[table_name] [sysname] NOT NULL,
			[column_name] [sysname] NULL,
			[ordinal_position] [int] NULL,
			[data_type] [nvarchar](128) NULL,
			[is_nullable] [varchar](3) NULL,
			[column_default] [nvarchar](4000) NULL,
			[length] [int] NULL,
			[precision] [smallint] NULL,
			[collation_name] [sysname] NULL,
			[classify_info_type] nvarchar(4000) NULL,
			[classify_label] nvarchar(4000) NULL,
			[classify_rank] [varchar](8) NULL,
			created_on datetime2(2) DEFAULT GETDATE(),
			modified_on datetime2(2) DEFAULT GETDATE(),
			is_deleted bit
		) ;
		CREATE CLUSTERED COLUMNSTORE INDEX cci_data_catalog on [data_catalog];
		END 

	--- Variables within procedure 
	Declare @TargetDb table (DBName varchar (126) );
	Declare @DBName nvarchar(128);
	Declare 
			@TSQL_TblInfo nvarchar (4000),
			@TSQL_ColumnInfo nvarchar (4000),
			@TSQL_Classify nvarchar (4000),
			@TSQL_Params nvarchar (4000),

			@isPrimaryReplica bit;


	Print Format (getdate(), 'yyyy-MMM-dd HH:mm:ss')+ ' TempTable Created; ';

		SET @TSQL_LinkServer = 	'''' + 
					'select	  [instancename] 
							, [databasename] 
							, [objectid]
							, [table_schema] 
							, [table_name] 
							, [column_name]
							, [ordinal_position] 
							, [data_type] 
							, [is_nullable]
							, [column_default]
							, [length]
							, [precision]
							, [collation_name]
							, [classify_info_type]
							, [classify_label]
							, [classify_rank]
							, is_deleted
						from DBAClient.dbo.data_catalog
						where is_deleted = 0'
								+ ''')' ;

		SET @TSQL_Local = 
						'select	  [instancename] 
							, [databasename] 
							, [objectid]
							, [table_schema] 
							, [table_name] 
							, [column_name]
							, [ordinal_position] 
							, [data_type] 
							, [is_nullable]
							, [column_default]
							, [length]
							, [precision]
							, [collation_name]
							, [classify_info_type]
							, [classify_label]
							, [classify_rank]
							, is_deleted
						from DBAClient.dbo.data_catalog
						where is_deleted = 0';

	---------------------------------------------------------------------------------------------------------
	-- Collecting data from local instance
	---------------------------------------------------------------------------------------------------------
	IF @ServerName is null 
		BEGIN 
			insert into data_catalog ([instancename], [databasename], [objectid], [table_schema], [table_name], [column_name], [ordinal_position], [data_type], [is_nullable], [column_default], [length], [precision], [collation_name], [classify_info_type], [classify_label], [classify_rank], is_deleted)
			Exec (@TSQL_Local);
			Print Format (getdate(), 'yyyy-MMM-dd HH:mm:ss')+ ' Local Server data loaded into TempTable; ' + ' RowsEffected: ' + cast(@@ROWCOUNT as varchar(10));
		END 
	---------------------------------------------------------------------------------------------------------
	-- Calling linked server procedure to process "extended events data" locally on every linked server  
	---------------------------------------------------------------------------------------------------------
		--- Set Target servers: 
		IF @ServerName is not null 
			BEGIN 
				Insert into #TargetServers 
				select server_id, name 
				from sys.servers
				where name in (select value from string_split(@ServerName, ',') );
			END
		ELSE 
			BEGIN 
				Insert into #TargetServers 
				select server_id, name 
				from sys.servers
				where is_linked = 1  and (provider = 'SQLNCLI' or provider = 'MSOLEDBSQL');
			END 
	
		--- Run Loop through target servers: 
	
			While exists (select * from #TargetServers) 
				BEGIN
					BEGIN TRY 
					SET @LinkedServer = (select top 1 LinkServer from #TargetServers order by ID);
					SET @OPENQUERY = 'SELECT * FROM OPENQUERY(['+ @LinkedServer + '],';

					-- Inserting linked Server data into Temp Table 
							Insert into #data_catalog ([instancename], [databasename], [objectid], [table_schema], [table_name], [column_name], [ordinal_position], [data_type], [is_nullable], [column_default], [length], [precision], [collation_name], [classify_info_type], [classify_label], [classify_rank], is_deleted)
							EXEC (@OPENQUERY+@TSQL_LinkServer);

					Print Format (getdate(), 'yyyy-MMM-dd HH:mm:ss')+ ' Linked Server ['+ @LinkedServer +'] data loaded into TempTable; ' + ' RowsEffected: ' + cast(@@ROWCOUNT as varchar(10));
					END TRY 

					BEGIN CATCH
						Print FORMAT(GETDATE(), 'yyyy-MMM-dd HH:mm:ss') +  ' ERROR While reading data from Linked Server ['+ @LinkedServer +'] ; ';				
						INSERT INTO dbo.error_log_dc (process_name,object_ref, error_msg)
						SELECT CAST(OBJECT_NAME(@@PROCID) AS nvarchar(200)), @LinkedServer, ERROR_MESSAGE();
					END CATCH 

					Delete from #TargetServers where LinkServer = @LinkedServer;
				END --- Target servers Loop End! 

				--- Inserting New Records ==========================================================================================
					insert into data_catalog ([instancename], [databasename], [objectid], [table_schema], [table_name], [column_name], [ordinal_position], [data_type], [is_nullable], [column_default], [length], [precision], [collation_name], [classify_info_type], [classify_label], [classify_rank], is_deleted)
					select	  [instancename]
							, [databasename]
							, [objectid]
							, [table_schema]
							, [table_name]
							, [column_name]
							, [ordinal_position] 
							, [data_type]
							, [is_nullable]
							, [column_default]
							, [length]
							, [precision]
							, [collation_name]
							, [classify_info_type]
							, [classify_label]
							, [classify_rank]
							, is_deleted
					from #data_catalog as t 
					WHERE NOT EXISTS (SELECT 1 as a FROM data_catalog as t2 
										WHERE t.[instancename] collate SQL_Latin1_General_CP1_CI_AS = t2.instancename and 
										t.[databasename] collate SQL_Latin1_General_CP1_CI_AS = t2.databasename and 
										t.[objectid] = t2.objectid and 
										t.[column_name] collate SQL_Latin1_General_CP1_CI_AS = t2.column_name
										);
					Print Format (getdate(), 'yyyy-MMM-dd HH:mm:ss')+ ' Data loaded into destination table; ' + ' RowsEffected: ' + cast(@@ROWCOUNT as varchar(10));


				--- Updating existing details ==========================================================================================
					IF @UpdateType = 2
						BEGIN 
							with dtl as (
								select	  t.[instancename] 
										, t.[databasename] 
										, t.[objectid] 
										, t.[table_schema] 
										, t.[table_name] 
										, t.[column_name]
										, t.[ordinal_position] 
										, [data_type] 
										, [is_nullable]
										, [column_default] 
										, [length]
										, [precision]
										, [collation_name] 
										, [classify_info_type] 
										, [classify_label]
										, [classify_rank]
								from #data_catalog as t 
							)

							update t2 set 
								 [data_type] = t1.data_type
								,[is_nullable] = t1.[is_nullable]
								,[column_default] = t1.[column_default]
								,[length] = t1.[length]
								,[precision] = t1.[precision]
								,[collation_name] = t1.[collation_name]
								,[classify_info_type] = t1.[classify_info_type]
								,[classify_label] = t1.[classify_label]
								,[classify_rank] = t1.[classify_rank]
								,modified_on = GETDATE()
							from dtl as t1
							join data_catalog as t2 on t1.[instancename] = t2.instancename and t1.[databasename] = t2.databasename and t1.[objectid] = t2.objectid and t1.[column_name] = t2.column_name;

							Print Format (getdate(), 'yyyy-MMM-dd HH:mm:ss')+ ' TYPE2 data UPDATED in destination table; ' + ' RowsEffected: ' + cast(@@ROWCOUNT as varchar(10));
						END 

					IF @UpdateType = 1
						BEGIN 
							with dtl as (
								select	  t.[instancename] 
										, t.[databasename] 
										, t.[objectid] 
										, t.[table_schema] 
										, t.[table_name] 
										, t.[column_name]
										, t.[ordinal_position] 
										, [data_type] 
										, [is_nullable]
										, [column_default] 
										, [length]
										, [precision]
										, [collation_name] 
										, [classify_info_type] 
										, [classify_label]
										, [classify_rank]
								from #data_catalog as t 
							)

							update t2 set 
								 [classify_info_type] = t1.[classify_info_type]
								,[classify_label] = t1.[classify_label]
								,[classify_rank] = t1.[classify_rank]
								,modified_on = GETDATE()
							from dtl as t1
							join dbo.data_catalog as t2 on 
								t1.[instancename] collate SQL_Latin1_General_CP1_CI_AS = t2.instancename and 
								t1.[databasename] collate SQL_Latin1_General_CP1_CI_AS = t2.databasename and 
								t1.[objectid] = t2.objectid and 
								t1.[column_name] collate SQL_Latin1_General_CP1_CI_AS = t2.column_name;

							Print Format (getdate(), 'yyyy-MMM-dd HH:mm:ss')+ ' TYPE1 data UPDATED in destination table; ' + ' RowsEffected: ' + cast(@@ROWCOUNT as varchar(10));
						END 

				--- Updating Deleted Records ==========================================================================================
						BEGIN 
							update t1 set 
								 is_deleted = 1
								,modified_on = GETDATE()
							from dbo.data_catalog as t1
							where not exists (	select 1 as a 
												from #data_catalog as t2 
												where 
													t1.[instancename]  collate SQL_Latin1_General_CP1_CI_AS = t2.instancename and 
													t1.[databasename]  collate SQL_Latin1_General_CP1_CI_AS = t2.databasename and 
													t1.[objectid] = t2.objectid and 
													t1.[column_name]  collate SQL_Latin1_General_CP1_CI_AS = t2.column_name
												)
						END
	END
	END
END
GO 


[dbo].[collect_catalogdetails] 


exec [dbo].[collect_catalogdetails] @UpdateType = 2; ---> One Linked servers, updates all details

select * from error_log_dc order by log_datetime desc;

select * from data_catalog where databasename = 'AXDB';



---Debug--------------------------------------------

DECLARE @OPENQUERY nvarchar(4000), 
		@TSQL_LinkServer nvarchar(4000), 
		@TSQL_Local nvarchar(4000), 
		@LinkedServer nvarchar(128);

SET @LinkedServer = 'G42PR-SQLAGERP';

SET @OPENQUERY = 'SELECT * FROM OPENQUERY(['+ @LinkedServer + '],';

SET @TSQL_LinkServer = 	'''' + 
					'select	  [instancename] 
							, [databasename] 
							, [objectid]
							, [table_schema] 
							, [table_name] 
							, [column_name]
							, [ordinal_position] 
							, [data_type] 
							, [is_nullable]
							, [column_default]
							, [length]
							, [precision]
							, [collation_name]
							, [classify_info_type]
							, [classify_label]
							, [classify_rank]
							, is_deleted
						from DBAClient.dbo.data_catalog
						where is_deleted = 0'
								+ ''')' ;

exec (@OPENQUERY+@TSQL_LinkServer)