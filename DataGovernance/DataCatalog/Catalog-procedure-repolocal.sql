USE [DBAClient]
GO

-- =========================================================
-- Author:			Shekar Kola
-- Create date:		2024-11-13
-- Modified date:	2024-11-15
-- Description:		Data collection for Data Catalog
-- =========================================================
CREATE OR ALTER PROCEDURE [dbo].[collect_catalogdetails]  
	@DatabaseName nvarchar (4000) = null,
	@ExcludeDatabases nvarchar(4000) = null,
	@UpdateType tinyint = null
	--@Help bit = 1
		
AS
BEGIN

	SET NOCOUNT ON;

IF @UpdateType IS NULL 
	BEGIN 
	print 'This procedure developed to collect the column level details including data classificaiton information for the data-catalog preparation, use following details and examples to execute it.
	
	Parameters: 
		- @DatabaseName: Accepts the database name, multiple database names can be passed with comma (,) separation. When it is NULL all the users databases considered as target.
		- @UpdateType: It is Mandatory, accepts integer value only, following are the acceptable values 
				1 = UPDATE ONLY CLASSIFICATIONS
				2 = UPDATE CLASSIFICATIONS + SCHEMA Details
	Example: 
		exec [dbo].[collect_catalogdetails] @DatabaseName = ''AXDB,AXDBReporting'', @UpdateType = 1; 
		'
	END 
ELSE 
BEGIN 

--- Output table
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
		[collation_name] NVARCHAR(100) NULL,
		[classify_info_type] NVARCHAR(4000) NULL,
		[classify_label] NVARCHAR(4000) NULL,
		[classify_rank] [varchar](8) NULL,
		created_on datetime2(2) DEFAULT GETDATE(),
		modified_on datetime2(2) DEFAULT GETDATE(),
		is_deleted bit
	) ;
	CREATE CLUSTERED INDEX ci_data_catalog on [data_catalog] ([instancename], [databasename], [objectid], [column_name]);
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


	IF @DatabaseName is null 
		begin
			Insert into @TargetDb
			select	name 
			from	sys.databases 
			where	database_id > 4 and is_read_only = 0
		end

	IF @DatabaseName is not null 
		begin
			Insert into @TargetDb
			select	value 
			from	string_split(@DatabaseName, ',')
		end

	IF @ExcludeDatabases is not null 
		begin
			delete from @TargetDb 
			where DBName in (
							select	value 
							from	string_split(@ExcludeDatabases, ',')
							)
		end

--- select * from @TargetDb;

while exists (select 1 from @TargetDb)
	begin 
		----> DROP Temp tables ;
		DROP TABLE IF EXISTS temp_table_info;
		DROP TABLE IF EXISTS temp_column_info;
		DROP TABLE IF EXISTS temp_classify_info;

		set @DBName = (select top 1 DBName from @TargetDb);
		set @TSQL_Params = '@DBName varchar(128)';
		
		IF EXISTS (select db.name
					from sys.databases as db 
						left join sys.dm_hadr_database_replica_states as hadr on hadr.group_database_id = db.group_database_id
					where db.name = @DBName and ( (is_primary_replica = 1) or db.group_database_id is null ) 
					)
			SET @isPrimaryReplica = 1 
		ELSE 
			SET @isPrimaryReplica = 0 

		IF @isPrimaryReplica = 1 

			BEGIN
				Print 'Begin for the database ' + QUOTENAME(@DBName);

				set @TSQL_TblInfo = 
					'USE ' + QUOTENAME(@DBName) + ';
						select 		@@SERVERNAME as instancename,
									cast (db_name () as varchar(128)) [databasename],
									schema_name(t.schema_id) as table_schema,
									t.name as table_name,
									(p.rows) as row_count
						into DBAClient.dbo.temp_table_info 
						from  
								sys.tables as t
							inner join      
								sys.indexes i on  t.object_id = i.object_id
							inner join 
								sys.partitions p on i.object_id = p.object_id and i.index_id = p.index_id
							left outer join 
								sys.schemas s on t.schema_id = s.schema_id
						where t.object_id > 255 and p.rows > 0 
						group by t.schema_id, t.name, p.rows
						';
				-- print @TSQL_TblInfo;
				exec (@TSQL_TblInfo);

				Print 'Table Info loaded for Database: ' + QUOTENAME(@DBName) +  '. RowsEffected: ' + cast(@@ROWCOUNT as varchar(10));
				---------------------------------------------------------------

				set @TSQL_ColumnInfo = 
					'USE ' + QUOTENAME(@DBName) + ';
							select  @@SERVERNAME as instancename
									,TABLE_CATALOG as databasename
									,OBJECT_ID(TABLE_CATALOG + ''.'' + TABLE_SCHEMA + ''.''+TABLE_NAME ) as objectid
									,TABLE_SCHEMA as table_schema
									,TABLE_NAME as table_name
									,COLUMN_NAME as column_name
									,ORDINAL_POSITION as ordinal_position
									,DATA_TYPE as data_type
									,IS_NULLABLE as is_nullable
									,COLUMN_DEFAULT as column_default
									,COALESCE(CHARACTER_MAXIMUM_LENGTH, NUMERIC_SCALE) AS length
									,COALESCE(NUMERIC_PRECISION, DATETIME_PRECISION) AS precision
									,COLLATION_NAME as collation_name
							into DBAClient.dbo.temp_column_info
							from INFORMATION_SCHEMA.COLUMNS as c
						';
				-- print @TSQL_ColumnInfo;
				exec (@TSQL_ColumnInfo);
				Print 'Column info loaded for Database: ' + QUOTENAME(@DBName) +  '. RowsEffected: ' + cast(@@ROWCOUNT as varchar(10));

				---------------------------------------------------------------

				set @TSQL_Classify = 
					'USE ' + QUOTENAME(@DBName) + ';
							select  @@SERVERNAME as instancename
									,@DBName as dbname 
									,sc.major_id as objectid
									,sc.minor_id as columnid
									,COL_NAME(sc.major_id, sc.minor_id) as column_name
									,TRY_CAST(sc.information_type AS NVARCHAR(4000)) as classify_info_type
									,TRY_CAST(sc.label as NVARCHAR(4000)) as classify_label
									,sc.rank_desc as classify_rank
							into DBAClient.dbo.temp_classify_info
							from sys.sensitivity_classifications as sc 
						';
				-- print @TSQL_ColumnInfo;
				exec sp_executesql @TSQL_Classify, @TSQL_Params, @DBName = @DBName;  ---> left side @DBName is declared parameter within Dynamic SQL, while the right side one is current session parameter
				Print 'Classificatoins info loaded for Database: ' + QUOTENAME(@DBName) +  '. RowsEffected: ' + cast(@@ROWCOUNT as varchar(10));

			--- Inserting New Records ==========================================================================================
				insert into data_catalog ([instancename], [databasename], [objectid], [table_schema], [table_name], [column_name], [ordinal_position], [data_type], [is_nullable], [column_default], [length], [precision], [collation_name], [classify_info_type], [classify_label], [classify_rank], is_deleted)
				select	  c.[instancename] collate SQL_Latin1_General_CP1_CI_AS
						, c.[databasename] collate SQL_Latin1_General_CP1_CI_AS
						, c.[objectid]
						, c.[table_schema] collate SQL_Latin1_General_CP1_CI_AS
						, c.[table_name] collate SQL_Latin1_General_CP1_CI_AS
						, c.[column_name] collate SQL_Latin1_General_CP1_CI_AS
						, c.[ordinal_position] 
						, [data_type] collate SQL_Latin1_General_CP1_CI_AS
						, [is_nullable]
						, [column_default] collate SQL_Latin1_General_CP1_CI_AS
						, [length]
						, [precision]
						, [collation_name] collate SQL_Latin1_General_CP1_CI_AS
						, [classify_info_type]
						, [classify_label]
						, [classify_rank]
						, 0 as is_deleted
				from dbo.temp_table_info as t 
				join dbo.temp_column_info as c on
						t.instancename = c.instancename and 
						t.databasename = c.databasename and  
						t.table_schema = c.table_schema and 
						t.table_name = c.table_name
				left join dbo.temp_classify_info as ci on 
						c.instancename = ci.instancename and 
						c.objectid = ci.objectid and 
						c.column_name = ci.column_name
				WHERE NOT EXISTS (SELECT 1 as a FROM data_catalog as t2 
									WHERE c.[instancename] collate SQL_Latin1_General_CP1_CI_AS = t2.instancename and 
									c.[databasename] collate SQL_Latin1_General_CP1_CI_AS = t2.databasename and 
									c.[objectid] = t2.objectid and 
									c.[column_name] collate SQL_Latin1_General_CP1_CI_AS = t2.column_name
									);


			--- Updating existing details ==========================================================================================
				IF @UpdateType = 2
					BEGIN 
						with dtl as (
							select	  c.[instancename] collate SQL_Latin1_General_CP1_CI_AS as [instancename]
									, c.[databasename] collate SQL_Latin1_General_CP1_CI_AS as [databasename]
									, c.[objectid] 
									, c.[table_schema] collate SQL_Latin1_General_CP1_CI_AS as [table_schema]
									, c.[table_name] collate SQL_Latin1_General_CP1_CI_AS as [table_name]
									, c.[column_name] collate SQL_Latin1_General_CP1_CI_AS as [column_name]
									, c.[ordinal_position] 
									, [data_type] collate SQL_Latin1_General_CP1_CI_AS as [data_type]
									, [is_nullable]
									, [column_default] collate SQL_Latin1_General_CP1_CI_AS as [column_default]
									, [length]
									, [precision]
									, [collation_name] collate SQL_Latin1_General_CP1_CI_AS as [collation_name]
									, [classify_info_type] 
									, [classify_label]
									, [classify_rank]
						from dbo.temp_table_info as t 
						join dbo.temp_column_info as c on
								t.instancename = c.instancename and 
								t.databasename = c.databasename and  
								t.table_schema = c.table_schema and 
								t.table_name = c.table_name
						left join dbo.temp_classify_info as ci on 
								c.instancename = ci.instancename and 
								c.objectid = ci.objectid and 
								c.column_name = ci.column_name
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
						join data_catalog as t2 on t1.[instancename] = t2.instancename and t1.[databasename] = t2.databasename and t1.[objectid] = t2.objectid and t1.[column_name] = t2.column_name
					END 
				IF @UpdateType = 1
					BEGIN 
						with dtl as (
							select	  c.[instancename] 
									, c.[databasename] 
									, c.[objectid] 
									, c.[table_schema] 
									, c.[table_name] 
									, c.[column_name]
									, c.[ordinal_position] 
									, [data_type] 
									, [is_nullable]
									, [column_default] 
									, [length]
									, [precision]
									, [collation_name]
									, [classify_info_type]
									, [classify_label]
									, [classify_rank]
						from dbo.temp_table_info as t 
						join dbo.temp_column_info as c on
								t.instancename = c.instancename and 
								t.databasename = c.databasename and  
								t.table_schema = c.table_schema and 
								t.table_name = c.table_name
						left join dbo.temp_classify_info as ci on 
								c.instancename = ci.instancename and 
								c.objectid = ci.objectid and 
								c.column_name = ci.column_name
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
							t1.[column_name] collate SQL_Latin1_General_CP1_CI_AS = t2.column_name
					END 

			--- Updating Deleted Records ==========================================================================================

					BEGIN 
						update t1 set 
							 is_deleted = 1
							,modified_on = GETDATE()
						from dbo.data_catalog as t1
						where t1.databasename = @DBName
						and not exists (	select 1 as a 
											from dbo.temp_column_info as t2 
											where 
												t1.[instancename]  collate SQL_Latin1_General_CP1_CI_AS = t2.instancename and 
												t1.[databasename]  collate SQL_Latin1_General_CP1_CI_AS = t2.databasename and 
												t1.[objectid] = t2.objectid and 
												t1.[column_name]  collate SQL_Latin1_General_CP1_CI_AS = t2.column_name
											)
					END 
			END
		ELSE 
		BEGIN 
			Print 'Database ' + @DBName + ' is part of AG and this replica hosts secondary copy, processes skipped here!'
		END 
		delete from @TargetDb where DBName = @DBName;
	END ---> Loop ends here
END
END 
GO 


[dbo].[collect_catalogdetails] @UpdateType = 2;

