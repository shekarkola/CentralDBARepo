use AXDB
go 

select  @@SERVERNAME as instance_name
		,TABLE_CATALOG as databasename
		,TABLE_NAME as table_name
		,COLUMN_NAME as column_name
		,ORDINAL_POSITION as ordinal_position
		,DATA_TYPE
		,IS_NULLABLE as is_nullable
		,COLUMN_DEFAULT as column_default
		,COALESCE(CHARACTER_MAXIMUM_LENGTH, NUMERIC_SCALE) AS length
		,COALESCE(NUMERIC_PRECISION, DATETIME_PRECISION) AS precision
		,ISNULL(COLLATION_NAME, '') as collation_name
		,sc.information_type as classify_info_type
		,sc.label as classify_label
		,sc.rank_desc as classify_rank
from INFORMATION_SCHEMA.COLUMNS as c
left join sys.sensitivity_classifications as sc 
		on OBJECT_ID(TABLE_CATALOG + '.' + TABLE_SCHEMA + '.'+TABLE_NAME )= sc.major_id
		and COL_NAME(sc.major_id, sc.minor_id) = c.COLUMN_NAME
		;

select  @@SERVERNAME as instance_name
		,TABLE_CATALOG as databasename
		,TABLE_NAME as table_name
		,COLUMN_NAME as column_name
		,ORDINAL_POSITION as ordinal_position
		,DATA_TYPE
		,IS_NULLABLE as is_nullable
		,COLUMN_DEFAULT as column_default
		,COALESCE(CHARACTER_MAXIMUM_LENGTH, NUMERIC_SCALE) AS length
		,COALESCE(NUMERIC_PRECISION, DATETIME_PRECISION) AS precision
		,ISNULL(COLLATION_NAME, '') as collation_name
into temp_column_info
from INFORMATION_SCHEMA.COLUMNS as c


select  @@SERVERNAME as instancename
		,@DatabaseName as dbname 
		,sc.major_id as obejctid
		,sc.minor_id as columnid
		,COL_NAME(sc.major_id, sc.minor_id) as column_name
		,sc.information_type as classify_info_type
		,sc.label as classify_label
		,sc.rank_desc as classify_rank
from sys.sensitivity_classifications as sc 
go 

select OBJECT_SCHEMA_NAME(105622944, DB_ID('AXDB'))

USE [DBAClient]
GO

-- DROP TABLE temp_column_info

CREATE TABLE [dbo].[temp_column_info]
(   -- RECID INT IDENTITY (1,1) PRIMARY KEY,
	[instance_name] [nvarchar](128),
	[databasename] [nvarchar](128),
	[table_name] [sysname] NOT NULL,
	[column_name] [sysname],
	[ordinal_position] [int] NULL,
	[data_type] [nvarchar](128) NULL,
	[is_nullable] [varchar](3) NULL,
	[column_default] [nvarchar](4000) NULL,
	[length] [int] NULL,
	[precision] [smallint] NULL,
	[collation_name] [sysname] NOT NULL,
	PRIMARY KEY NONCLUSTERED ([instance_name], [databasename], [table_name], [column_name])
)  

-- WITH (MEMORY_OPTIMIZED=ON, DURABILITY=SCHEMA_ONLY)



select  @@SERVERNAME as instance_name
		,TABLE_CATALOG as databasename
		,TABLE_NAME as table_name
		,COLUMN_NAME as column_name
		,ORDINAL_POSITION as ordinal_position
		,DATA_TYPE
		,IS_NULLABLE as is_nullable
		,COLUMN_DEFAULT as column_default
		,COALESCE(CHARACTER_MAXIMUM_LENGTH, NUMERIC_SCALE) AS length
		,COALESCE(NUMERIC_PRECISION, DATETIME_PRECISION) AS precision
		,ISNULL(COLLATION_NAME, '') as collation_name
		--,sc.information_type as classify_info_type
		--,sc.label as classify_label
		--,sc.rank_desc as classify_rank
from INFORMATION_SCHEMA.COLUMNS as c
go 

select 		@@SERVERNAME as instancename,
			cast (db_name () as varchar(128)) [database_name],
			schema_name(t.schema_id) as schema_name,
			t.name as table_name,
			(p.rows) as row_count
into #table_info 
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




use DBAClient
go 

---> Procedure Parameters: 
Declare @DatabaseName varchar (8000);
Declare @ExcludeDatabases nvarchar(4000);

--- Variables within procedure 
Declare @TargetDb table (DBName varchar (126) );

Declare @DBName nvarchar(128);
Declare 
		@TSQL_TblInfo nvarchar (4000),
		@TSQL_ColumnInfo nvarchar (4000),
		@TSQL_Classify nvarchar (4000),
		@TSQL_Params nvarchar (4000),

		@isPrimaryReplica bit;

	IF @DatabaseName is null and @ExcludeDatabases is not null 
		begin
			Insert into @TargetDb
			select	name 
			from	sys.databases 
			where	database_id > 4  and is_read_only = 0
					and name not in (select value from string_split(@ExcludeDatabases, ',') )
		end

	IF @DatabaseName is null and @ExcludeDatabases is null 
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


----> DROP Temp tables ;
DROP TABLE IF EXISTS temp_table_info;
DROP TABLE IF EXISTS temp_column_info;
DROP TABLE IF EXISTS temp_classify_info;

--- select * from @TargetDb;

while exists (select 1 from @TargetDb)
	begin 
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
						into temp_table_info 
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
							into temp_column_info
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
									,sc.information_type as classify_info_type
									,sc.label as classify_label
									,sc.rank_desc as classify_rank
							into temp_classify_info
							from sys.sensitivity_classifications as sc 
						';
				-- print @TSQL_ColumnInfo;
				exec sp_executesql @TSQL_Classify, @TSQL_Params, @DBName = @DBName;  ---> left side @DBName is declared parameter within Dynamic SQL, while the right side one is current session parameter
				Print 'Classificatoins info loaded for Database: ' + QUOTENAME(@DBName) +  '. RowsEffected: ' + cast(@@ROWCOUNT as varchar(10));
			END
		
		delete from @TargetDb where DBName = @DBName;
	end 

--truncate table temp_column_details;

--insert into temp_column_details([instancename], [databasename], [objectid], [table_schema], [table_name], [column_name], [ordinal_position], [data_type], [is_nullable], [column_default], [length], [precision], [collation_name], [classify_info_type], [classify_label], [classify_rank])
select	  c.[instancename], c.[databasename], c.[objectid], c.[table_schema], c.[table_name], c.[column_name]
		, c.[ordinal_position], [data_type], [is_nullable], [column_default], [length], [precision]
		, [collation_name], [classify_info_type], [classify_label], [classify_rank]
from temp_table_info as t 
join temp_column_info as c on
		t.instancename = c.instancename and 
		t.databasename = c.databasename and  
		t.table_schema = c.table_schema and 
		t.table_name = c.table_name
left join temp_classify_info as ci on 
		c.instancename = ci.instancename and 
		c.objectid = ci.objectid and 
		c.column_name = ci.column_name;

go



select * from [dbo].temp_column_details





select * from sys.dm_hadr_availability_group_states;



select dc.database_name, dr.database_id, dr.synchronization_state_desc,   
dr.suspend_reason_desc, dr.synchronization_health_desc  
from sys.dm_hadr_database_replica_states dr  
join sys.availability_databases_cluster dc  
on dr.group_database_id=dc.group_database_id   
where is_local=1  
go
