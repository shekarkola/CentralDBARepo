USE [DBAClient]
GO
/****** Object:  StoredProcedure [dbo].[get_tables]    Script Date: 10/28/2024 9:44:38 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER   PROCEDURE [dbo].[get_tables]
		@DBName varchar (128) = null,
		@Mode tinyint = 1,
		@Help bit = 0
AS
BEGIN

SET NOCOUNT ON;

IF @Help = 1 
PRINT 
'---------------------------------------------------------------------------------------------------------------------	
Parameters:
	@DBName		Accepts database name within SQL Instance, data type "VARCHAR (128)", Default is current database 
	@Mode		1 = Tables with data, 0 = All User Tables. Data type "tinyint", Default is "1"

	@Help		When "1" It will print the message that you''re reading now! Default is "0"
-----------------------------------------------------------------------------------------------------------------------';

ELSE BEGIN
	
	Declare @TSQL nvarchar(max);

	IF @DBName is null 
		begin
			SET @DBName = CAST (DB_NAME() as varchar(128));
		end

	IF (SELECT OBJECT_ID ('tempdb..#tbl_details') ) IS NOT NULL
		BEGIN
			DROP TABLE #tbl_details;
		END

	CREATE TABLE [dbo].#tbl_details
	(
		[database_name] [varchar](128) NULL,
		[schema_name] [nvarchar](128) NULL,
		[table_name] [sysname] NOT NULL,
		[row_count] [bigint] NULL,
		[totalsize_mb] [numeric](16, 2) NULL,
		[totalsize_gb] [numeric](16, 2) NULL,
		[usedsize_mb] [numeric](16, 2) NULL,
		[usedsize_gb] [numeric](16, 2) NULL
	);

IF @Mode = 1 
	begin 
		set @TSQL = 
			'USE ' + QUOTENAME(@DBName) + ';
				select 		cast (db_name () as varchar(128)) [database_name],
							schema_name(t.schema_id) as schema_name,
							t.name as table_name,
							(p.rows) as row_count,
							CAST( sum ((a.total_pages * 8.00) /1024) as numeric(16,2)) totalsize_mb,
							CAST( sum ((a.total_pages * 8.00) /1048576.0) as numeric(16,2)) totalsize_gb,
							CAST( sum ( (a.used_pages * 8.00) /1024) as numeric(16,2)) usedsize_mb,
							CAST( sum ( (a.used_pages * 8.00) /1048576.0) as numeric(16,2)) usedsize_gb
				from  
						sys.tables as t
					inner join      
						sys.indexes i on  t.object_id = i.object_id
					inner join 
						sys.partitions p on i.object_id = p.object_id and i.index_id = p.index_id
					inner join 
						sys.allocation_units a on p.partition_id = a.container_id
					left outer join 
						sys.schemas s on t.schema_id = s.schema_id
				where t.object_id > 255 and p.rows > 0 
				group by schema_name(t.schema_id), t.name, p.rows--, a.type_desc
				order by totalsize_mb desc, p.rows desc
				';
		print @TSQL;

		insert into #tbl_details
		exec sp_executesql @TSQL;
		
		Print 'Database: ' + @DBName +  '. RowsEffected: ' + cast(@@ROWCOUNT as varchar(10)) + '. Mode = 1';
		
	end 

IF @Mode = 0 
	begin 
			insert into #tbl_details
			exec (
			'USE [' + @DBName + '];
				select 		cast (db_name () as varchar(128)) [database_name],
							schema_name(t.schema_id) as schema_name,
							t.name as table_name,
							(p.rows) as row_count,
							CAST( sum ((a.total_pages * 8.00) /1024) as numeric(16,2)) totalsize_mb,
							CAST( sum ((a.total_pages * 8.00) /1048576.0) as numeric(16,2)) totalsize_gb,
							CAST( sum ( (a.used_pages * 8.00) /1024) as numeric(16,2)) usedsize_mb,
							CAST( sum ( (a.used_pages * 8.00) /1048576.0) as numeric(16,2)) usedsize_gb
				from  
						sys.tables as t
					inner join      
						sys.indexes i on  t.object_id = i.object_id
					inner join 
						sys.partitions p on i.object_id = p.object_id and i.index_id = p.index_id
					inner join 
						sys.allocation_units a on p.partition_id = a.container_id
					left outer join 
						sys.schemas s on t.schema_id = s.schema_id
				where t.object_id > 255 
				group by schema_name(t.schema_id), t.name, p.rows--, a.type_desc
				order by totalsize_mb desc, p.rows desc
				'
			);
			Print 'Database: ' + QUOTENAME(@DBName) +  '. RowsEffected: ' + cast(@@ROWCOUNT as varchar(10))  + '. Mode = 0';
			
	END
	SELECT
		[database_name],
		[schema_name] ,
		[table_name] ,
		FORMAT([row_count], '#,###') [row_count] ,
		[totalsize_mb] ,
		[totalsize_gb] ,
		[usedsize_mb] ,
		[usedsize_gb] 
	FROM #tbl_details
	END
END
GO 



use FSIEvolution
go 

select OBJECT_NAME(object_id), OBJECT_NAME(referenced_major_id),* from sys.sql_dependencies;

select * from sys.sql_modules;
select * from sys.tables 

select * from sys.dm_sql_referenced_entities ();


select OBJECT_NAME(referenced_id),* from sys.sql_expression_dependencies;

set statistics io, time on;

select OBJECT_NAME(major_id) as table_name, COUNT (1) as Classifications 
from sys.sensitivity_classifications as t1
group by major_id
;

select * from INFORMATION_SCHEMA.COLUMNS as c

select  @@SERVERNAME as sinstance_name
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


select t2.name, t1.* 
from sys.sensitivity_classifications as t1
join sys.tables as t2 on t1.major_id = t2.object_id
;
go 


use FSIReporting
go

select	@@SERVERNAME as sql_servername, DB_NAME() as dbname
		, OBJECT_NAME(referencing_id) as objectname
		, ISNULL(referenced_database_name, DB_NAME() ) as referenced_dbname
		, ISNULL(referenced_schema_name, SCHEMA_NAME (referencing_id)) referenced_schema_name
		--, ISNULL(refre, SCHEMA_NAME (referencing_id)) referenced_object_name
		,* 
from sys.sql_expression_dependencies
where referencing_id = OBJECT_ID('pbi_contract_inv_dtl');
go 


select * from INFORMATION_SCHEMA.VIEW_COLUMN_USAGE

SELECT
    sys.objects.object_id,
    sys.schemas.name AS [Schema], 
    sys.objects.name AS Object_Name, 
    sys.objects.type_desc AS [Type]
FROM sys.sql_modules (NOLOCK) 
INNER JOIN sys.objects (NOLOCK) ON sys.sql_modules.object_id = sys.objects.object_id 
INNER JOIN sys.schemas (NOLOCK) ON sys.objects.schema_id = sys.schemas.schema_id