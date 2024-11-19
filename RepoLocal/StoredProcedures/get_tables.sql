USE [DBAClient]
GO
/****** Object:  StoredProcedure [dbo].[get_tables]    Script Date: 10/10/2024 12:02:13 PM ******/
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
