USE [DBAClient]
GO

/*-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Author:			Shekar Kola
-- Create date:		2020-01-27
-- Modified date:	2020-01-27
-- Description:		

Following script from community (MSDN), the recommended approach to REBUILD COLUMNSTORE INDEX is besed on following conditions:
    a. ROWGROUP QUALITY MEASURE: RGQualityMeasure is not met for @PercentageRGQualityPassed Rowgroups
								 This is an arbitrary number, if the average is above this number, dont bother rebuilding as we consider this number to be good quality rowgroups
								 
    b. DELETED ROWS: Second constraint is the Deleted rows, currently the default is 10% of the partition itself. 
					 If the partition is very large or small consider adjusting this.
					 
    c. DICTIONORY NOT FULL: In SQL 2014, post index rebuild, the dmv doesn't show why the RG is trimmed to < 1 million. in this case (in SQL 2014):
						If the Dictionary is full (16MB) then no use in rebuilding this rowgroup as even after rebuild it may get trimmed
						If dictionary is full, consider above point ("b.")
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

CREATE OR ALTER PROCEDURE [dbo].[IndexMaintenance_Columnstore]
		@DBName varchar (128) = null,
		@DeletedRowsPercent Decimal (6,2) = 10,
		@RGQualityMeasure varchar(25) = null,
		@RGQualityMeasure_Percent  Decimal (6,2) = null,
		@Debug int = 0
		
AS
BEGIN
	
	SET NOCOUNT ON;

	Declare @TargetDb table (DBName varchar (128) );
	IF @DBName is null 
	begin
		Insert into @TargetDb
		select	name
		from	sys.databases 
		where	name not in ('master', 'model') and is_read_only = 0 and state = 0
	end
	else 
	begin
		Insert into @TargetDb
		select	@DBName 
	end
	SELECT @@ROWCOUNT as TargetDatabases;

-- Percent of deleted rows for the partition
	Declare @DeletedRowsPercent_Txt varchar(25);
	IF @DeletedRowsPercent IS NULL
		BEGIN
			Set @DeletedRowsPercent_Txt = '10';
		END
	ELSE 
		BEGIN
			select @DeletedRowsPercent_Txt = CAST (@DeletedRowsPercent as varchar(25));
		END
-- RGQuality means, any rowgroup that compressed with "mentioned" number of rows is good row group quality, anything less need to re-evaluated.
	Declare @RGQualityMeasure_Txt varchar(25);
	IF @RGQualityMeasure IS NULL
		BEGIN
			Set @RGQualityMeasure_Txt = '100420';
		END
	ELSE 
		BEGIN
			select @RGQualityMeasure_Txt = CAST (@RGQualityMeasure as varchar(25));
		END
-- Means N% of rowgroups are < @RGQUality from the rows/rowgroup perspective
	Declare @RGQualityMeasure_Percent_Txt varchar(25);
	IF @RGQualityMeasure_Percent IS NULL
		BEGIN
			Set @RGQualityMeasure_Percent_Txt = '20';
		END
	ELSE 
		BEGIN
			select @RGQualityMeasure_Percent_Txt = CAST (@RGQualityMeasure_Percent as varchar(25));
		END
--------------------------------------------------------------------------------------------------------------------------------
if object_id('tempdb..#TempColumnStoreStats') IS NULL
	begin
		Print	CONVERT(VARCHAR(20), GETDATE(),120) + ' Creating TempDB Table'; 
		CREATE TABLE #TempColumnStoreStats
		(   [object_id] [int] NOT NULL,
			[TableName] [nvarchar](128) NULL,
			[index_id] [int] NOT NULL,
			IndexName [nvarchar](128) NULL,
			[partition_number] [int] NOT NULL,
			[Count_RowGroups] [int] NULL,
			[TotalRows] [bigint] NULL,
			[AvgRowsPerRG] [bigint] NULL,
			[UnderQualityMeasure_RowGroups] [int] NULL,
			[RGQualityMeasure] [int] NOT NULL,
			[UnderQualityMeasure_Percent] [decimal](8, 2) NULL,
			[DeletedRowsPercent] [numeric](8, 2) NULL,
			[RowgroupsWithDeletedRows] [int] NULL,
			[maxdictionary_Size] [bigint] NULL,
			[maxdictionary_Entrycount] [bigint] NULL,
			[maxpartition_number] [int] NULL
		);
		CREATE CLUSTERED INDEX CI_TempColumnStoreStats ON #TempColumnStoreStats ([object_id], [index_id], [partition_number]);
	end

	if object_id('tempdb..#CSDictionaries') IS NOT NULL
	begin
		drop table #CSDictionaries;
	end

Declare @is_DB_HADREnabled bit,
		@isPrimaryReplica bit,
		@Databasename varchar(128);

-------------------------------------------------------------------------------------------
While exists (select DBName from @TargetDb)
	BEGIN
		set @Databasename = (select top 1 DBName from @TargetDb);
		-------Verify Database joined AG-----------------------------------------------------------------------------------------------------
		Print	CONVERT(VARCHAR(20), GETDATE(),120) + ' Verifying Database AG...[' + @Databasename + ']'; 
		Select  @is_DB_HADREnabled = IIF(group_database_id IS NULL, 0,1) from sys.databases where [name] = @Databasename;
		BEGIN
			IF EXISTS (select db.name
						from sys.dm_hadr_database_replica_states as hadr
							join sys.databases as db on hadr.group_database_id = db.group_database_id
						where is_local = 1 and is_primary_replica = 1 and db.name = @Databasename
						)
				SET @isPrimaryReplica = 1 
			ELSE 
				SET @isPrimaryReplica = 0 
		END

		IF @is_DB_HADREnabled = 0 or @isPrimaryReplica = 1
	--------Verif AG end --------------------------------------------------------------------------------------------------------------------------
		

	---COLUMN STORE LOOP----------------------------------------------------------------------------------------
		BEGIN
			TRUNCATE TABLE #TempColumnStoreStats;

			Print	CONVERT(VARCHAR(20), GETDATE(),120) + ' Executing Dynamic T-SQL for COLUMNSTORE INDEXES target within [' + @Databasename + ']'; 

			exec 
				('USE [' + @Databasename + 
				'];
				with 
				CSAnalysis AS
				(SELECT		object_id,
							object_name(object_id) as TableName, 
							index_id,
							rg.partition_number,
							count(rg.object_id) as Count_RowGroups, 
							sum(total_rows) as TotalRows, 
							Avg(total_rows) as AvgRowsPerRG,
							SUM(CASE 
									WHEN rg.Total_Rows < ' + @RGQualityMeasure_Txt + 'THEN 1 ELSE 0 
								END
								) as UnderQualityMeasure_RowGroups, '
							 + @RGQualityMeasure_Txt +' as RGQualityMeasure,
							cast((SUM(CASE 
										WHEN rg.Total_Rows < ' + @RGQualityMeasure_Txt + ' THEN 1.0 ELSE 0 
									  END) / ( (count(rg.object_id)) * 100)
								  ) as Decimal(5,2)
								) as UnderQualityMeasure_Percent,
							(Sum(rg.deleted_rows * 1.0) / sum(rg.total_rows * 1.0)) * 100 as DeletedRowsPercent,
							sum (case when rg.deleted_rows > 0 then 1 else 0 end) as RowgroupsWithDeletedRows
				FROM sys.column_store_row_groups rg
				where rg.state = 3  --- Status with "COMPRESSED"
				group by rg.object_id, rg.partition_number,index_id
				),

				CSDictionaries 
					AS
					( select     max(dict.on_disk_size) as maxdictionary_Size
								,max(dict.entry_count) as maxdictionary_Entrycount
								,max(partition_number) as maxpartition_number
								,part.object_id
								,part.partition_number
					from    sys.column_store_dictionaries dict
					join    sys.partitions part 
							on dict.hobt_id = part.hobt_id
					group by part.object_id, part.partition_number
					)


				Insert into #TempColumnStoreStats
						([object_id], 
						  [TableName], 
						  [index_id], 
						  IndexName,
						  [partition_number], 
						  [Count_RowGroups], 
						  [TotalRows], 
						  [AvgRowsPerRG], 
						  [UnderQualityMeasure_RowGroups], 
						  [RGQualityMeasure], 
						  [UnderQualityMeasure_Percent], 
						  [DeletedRowsPercent], 
						  [RowgroupsWithDeletedRows], 
						  [maxdictionary_Size], 
						  [maxdictionary_Entrycount], 
						  [maxpartition_number])
				select  a.[object_id], 
						  [TableName], 
						a.[index_id], 
						  i.name,
						a.[partition_number], 
						  [Count_RowGroups], 
						  [TotalRows], 
						  [AvgRowsPerRG], 
						  [UnderQualityMeasure_RowGroups], 
						  [RGQualityMeasure], 
						  [UnderQualityMeasure_Percent], 
						  [DeletedRowsPercent], 
						  [RowgroupsWithDeletedRows], 
						d.[maxdictionary_Size], 
						d.[maxdictionary_Entrycount], 
						d.[maxpartition_number]
				from        CSAnalysis a
				inner join  CSDictionaries d
							on  a.object_id = d.object_id 
							and a.partition_number = d.partition_number
				inner join sys.indexes as i 
							on  a.object_id = i.object_id
							and a.index_id = i.index_id
							and i.type in (5,6)
				where DeletedRowsPercent > ' + @DeletedRowsPercent_Txt + 
					' or (AvgRowsPerRG < ' + @RGQualityMeasure_Txt +
						  ' and TotalRows > ' + @RGQualityMeasure_Txt +
						  ' and maxdictionary_Size < (16000000) ---- 16mb
						);
				select @@rowcount as Target_CSIndex, DB_NAME() as InDatabase;'
					)

		---- Nested Loop DB Level -----------------------------------------------------------------------------------

			While exists (select 1 from #TempColumnStoreStats)
			begin 
				Print	CONVERT(VARCHAR(20), GETDATE(),120) + ' Nested Loop within database [' + @Databasename + '] started'; 
				Declare @command varchar(2000),
						@objectID int,
						@IndexID varchar(500),
						@ParNumber int,

						@maxdophint smallint, 
						@effectivedop smallint;

				select @effectivedop = effective_max_dop 
				from sys.dm_resource_governor_workload_groups
				where group_id in (select group_id from sys.dm_exec_requests where session_id = @@spid);

				set @objectID =		(select top 1 object_id from #TempColumnStoreStats);
				set @IndexID =		(select top 1 index_id from #TempColumnStoreStats where object_id = @objectID);
				set @ParNumber =	(select top 1 partition_number from #TempColumnStoreStats where object_id = @objectID and index_id = @IndexID);
			--	set @tablaName =	(select top 1 TableName from #TempColumnStoreStats where object_id = @objectID);
			--	set @SchemaName =	(SELECT OBJECT_SCHEMA_NAME(@objectID, DB_ID(@Databasename)) );
			--	set @indexName =	(select top 1 IndexName from #TempColumnStoreStats where object_id = @objectID);

				---Generating command--------------------------------------------------------------------------------------------------
				select DISTINCT @command = 
						'Alter INDEX ' + QuoteName(IndexName) + ' ON '+ QUOTENAME(OBJECT_SCHEMA_NAME (a.object_id, DB_ID(@Databasename))) + 
						+ '.' +QuoteName(TableName) + ' REBUILD ' +
						Case
							when maxpartition_number = 1 THEN ' '
							else ' PARTITION = ' + cast(partition_number as varchar(10))
						End
						+ ' WITH (MAXDOP =' + 
						cast((Case 	WHEN (TotalRows * 1.0/1048576) < 1.0 THEN 1 
									WHEN (TotalRows * 1.0/1048576) < @effectivedop THEN FLOOR(TotalRows*1.0/1048576) 
									ELSE 0 
								END) as varchar(10)) + ');'
				from #TempColumnStoreStats a
				where a.object_id = @objectID and a.index_id = @IndexID and partition_number = @ParNumber;
				---Generating command end --------------------------------------------------------------------------------------------------
				BEGIN TRY
				Print 'Executing: ' + @command;
				Exec (@Command);
				END TRY

				BEGIN CATCH
					Select 'Error while executing: ' + @Command;
				END CATCH

				Delete from #TempColumnStoreStats where object_id = @objectID and index_id = @IndexID and partition_number = @ParNumber;
			END
---COLUMN STORE LOOP END ----------------------------------------------------------------------------------------
		END
		Delete from @TargetDb where DBName = @Databasename;
	END
END
