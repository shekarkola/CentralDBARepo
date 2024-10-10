USE [DBAClient]
GO
/****** Object:  StoredProcedure [dbo].[IndexMaintenanceScope]    Script Date: 10/10/2024 11:53:38 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- ===================================================================================
-- Author:		Shekar Kola
-- Create date: 2019-06-09
-- Create date: 2020-01-26
-- Description:	defragment indexes daily basis, this procedure called by SQL Agent
-- ===================================================================================
CREATE OR ALTER PROCEDURE [dbo].[IndexMaintenanceScope]
		@DBName varchar (126) = null,
		@PageCount varchar(8) = 900
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	Declare @TotalCount int,
		@TableName varchar (258),
		@IndexName varchar (258),
		@FragmentPercent float,
		@IndexType tinyint, 
		@IsPrimary bit,
		@AlterIndexCmd varchar (1000);

	Declare @TargetDb table (DBName varchar (126) );

	IF @DBName is null 
		begin
			Insert into @TargetDb
			select	name 
			from	sys.databases 
			where	name not in ('master', 'model') and is_read_only = 0
		end
	else 

		begin
			Insert into @TargetDb
			select	@DBName 
		end

	--IF EXISTS (	select stat.replica_id
	--			from sys.dm_hadr_availability_replica_states as stat
	--					join (select replica_id, database_name 
	--						  from sys.dm_hadr_database_replica_cluster_states 
	--						  where group_database_id = (select group_database_id from sys.databases where database_id = DB_ID(@DBName))
	--						  ) as D ON Stat.replica_id = D.replica_id
	--			where is_local = 1 and role = 1)

	Begin
		IF (SELECT OBJECT_ID ('TempDB..#TempIndexScopeStats') ) IS NULL
				BEGIN
				--drop table #TempIndexScopeStats
				CREATE TABLE #TempIndexScopeStats
				(
					[database_id] [smallint] not NULL,
					[object_id] [int] not NULL,
					[index_id] [int] not NULL,
					[partition_number] [int] not NULL,
					[DatabaseName] [nvarchar](128) NULL,
					[SchemaName] [sysname] NULL,
					[TableName] [nvarchar](128) NULL,
					[IndexName] [sysname] NULL,
					[Size_MB] numeric(8,2),
					[avg_fragmentation_in_percent] numeric(8,2) NULL,
					[type] [tinyint] NOT NULL,
					[is_primary_key] [bit] NULL,
					[fill_factor] [tinyint] NOT NULL,
					[Page_Count] [bigint] NULL,
					[User_Reads] [bigint] NULL,
					[system_Reads] [bigint] NULL,
					[Write_Percent] [numeric](8, 0) NULL,
					[Read_Percent] [numeric](8, 2) NULL,
					[Total_PageSplits] [bigint] NOT NULL,
					[PageSplit_Percent] [numeric](8, 2) NOT NULL,
					[Total_Inserts] [int] NOT NULL,
					[Total_Updates] [int] NOT NULL,
					[Total_Deletes] [int] NOT NULL,
					[Total_UserReads] [int] NOT NULL,
					[Total_SystemReads] [int] NOT NULL
					);
				CREATE CLUSTERED INDEX CI_TempIndexScopeStats on #TempIndexScopeStats (database_id, object_id, index_id, partition_number);
			END

	TRUNCATE TABLE #TempIndexScopeStats;

While exists (select DBName from @TargetDb)
	BEGIN
		SET @DBName = (SELECT TOP 1 DBName FROM @TargetDb);

		BEGIN TRY 
			exec (
			'USE ' + @DBName + '
			Insert into #TempIndexScopeStats
			([database_id], [object_id], [index_id], [partition_number], [DatabaseName], [SchemaName], [TableName], [IndexName], Size_MB,[avg_fragmentation_in_percent], [type], [is_primary_key], [fill_factor], [Page_Count], [User_Reads], [system_Reads], [Write_Percent], [Read_Percent], [Total_PageSplits], [PageSplit_Percent], [Total_Inserts], [Total_Updates], [Total_Deletes], [Total_UserReads], [Total_SystemReads])
			select	    s.database_id,
						s.object_id,
						S.index_id,
						S.partition_number,
						DB_NAME(s.database_id) as DatabaseName,
						sch.name as SchemaName,
						object_name (s.object_id) as TableName, 
						I.name as IndexName, 
						isnull( (S.Page_Count * 8)/1024.0 ,0) as SizeMB,
						s.avg_fragmentation_in_percent, 
						I.type, 
						I.is_primary_key, 
						isnull(I.fill_factor,0) fill_factor,
						isnull(S.Page_Count,0) Page_Count,
						(isnull(user_scans,0) + isnull(user_seeks,0) + isnull(user_lookups,0) ) as User_Reads,
						(isnull(system_scans,0) + isnull(system_seeks,0) + isnull(system_lookups,0)) as system_Reads,
						
						IIF( (isnull(user_scans,0) + isnull(user_seeks,0) + isnull(user_lookups,0) + isnull(user_updates, 0) ) = 0, 
								0, 
								(isnull(user_updates,0) + 0.0) / 
								(isnull(user_scans,0) + isnull(user_seeks,0) + isnull(user_lookups,0) + isnull(user_updates, 0) )
							 ) as Write_Percent,
						
						IIF( (isnull(user_scans,0) + isnull(user_seeks,0) + isnull(user_lookups,0) + isnull(user_updates, 0) ) = 0, 
								0, 
								(isnull(user_scans,0) + isnull(user_seeks,0) + isnull(user_lookups,0) ) / 
							    (isnull(user_scans,0) + isnull(user_seeks,0) + isnull(user_lookups,0) + isnull(user_updates, 0) )
							 ) as Read_Percent,
						0 as Total_PageSplits,
						000.00 as PageSplit_Percent, 
						0 as Total_Inserts,
						0 as Total_Updates,
						0 as Total_Deletes,
						0 as Total_UserReads,
						0 as Total_SystemReads
			from sys.dm_db_index_physical_stats (DB_ID (), NULL, NULL, NULL, NULL) as S
				join sys.indexes as I on s.object_id = I.object_id and s.index_id = I.index_id
				join sys.objects as O on s.object_id = O.object_id 
				join sys.schemas as sch on O.schema_id = sch.schema_id
				join sys.dm_db_index_usage_stats as us 
					on s.database_id = us.database_id 
					and s.object_id = us.object_id
					and s.index_id = us.index_id 
			where  i.name is not null
					and s.avg_fragmentation_in_percent > 2 and S.page_count > ' + @PageCount +';

			update t 
				set 
					t.Total_PageSplits = s2.Total_PageSplits,
					t.PageSplit_Percent = (s2.PageSplit_Percent * 100),
					t.Total_Inserts = s2.Total_Inserts,
					t.Total_Updates = s2.Total_Updates,
					t.Total_Deletes = s2.Total_Deletes,
					t.Write_Percent = (t.Write_Percent * 100.0),
					t.Read_Percent = (t.Read_Percent * 100.0)
			from #TempIndexScopeStats as t
				left join 
				(SELECT  database_id,
							s.object_id,
							s.index_id,
							s.partition_number
							,(isnull(leaf_allocation_count,0) + isnull(nonleaf_allocation_count,0)) as Total_PageSplits
							,IIF( ((leaf_insert_count + nonleaf_insert_count) + (leaf_update_count + nonleaf_update_count) ) = 0,
									0,
									(leaf_allocation_count + nonleaf_allocation_count) /
									((leaf_insert_count + nonleaf_insert_count) + (leaf_update_count + nonleaf_update_count) + 0.0)
								  ) as PageSplit_Percent
							,(leaf_insert_count + nonleaf_insert_count) as Total_Inserts
							,(leaf_update_count + nonleaf_update_count) as Total_Updates
							,(leaf_delete_count + nonleaf_delete_count) as Total_Deletes
					FROM sys.dm_db_index_operational_stats (db_id (), null,null, null) as s
				) as s2
				on t.database_id = s2.database_id
				and t.object_id = s2.object_id 
				and t.index_id = s2.index_id
				and t.partition_number = s2.partition_number; 
				'
			);
			Print 'Database: ' + @DBName +  '. RowsEffected: ' + cast(@@ROWCOUNT as varchar(10));
		END TRY
			BEGIN CATCH
					Select 'Error occurred at ' + @DBName + ' Error Message: ' + ERROR_MESSAGE();
					DELETE FROM @TargetDb WHERE DBName = @DBName			
				
			END CATCH
			
			DELETE FROM @TargetDb WHERE DBName = @DBName;

			--COMMIT TRAN;
		END


	select *,
			CASE WHEN t.avg_fragmentation_in_percent > 30 
					then 'Use ' + t.DatabaseName + '; alter index ['+ IndexName + '] on ['+ SchemaName +'].[' + TableName + '] Rebuild WITH (ONLINE = OFF); '
				ELSE  'Use ' + t.DatabaseName + '; alter index ['+ IndexName + '] on ['+ SchemaName +'].[' + TableName + '] REORGANIZE ; '
			END as AlterIndexCommand
	from #TempIndexScopeStats as t 
	ORDER BY avg_fragmentation_in_percent desc 
	--TableName, IndexName
	end
END
