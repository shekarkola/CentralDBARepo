USE [DBAClient]
GO

-- =============================================
-- Author:		Shekar Kola
-- Create date: 2019-06-09
-- Create date: 2019-11-10
-- Description:	Collecting index usage info from all databases 
-- =============================================

CREATE OR ALTER Procedure [dbo].[collect_index_usageinfo]
	
AS 
BEGIN 

IF (select OBJECT_ID ('TempDB..#IndexUsageTargetDb')) is null
	begin
	Create table #IndexUsageTargetDb (DBName varchar (128) );
	end
Declare @DbName varchar (126);
Declare @isPrimaryReplica bit;
Declare @is_DB_HADREnabled bit;

	Truncate table TempIndexUsageInfo;

		begin
			Insert into #IndexUsageTargetDb
			select	name 
			from	sys.databases 
			where	database_id > 4 and state = 0
		end

While exists (select 1 from #IndexUsageTargetDb)
	BEGIN
		SET @DBName = (SELECT TOP 1 DBName FROM #IndexUsageTargetDb);
		select @is_DB_HADREnabled = IIF(group_database_id IS NULL, 0,1) from sys.databases where [name] = @DBName;
		
		IF EXISTS (select db.name
						from sys.dm_hadr_database_replica_states as hadr
							join sys.databases as db on hadr.group_database_id = db.group_database_id
						where is_local = 1 and is_primary_replica = 1 and db.name = @DBName
					)
				BEGIN
				SET @isPrimaryReplica = 1 
				END
			ELSE 
				BEGIN
				SET @isPrimaryReplica = 0 
				END

		IF @is_DB_HADREnabled = 0 or @isPrimaryReplica = 1
		BEGIN
					BEGIN TRY 
							Insert into TempIndexUsageInfo
									(	InstanceFullName, [DatabaseName], [ObjectID], ObjectName,
										[indexID], IndexName, IndexType,
										RowsCount,  RowData_Mb, CluIndex_Mb,
										NCluIndex_Mb, totalreads_user, totalreads_sys,
										totalwrites_user, Write_Percent, Read_Percent,
										lastRead_User, lastRead_Sys, lastWrite_user,
										LastUsed
									)

							Exec(
									'USE [' + @Dbname +
									']
											select	 @@SERVERNAME as InstanceFullName
												,DB_Name() as DBName
												, IxSize.object_id
												,OBJECT_NAME(IxSize.object_id) as ObjectName
												,IxSize.index_id
												,I.name as IndexName
												,I.type_desc as IndexType
												,IxSize.[RowsCount]
												,IxSize.RowData_Mb
												,IxSize.CluIndex_Mb
												,IxSize.NCluIndex_Mb
												, (user_scans + user_seeks + user_lookups) as User_Reads
												, (system_scans + system_seeks + system_lookups) as Sys_Reads
												,user_updates as Writes
												, ((user_updates + 0.0) / 
													IIF( (user_scans + user_seeks + user_lookups + user_updates)=0, NULL, (user_scans + user_seeks + user_lookups + user_updates) ) )*100 as Write_Percent
												,( (user_scans + user_seeks + user_lookups + 0.0) / 
													IIF( (user_scans + user_seeks + user_lookups + user_updates)=0, NULL, (user_scans + user_seeks + user_lookups + user_updates) ) )*100 as Read_Percent
												, ISNULL(  (ISNULL(last_user_scan, last_user_seek)),  last_user_lookup) as lastRead_User
												, ISNULL(  (ISNULL(last_system_scan, last_system_seek)),  last_system_LOOKUP) as lastRead_Sys
												, Last_user_update as lastWrite_user
												, COALESCE (last_user_scan, last_user_seek, last_user_lookup, Last_user_update, last_system_scan, last_system_seek, last_system_LOOKUP) as LastUsed
										from	(
												select	ps.object_id,
														ps.index_id,
														partition_number,
														SUM (CASE WHEN ps.index_id = 0 -- HEAP (actual data)
																then (used_page_count * 8.00) / 1024 end
															) as RowData_Mb
														,SUM (CASE WHEN ps.index_id = 1 -- Clustered Indexes (actual data)
																then (used_page_count * 8.00) / 1024 end
															) as CluIndex_Mb
														,SUM (CASE WHEN ps.index_id > 1 -- Clustered Indexes (actual data)
																then (used_page_count * 8.00) / 1024 end
															) as NCluIndex_Mb
														,max (row_count) as [RowsCount]
												from sys.dm_db_partition_stats as ps
												where object_id > 250
												GROUP BY ps.object_id, ps.index_id, partition_number
												) as IxSize
											join
											sys.indexes as I 
												on IxSize.object_id = I.object_id 
												and IxSize.index_id = I.index_id

											left join 
											sys.dm_db_index_usage_stats as UseStats 
													on  IxSize.object_id = UseStats.object_id 
													and IxSize.index_id = UseStats.index_id
											'
									);
							END TRY
						BEGIN CATCH
								Select 'Error occurred at ' + @DBName + ' Error Message: ' + ERROR_MESSAGE();
								DELETE FROM #IndexUsageTargetDb WHERE DBName = @DBName			
				
						END CATCH
				END
			ELSE 
			PRINT 'The database [' + @DBName + '] is part of Availability group, and this is not primary replica, data not processed!';

			DELETE FROM #IndexUsageTargetDb WHERE DBName = @DBName;
	END

 End
