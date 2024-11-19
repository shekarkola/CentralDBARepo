USE [DBAClient]
GO

CREATE OR ALTER Procedure [dbo].[collect_index_unused]
		@UsageCount varchar(2) = NULL
	
AS 
BEGIN 

Declare @query nvarchar (3000);
	IF @UsageCount is null
		BEGIN
		SET @UsageCount = '200';
		END
		Truncate table dbo.TempUnusedIndexes;

		SET @query = ('USE [?]
				select	@@SERVERNAME as InstanceFullName
						, @@SERVICENAME as InstanceName
						, database_id
						, DB_NAME ()
						, IUS.object_id
						, Object_Name(IUS.object_id) as ObjectName
						, IUS.index_id
						, I.name as IndexName
						, (user_scans + user_seeks + user_lookups) TotalReads_user
						, (system_scans + system_seeks + system_lookups) TotalReads_Sys
						, user_updates as TotalWrites_User
						, ISNULL(  (ISNULL(last_user_scan, last_user_seek)),  last_user_lookup) as lastRead_User
						, ISNULL(  (ISNULL(last_system_scan, last_system_seek)),  last_system_LOOKUP) as lastRead_Sys
						, Last_user_update as lastWrite_user
						, COALESCE (last_user_scan, last_user_seek, last_user_lookup, Last_user_update, last_system_scan, last_system_seek, last_system_LOOKUP) as LastUsed
						, a.total_pages, p.rows
						, STATS_DATE(I.object_id, I.index_id) as index_create_date
				from sys.dm_db_index_usage_stats IUS
				join sys.indexes as I on IUS.object_id = I.object_id and IUS.index_id = i.index_id
				join sys.partitions p on i.object_id = p.object_id and i.index_id = p.index_id
				join sys.allocation_units a on p.partition_id = a.container_id
				where (I.is_primary_key = 0 and I.Type > 1)' + 
				--- when a index got more than 500 pages and not been used N times or not been used more than 30 days are required investigation 
					' and a.total_pages >= 500' + 
					' and( (user_scans + user_seeks + user_lookups) <='+ @UsageCount  + '
							or (DATEDIFF (DAY, ISNULL(  (ISNULL(last_user_scan, last_user_seek)),  last_user_lookup), GETDATE ()) > 30)
							)'
			);

		Insert into dbo.TempUnusedIndexes  
		([InstanceFullName], [InstanceName], [DatabaseID], [DatabaseName], [ObjectID], [ObjectName], [indexID], [indexname], [totalreads_user], [totalreads_sys], [totalwrites_user], [lastRead_user], [lastRead_Sys], [lastWrite_user], [lastUsed], [total_pages], [total_rows], index_creation_date)
		exec sp_MSforeachdb @query

 End
