USE [DBA]
GO

CREATE OR ALTER View [dbo].[pbi_missing_indexes] as 

select  [hostname] ,
		[sql_instance],
		[hostname] + '\'+[sql_instance] as server_fullname,
		[database_name],
		idx_table,
		index_handle,
		idx_columns,
		idx_columns_include,
		[user_requests] ,
		[impact_reduced],
		[hostname] + '-' + [sql_instance] + '-' + cast (databaseID as varchar (2)) as database_key
from missing_indexes
where  impact_reduced > 50
and user_requests > 500 and [optimized_on] is null
GO

