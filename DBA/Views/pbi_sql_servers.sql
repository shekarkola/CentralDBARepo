USE [DBA]
GO

CREATE OR ALTER view [dbo].[pbi_sql_servers] as 

SELECT	Distinct 
		instance_host as hostname
		,instance_full_name as instance_fullname
		,instance_name
		,environment as environ
        ,dc
FROM db_instances
where is_deleted = 0
GO