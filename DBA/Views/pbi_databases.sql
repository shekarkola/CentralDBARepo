USE [DBA]
GO

CREATE OR ALTER View  [dbo].[pbi_databases] as
with db as (select	 t.DATABASENAME as DatabaseName, IS_JOINED_AVAILABILITYGROUPS, RECOVERY_MODEL, t.APPLICATION_NAME
					, ROW_NUMBER() OVER (PARTITION BY t.DatabaseName order by t.APPLICATION_NAME desc,t.CREATED_ON desc) as rn
			from database_master as t 
			where IS_DROPPED = 0 and (IS_AG_PRIMARY = 1 or IS_JOINED_AVAILABILITYGROUPS = 0)
			)
, primary_rep as (
select DATABASENAME, max(INSTANCEFULLNAME) as INSTANCE_FULLNAME  
from database_master as t 
where IS_DROPPED = 0 and (IS_AG_PRIMARY = 1 or IS_JOINED_AVAILABILITYGROUPS = 0)
group by DATABASENAME
)

,dbsize as (SELECT DatabaseName
		,SUM(RESERVED_MB) as reserved_mb
		,SUM(USED_MB) as used_mb

FROM (
select sl.DatabaseName
		,sl.LOG_DATE as Logdate
		,ROW_NUMBER() OVER (PARTITION BY sl.DatabaseName, sl.ALLOCATIONTYPE ORDER BY sl.LOG_DATE desc) as rn 
		, sl.ALLOCATIONTYPE
		, sl.RESERVED_MB
		, sl.USED_MB
FROM DatabaseSizeLogDetails as sl
where sl.LOG_DATE >=  DATEADD(DAY, -10, GETDATE())
) AS T 
WHERE rn = 1
GROUP BY DatabaseName
)

-- select * from dbsize

select db.*, r.INSTANCE_FULLNAME, dbsize.reserved_mb, dbsize.used_mb
from db 
join primary_rep as r on db.DATABASENAME = r.DATABASENAME 
left join dbsize on db.DatabaseName = dbsize.DatabaseName
where db.rn = 1
GO
