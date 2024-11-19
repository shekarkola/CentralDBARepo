USE [DBA]
GO

CREATE OR ALTER view [dbo].[pbi_cpu_utilization] as 

select   hostname as [host_name]
		,Cast (event_time as smalldatetime) as event_datetime
		,Cast (event_time as date) as event_date
		,hostname + IIF(sql_instance_name is null, '', '\'+sql_instance_name) as instance_fullname
		,AVG (cpu_percent_sql_server) as cpu_utilization_sqlserver
		,AVG (cpu_percent_others) as cpu_utilization_others
from dbo.cpu_ringbuffer
where event_time >= DATEADD(DAY, -45, GETDATE() )
Group by hostname, sql_instance_name, Cast (event_time as smalldatetime) , Cast (event_time as date)

GO

