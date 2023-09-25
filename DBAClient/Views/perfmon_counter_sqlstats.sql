USE DBAClient
go

CREATE OR ALTER VIEW perfmon_counter_sqlstats_view AS
---- SQL Stats View:
with sqlstats as (
select instance_fullname, sample_time, counter_name
		,cntr_value - 
		 LAG(cntr_value) OVER (PARTITION BY instance_fullname, object_name, counter_name, instance_name ORDER BY sample_time) + 0.0 as counter_value
		,DATEDIFF(SECOND, 
		LAG(sample_time) OVER (PARTITION BY instance_fullname, object_name, counter_name, instance_name ORDER BY sample_time)
		,sample_time) + 0.0 as sample_secs
from dbo.os_performance_counters 
where  object_name like '%SQL Statistics'
		-- counter_name = 'SQL Re-Compilations/sec'
)

select instance_fullname, sample_time, counter_name
		,CAST(ISNULL(IIF(counter_value < 0, 0, counter_value), 0) /
		 ISNULL(IIF(sample_secs <= 0, 1, sample_secs), 1) as NUMERIC(10,2)) counter_value
from sqlstats 
-- where counter_name <> 'Batch Requests/sec'