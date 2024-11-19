use DBAClient
go

CREATE OR ALTER VIEW perfmon_counter_replicastats_view as 

with RepStats as (
select instance_fullname, sample_time, counter_name
		,cntr_value - 
		 LAG(cntr_value) OVER (PARTITION BY instance_fullname, object_name, counter_name, instance_name ORDER BY sample_time) + 0.0 as counter_value
		,DATEDIFF(SECOND, 
		LAG(sample_time) OVER (PARTITION BY instance_fullname, object_name, counter_name, instance_name ORDER BY sample_time)
		,sample_time) + 0.0 as sample_secs
from DBAClient.dbo.os_performance_counters 
where  [object_name] like '%Database Replica'
)

select instance_fullname, sample_time
		,SUM(CASE WHEN counter_name = 'Mirrored Write Transactions/sec' THEN (IIF(counter_value<=0, 1, counter_value)/sample_secs) END) as tran_count
		,SUM(CASE WHEN counter_name = 'Transaction Delay' THEN IIF(counter_value<0, 0, counter_value) END) as tran_delay
		,SUM(CASE WHEN counter_name = 'Transaction Delay' THEN counter_value END) / 
		 SUM(CASE WHEN counter_name = 'Mirrored Write Transactions/sec' THEN (IIF(counter_value<=0, 1, counter_value)/sample_secs) END) as avg_tran_delay
from RepStats
group by instance_fullname, sample_time
go



select * from FSIEvolution.dbo.f_