USE [ReportServerArchive]
GO

CREATE OR ALTER view [dbo].[pbi_execution_stats] as 
select	ItemID,
		UserName, 
		ItemPath,
		FORMAT(TimeStart,'yyyyMM') as PeriodMonth
		,CASE WHEN ItemPath = '' or (CHARINDEX('/',(ItemPath),3 )-2) < 0  THEN ''
			ELSE 
			SUBSTRING(ItemPath, 2,  (CHARINDEX('/',(ItemPath),2 )-2))
			END as ParentFolder,
		(CASE ItemType 
			WHEN 13 Then 'Http://bi.pom.ae/BI/powerbi' + ItemPath
			ELSE 'Http://bi.pom.ae/BI/report' + ItemPath
		END) as ReportURL
		,ItemName
		--,ItemAction
		,COUNT (distinct UserName) as distinct_users
		,sum(TimeProcessing / 1000.0) as Total_TimeProcessing
		,sum(TimeRendering / 1000.0) as Total_TimeRendering
		,sum(ByteCount) as Total_ByteCount
		,COUNT (ExecutionId) as Total_Executions
		,MAX(T.TimeEnd) as LastAccessed
		,DATEDIFF(DAY, MIN(T.TimeEnd), MAX(T.TimeEnd)) as StatsPeriod_Days
		,AVG( CASE WHEN ItemAction = 'DataRefresh' then DATEDIFF(SECOND, T.TimeStart, T.TimeEnd) END ) as AvgModelRefresh_Sec
from ExecutionLog as T
where TimeStart >= DATEADD(YEAR,-13, GETDATE())
group by ItemID,
		UserName,
		ItemType, ItemPath, ItemName
		,FORMAT(TimeStart,'yyyyMM')
		--,ItemAction
GO

