USE [DBA]
GO

CREATE OR ALTER view [dbo].[pbi_database_size_log] as

select	
		sl.INSTANCEFULLNAME as SQLInstanceFullName, 
		 (CASE WHEN CHARINDEX('\', sl.INSTANCEFULLNAME,1) = 0 THEN sl.INSTANCEFULLNAME
			ELSE SUBSTRING(sl.INSTANCEFULLNAME, 1, CHARINDEX('\', sl.INSTANCEFULLNAME,1) - 1) 
		 END) as [host_name],
		SUBSTRING(sl.INSTANCEFULLNAME, CHARINDEX('\', sl.INSTANCEFULLNAME,1) + 1, 15) as InstanceName,
		DatePart(month, DB_LogDate) as MonthNum,
		DatePart(YEAR, DB_LogDate) as YearNum,
		DB_LogDate,
		sl.DatabaseName,
		--CAST(DB_LogDate AS VARCHAR(11)) as DBLogDateName,
		--Cast (DatePart(YEAR, DB_LogDate) as varchar (4)) + Cast(Datepart(month, DB_LogDate) as varchar(2))  as MonthKey,
		--DateName(month, DB_LogDate) + '-' + Cast (DatePart(YEAR, DB_LogDate) as varchar (4)) as MonthNameYear,
		AG_IsJoined,
		AG_IsPrimary,
		DB_FileType,
		FileSize_GB as DB_FileSizeGB,
		FileSize_MB as DB_FileSizeMB
from dbo.DatabaseSizeLog as sl
WHERE ((AG_IsPrimary = 1) or AG_IsPrimary = 2) --and ( (d.is_in_standby = 0 ) and d.DB_State = 0)
	 and DB_LogDate >= DATEADD(DAY, -60, GETDATE())
	-- and sl.DatabaseName = 'AXDB'
GO

