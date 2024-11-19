USE [ReportServerArchive]
GO

CREATE OR ALTER View [dbo].[pbi_reports_catalog] as 

select 	ROW_NUMBER() over (ORDER BY c.NAME) AS SlNum
		,c.ItemID as ReportItemID
		,Name as ReportName
		,Path as ReportLocation
		,(CASE 
			  WHEN (Left (Path,11) LIKE '%WorkSpace%' ) and not PATH like '%Hyperlinks%' and PATH like '%Subscriptions and Notifications%' 
				THEN 'WorkSpace'
			  WHEN PATH like '%Hyperlinks%' 
				THEN 'URL Ref.'
			  WHEN PATH like '%Subscriptions and Notifications%' 
				THEN 'Subscriptions'
			  ELSE 'LIVE'
		  END) as ReportStage
		,CASE (c.Type)
		WHEN 1 THEN 'Folder'
		WHEN 2 THEN 'Paginated Report'
		WHEN 3 THEN 'Embeded Files'
		WHEN 5 THEN 'Data Sources'
		WHEN 8 THEN 'Data Sets'
		WHEN 13 THEN 'PowerBI Report'
	ELSE 'Unknown' End as ReportType
	,Description as ReportDescription
	,Cast (CreationDate as date) as CreatedOn
	,Cast (ModifiedDate as date) as ModifiedOn
from ReportServerPBI.dbo.[Catalog] as c
Where C.Type NOT IN (1, 3, 8, 5)
GO

