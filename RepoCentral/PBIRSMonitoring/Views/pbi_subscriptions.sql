USE [ReportServerArchive]
GO

CREATE OR ALTER view [dbo].[pbi_subscriptions] as 

select	c.ItemID as ReportItemID,
		c.Name as ReportName,
		c.Path as ReportLocation,
		s.Description as SubscriptionTitle,
		EventType,
		LastRunTime,
		LastStatus,
		CreationDate
from ReportServerPBI.dbo.Subscriptions as s
	 join ReportServerPBI.dbo.Catalog as c on s.Report_OID = c.ItemID 
GO

