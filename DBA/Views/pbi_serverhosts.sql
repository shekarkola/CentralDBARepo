USE [DBA]
GO

CREATE OR ALTER view [dbo].[pbi_serverhosts] as 

SELECT	Distinct 
		instance_host as Hostname
FROM db_instances 
GO

