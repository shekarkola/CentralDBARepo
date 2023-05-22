USE [DBA]
GO

CREATE OR ALTER view [dbo].[pbi_storage] as 
select hostname, drive, logical_name, available_mb, total_size_mb
from server_storage
GO

