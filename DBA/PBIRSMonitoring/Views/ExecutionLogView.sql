USE [ReportServerArchive]
GO


ALTER VIEW [dbo].[ExecutionLogView]
AS
SELECT
	EL.LogEntryId,
    InstanceName,
    COALESCE(CASE(ReportAction)
        WHEN 11 THEN AdditionalInfo.value('(AdditionalInfo/SourceReportUri)[1]', 'nvarchar(max)')
        ELSE C.Path
        END, 'Unknown') AS ItemPath,
    UserName,
	Cast (c.ItemID as nvarchar(50)) as ItemID,
	IIF(c.ItemID is null, 1, 0) as IsItemDeleted,
	ISNULL(c.Name, 'Deleted') as ItemName,
	c.Type as ItemType,
	CASE (c.Type)
		WHEN 1 THEN 'Folder'
		WHEN 2 THEN 'Paginated Report'
		WHEN 3 THEN 'Embeded Files'
		WHEN 5 THEN 'Data Sources'
		WHEN 8 THEN 'Data Sets'
		WHEN 13 THEN 'PowerBI Report'
	ELSE 'Unknown' End as ItemTypeDesc,
    ExecutionId,
    CASE(RequestType)
        WHEN 0 THEN 'Interactive'
        WHEN 1 THEN 'Subscription'
        WHEN 2 THEN 'Refresh Cache'
        ELSE 'Unknown'
        END AS RequestType,
   -- SubscriptionId,
    Format,
    Parameters,
    CASE(ReportAction)
        WHEN 1 THEN 'Render'
        WHEN 2 THEN 'BookmarkNavigation'
        WHEN 3 THEN 'DocumentMapNavigation'
        WHEN 4 THEN 'DrillThrough'
        WHEN 5 THEN 'FindString'
        WHEN 6 THEN 'GetDocumentMap'
        WHEN 7 THEN 'Toggle'
        WHEN 8 THEN 'Sort'
        WHEN 9 THEN 'Execute'
        WHEN 10 THEN 'RenderEdit'
        WHEN 11 THEN 'ExecuteDataShapeQuery'
        WHEN 12 THEN 'RenderMobileReport'
        WHEN 13 THEN 'ConceptualSchema'
        WHEN 14 THEN 'QueryData'
        WHEN 15 THEN 'ASModelStream'
        WHEN 16 THEN 'RenderExcelWorkbook'
        WHEN 17 THEN 'GetExcelWorkbookInfo'
        WHEN 18 THEN 'SaveToCatalog'
        WHEN 19 THEN 'DataRefresh'
        ELSE 'Unknown'
        END AS ItemAction,
    TimeStart,
    TimeEnd,
    TimeDataRetrieval,
    TimeProcessing,
    TimeRendering,
    CASE(Source)
        WHEN 1 THEN 'Live'
        WHEN 2 THEN 'Cache'
        WHEN 3 THEN 'Snapshot'
        WHEN 4 THEN 'History'
        WHEN 5 THEN 'AdHoc'
        WHEN 6 THEN 'Session'
        WHEN 7 THEN 'Rdce'
        ELSE 'Unknown'
        END AS Source,
    Status,
    ByteCount,
    [RowCount],
    AdditionalInfo
FROM [ReportServerPBI].dbo.ExecutionLogStorage EL WITH(NOLOCK)
LEFT OUTER JOIN [ReportServerPBI].dbo.Catalog C WITH(NOLOCK) ON (EL.ReportID = C.ItemID)
Where UserName not like 'POM\Admin.%'
GO

