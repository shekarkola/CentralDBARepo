USE [DBAClient]
GO
/****** Object:  StoredProcedure [dbo].[read_error_log]    Script Date: 10/10/2024 12:09:44 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER proc [dbo].[read_error_log]   
	@FirstEntry SMALLDATETIME = null
   ,@LastEntry SMALLDATETIME = null

AS 
SET NOCOUNT ON

begin 
DECLARE @ArchiveID INT
   ,@Filter1Text NVARCHAR(4000)
   ,@Filter2Text NVARCHAR(4000)

SELECT @ArchiveID = 0
   ,@Filter1Text = ''
   ,@Filter2Text = ''

IF @FirstEntry is null 
	begin 
	set @FirstEntry = cast(DATEADD(DAY, - 1, getdate()) as date)
	end
IF @LastEntry is null 
	begin
	set @LastEntry = getdate()
	end

IF (OBJECT_ID ('tempdb..#ErrorLog')) is not null 
	BEGIN
		DROP TABLE #ErrorLog
	END

CREATE TABLE #ErrorLog (
   [date] [datetime] NULL
   ,[processinfo] [varchar](2000) NOT NULL
   ,[text] [varchar](2000) NULL
   ) ON [PRIMARY]

INSERT INTO #ErrorLog
EXEC master.dbo.xp_readerrorlog @ArchiveID
   ,1
   ,@Filter1Text
   ,@Filter2Text
   ,@FirstEntry
   ,@LastEntry
   ,N'asc'

SELECT *
FROM (
   SELECT [date]
      ,[processinfo]
      ,[text] AS [MessageText]
      ,LAG([text], 1, '') OVER (
         ORDER BY [date]
         ) AS [error]
   FROM #ErrorLog
   Where [text] not like 'This instance of SQL Server has been using a process ID %'
   ) AS ErrTabl
Order by [date] desc
--WHERE [error] LIKE 'Error%' 
-- you can change the text to filter above.
end 
