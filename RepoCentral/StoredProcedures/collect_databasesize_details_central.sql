USE [DBA]
GO

/*-- ----------------------------------------------------------------------------------------
-- Author:			SHEKAR KOLA
-- Create date:		2019-11-17
-- Description:		Central database size log with details 

	Version: 20210317
		-	Changed the remote procedure execution method, 
			"exec ('') at linked server" used instead 4 part name, 
			so that it won't fail with "remote access disabled" error 
			which should be always disabled as security best practices 

	Version: 20191117
--------------------------------------------------------------------------------------------*/

CREATE OR ALTER PROCEDURE [dbo].[collect_databasesize_details_central]
	-- Add the parameters for the stored procedure here
AS
BEGIN

SET NOCOUNT ON;

------------------------------------------------------------------------------------------------------------------------------------------
-- Validate and run if it's only primary replica 
Declare @IsHADR bit;
select @IsHADR = Cast(SERVERPROPERTY ('Ishadrenabled') as int);
if @IsHADR = 0 or exists (	select r.replica_id 
							from sys.dm_hadr_availability_replica_states r
							join sys.availability_groups ag on r.group_id = ag.group_id
							join sys.availability_databases_cluster agc on ag.group_id = agc.group_id and database_name = DB_NAME()
							where is_local =1 and role = 1)
------------------------------------------------------------------------------------------------------------------------------------------
BEGIN

		DECLARE @OPENQUERY nvarchar(4000), 
				@TSQL_LinkServer nvarchar(4000), 
				@TSQL_Local nvarchar(4000), 
				@LinkedServer nvarchar(50);

		Declare @TargetServers table (ID int, LinkServer nvarchar(50));

		CREATE TABLE #TempTable 
		(
			[INSTANCEFULLNAME] [nvarchar](128) NULL,
			[DATABASENAME] [sysname] NOT NULL,
			DATABASEID int,
			LOG_DATE date,
			USAGETYPE [nvarchar](25) NULL,
			ALLOCATIONTYPE [nvarchar](25) NULL,
			FILEGROUPNAME [nvarchar](125) NULL,
			RESERVED_MB  DECIMAL (15,3),
			USED_MB  DECIMAL (15,3),
		);

		Print Format (getdate(), 'yyyy-MMM-dd HH:mm:ss')+ ' TempTable Created; ';

		SET @TSQL_LinkServer = 	'''' + 
									'SELECT  [INSTANCEFULLNAME]
											,DATABASEID
											,DatabaseName 
											,LOG_DATE
											,[USAGETYPE]
											,[ALLOCATIONTYPE]
											,[FILEGROUPNAME]
											,[RESERVED_MB]
											,[USED_MB]
								FROM [DBAClient].[dbo].[TempDatabaseSizeLogDetails]' 
							+ ''')' ;

		SET @TSQL_Local = 	
									'SELECT  [INSTANCEFULLNAME]
											,DATABASEID
											,DatabaseName 
											,LOG_DATE
											,[USAGETYPE]
											,[ALLOCATIONTYPE]
											,[FILEGROUPNAME]
											,[RESERVED_MB]
											,[USED_MB]
								FROM [DBAClient].[dbo].[TempDatabaseSizeLogDetails]'   ;
---------------------------------------------------------------------------------------------------------------------------------
		 --Inserting Local Server data into TempTable 
		EXEC DBAClient.dbo.ProcessDatabaseSizeDetails;

		Insert into #TempTable		 (	[INSTANCEFULLNAME]
										,DATABASEID
										,DatabaseName 
										,LOG_DATE
										,[USAGETYPE]
										,[ALLOCATIONTYPE]
										,[FILEGROUPNAME]
										,[RESERVED_MB]
										,[USED_MB]
									 )
		Exec (@TSQL_Local)
		Print FORMAT(GETDATE(), 'yyyy-MMM-dd HH:mm:ss') +  'Local Server data loaded into TempTable; ';
----------------------------------------------------------------------------------------------------------------------------------------------------------
-- Calling linked server procedure to process "Unused-Indexes" locally on every linked server  
----------------------------------------------------------------------------------------------------------------------------------------------------------
	Insert into @TargetServers select server_id, name from sys.servers where is_linked = 1  and (provider = 'SQLNCLI' or provider = 'MSOLEDBSQL');
	
	While exists (select * from @TargetServers) 
		BEGIN
			SET @LinkedServer = (select top 1 LinkServer from @TargetServers order by ID);
			SET @OPENQUERY =  'Exec (''DBAClient.dbo.ProcessDatabaseSizeDetails'') at [' +@LinkedServer+ ']';

			Print FORMAT(GETDATE(), 'yyyy-MMM-dd HH:mm:ss') +  ' Calling remote server client procedure at [' + @LinkedServer + ']; ';
			exec (@OPENQUERY);

			IF (@@ERROR <> 0)
					begin
						Print	FORMAT(GETDATE(), 'yyyy-MMM-dd HH:mm:ss') +  
								' Error  occurred at ['+ @LinkedServer +'], Error Message: ' 
								+ Cast (ERROR_MESSAGE () as nvarchar (500)) +'; ';

						Delete from @TargetServers where LinkServer = @LinkedServer
					end
			
				Delete from @TargetServers where LinkServer = @LinkedServer
		END

----------------------------------------------------------------------------------------------------------------------------------------------------------
-- Calling linked server procedure to process "Indexe usage info" into central temp table from all linked servers
----------------------------------------------------------------------------------------------------------------------------------------------------------

		Insert into @TargetServers select server_id, name from sys.servers where is_linked = 1  and (provider = 'SQLNCLI' or provider = 'MSOLEDBSQL');

		-- begin Loop to prepare staging data from all linked servers 
		While exists (select * from @TargetServers) 
			BEGIN
				SET @LinkedServer = (select top 1 LinkServer from @TargetServers order by ID)
				SET @OPENQUERY = 'SELECT * FROM OPENQUERY(['+ @LinkedServer + '],'

				-- Inserting linked Server data into Temp Table 
				Insert into #TempTable	(	[INSTANCEFULLNAME]
											,DATABASEID
											,DatabaseName 
											,LOG_DATE
											,[USAGETYPE]
											,[ALLOCATIONTYPE]
											,[FILEGROUPNAME]
											,[RESERVED_MB]
											,[USED_MB]
											 )

				EXEC (@OPENQUERY + @TSQL_LinkServer);
				Print FORMAT(GETDATE(), 'yyyy-MMM-dd HH:mm:ss') +  ' Linked Server "'+ @LinkedServer +' " data loaded into TempTable; ';

				Delete from @TargetServers where LinkServer = @LinkedServer
			END

		---- Delete duplicate entries if any exists: 
           ;WITH dup as (
                select ROW_NUMBER() OVER (PARTITION BY [DATABASENAME], LOG_DATE, USAGETYPE, ALLOCATIONTYPE ORDER BY LOG_DATE, USAGETYPE, ALLOCATIONTYPE ) AS RN 
                       ,* 
                from #TempTable 
            )
            DELETE FROM dup WHERE RN > 1;
--------------------------------------------------------------------------------------------------------------------------------------
				 -- Inserting DatabaseMaster from staging table
					BEGIN
					Print Format (getdate(), 'yyyy-MMM-dd HH:mm:ss')+ ' Inserting into Actual Table - Starting; '
						INSERT INTO DatabaseSizeLogDetails 
							([INSTANCEFULLNAME]
							,[DATABASENAME]
							,DATABASEID
							,LOG_DATE
							,[USAGETYPE]
							,[ALLOCATIONTYPE]
							,[FILEGROUPNAME]
							,[RESERVED_MB]
							,[USED_MB]
							 )
						SELECT	 S.[INSTANCEFULLNAME]
								,S.[DATABASENAME]
								,s.DATABASEID
								,s.LOG_DATE
								,S.[USAGETYPE]
								,S.[ALLOCATIONTYPE]
								,S.[FILEGROUPNAME]
								,S.[RESERVED_MB]
								,S.[USED_MB]
								
						FROM #TempTable as S
						left outer join DatabaseSizeLogDetails as D 
										on  s.[INSTANCEFULLNAME] = d.[INSTANCEFULLNAME] 
										and s.DATABASEID = d.DATABASEID
										and s.LOG_DATE = D.LOG_DATE
										and D.FILEGROUPNAME = S.FILEGROUPNAME
										and D.USAGETYPE = S.USAGETYPE
										and D.ALLOCATIONTYPE = S.ALLOCATIONTYPE
						Where D.DATABASENAME is null;
						SELECT  @@ROWCOUNT as [Inserted Rows];
					END
					
--------------------------------------------------------------------------------------------------------------------------------------
					-- Updating DatabaseStats  from staging table
					begin	
					Print Format (getdate(), 'yyyy-MMM-dd HH:mm:ss')+ ' Updating Actual Table - Starting; '

						UPDATE D set	D.USED_MB = S.USED_MB,
										D.RESERVED_MB = S.RESERVED_MB,
										MODIFIED_ON = SYSDATETIME()
						FROM DatabaseSizeLogDetails as D
							left outer join #TempTable as S
											on  d.[INSTANCEFULLNAME] = s.[INSTANCEFULLNAME] 
											and d.DATABASEID = s.DATABASEID
											and d.LOG_DATE = s.LOG_DATE
											and D.FILEGROUPNAME = S.FILEGROUPNAME
											and D.USAGETYPE = S.USAGETYPE
											and D.ALLOCATIONTYPE = S.ALLOCATIONTYPE
						Where S.DATABASENAME is not null;
						SELECT @@ROWCOUNT as [Updated Rows];
					end

------------------------------------------------------------------------------------------------------------------------------------

		Drop table #TempTable;
		Print Format (getdate(), 'yyyy-MMM-dd HH:mm:ss')+ ' TempTable Dropped; ';
	END
	ELSE PRINT 'This is NOT a Primary replica!'
END
GO
