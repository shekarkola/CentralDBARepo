USE [DBA]
GO


/*
-- ======================================================================
-- Author:			SHEKAR KOLA
-- Create date:		2022-12-19
-- Description:		Database backups archival and compression
-- ======================================================================

Prerequisites:
		- 7zip tool, the executable file must be available in "C:\Program Files\7-Zip\7z.exe"
		- "xp_cmdshell" used for executing COPY, COMPRESS, and DELETE Files or Folders
		- This Job will enable and disable the "xp_cmdshell" setting in SQL Instance, the login executing this job must need the permissions for "sp_configure"
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
Version 20221219
    -   @WeeklyFullBackupDay parameter added, based on this parameter
        Last Full Backup of the month will be identified, especically when the Weekly Full backup schedule followed for some databases

Version 20221010
    -   Query stracture format improved as per Azure Data-Studio 
    -   New parameters added 
        - @IncludeServers: Similar to exclude servers this will set the scope for only servers mentioned in this parameter, multiple servers names accepted, which must be separated with semicolon (;)
        - @Compress: a bit parameter added default value is 1, when 0 is passed it only perform copy and delete as per retention period

Version 20220314
	-	Exclude servers parameter added, by using this Linked servers can be excluded from target servers list, 
		with this option multiple SQL-JOBS can be created to call same procedure for group of servers while their destination shared location is different
	-	print message added after enabling xp_cmdshell

Version 20220205
	-	Custom destination location added for moving the backup files, the destination location will be used from the passed parameter 
		instead moving the database files into dated folder of same parent where backups originally created 

Version 20211212
	-	Double quotes (") added for ROBOCOPY command in @movefile parameter, to avoid errors when DB name contains spaces or special characters 

Version 20211201
	-	CASE condition added for @MoveFile parameter assignment to avoid unexpected parameter error for RIGHT function 

Version 20210401
	-	Added TRY CATCH block to avoid failures when a linked server not accessible

Version 20210401
	-	Changed production backup location for all servers

Version 20201102
	-	Removed the dependency of user view (backuplog), following parameters (dynamic sql) depends on system catalog msdb.dbo.backupset
		o	@TSQL_LinkServer
		o	@TSQL_Local
	-	Changed following 7zip switches. It’s been noticed since last one month that the Compress Job duration is 4-6 Hours during this time CPU utilization >80%, following switches can dramatically change the compress job duration
		o	–mm = copy 
			This disables the compression mode, since anyway the backup files already compressed, having default compression options from 7zip can maximum yield 2-5% of storage
		o	–mmt = off
			This will allow 7zip to use all CPU to finish compression process quicker

Version 20200503 
	-	Changed current database deletion scope to 3 days old
	-	Changed old database deletion scope to 33 days old

Version 20191223
	-	Added following switches inside 7zip command which would set CPU affinity to 2 so that the high utilization reduced during compress procedure
		o	-mmt=2

Version 20191223
	-	Added following switches inside 7zip command which would set CPU affinity to 2 so that the high utilization reduced during compress procedure
	o	-mmt=2


Version 20191219
	-	Added following switches inside 7zip command 
	o	–mx1 this will reduce processing time, this improvement based on CPU utilization monitoring which hitting 80% during the compression procedure running time
	o	–sdel this will delete the files after compression. Earlier version the delete operation happens separately

Version 20191117
	-	Fix Log Shipping Failure
	-	Issue description: Log Shipping backups named with UTC time stamp wherein regular backups named with local zone time stamp and when Log Shipping backup file compressed and deleted before it copied to secondary server, it would break the Log Shipping
	-	Earlier version the compression scope defined based on file name suffix. Now it will be followed by database log from msdb database and all database backup Files related to same date will be moved into a sub folder (YYYYMMDD) after the day. I.e. files belongs today would not be considered.

Version 20191013
	-	Included BIN folder clean-up 

Version 20191003
	-	Changed the default behavior of @DatabaseName parameter, it accepts default NULL, when it’s NULL compression procedure go through all sub-folders which are created with database name during the backup by dobackup procedure

*/

CREATE OR ALTER PROCEDURE [dbo].[CompressBackups]

	@DatabaseName varchar(128) = null,
	@BackupDate date = null,
	@BackupDestination varchar(8000) = '', --- Default destination can be passed here
	@ExcludeServers varchar(8000) = null,
	@IncludeServers varchar(8000) = null,
	@RetentionDays int = 32,
    @Compress bit = 1,
	@WeeklyFullBackupDay varchar(25) = null ---- In case weekly Full Backup followed, mentioned the day of Full backup, so that Last Full Backup of each month moved into Monthly Folder
AS
BEGIN

    SET NOCOUNT ON;
    --------------------------------------------------------------------------------------------------------------------------------------
    -- Validate and run if it's only primary replica 
    Declare @IsHADR bit;
    select @IsHADR = Cast(SERVERPROPERTY ('Ishadrenabled') as int);

    if @IsHADR = 0 or exists (	select r.replica_id 
                                from sys.dm_hadr_availability_replica_states r
                                join sys.availability_groups ag on r.group_id = ag.group_id
                                join sys.availability_databases_cluster agc on ag.group_id = agc.group_id and database_name = DB_NAME()
                                where is_local =1 and role = 1)
    --------------------------------------------------------------------------------------------------------------------------------------
    BEGIN
            DECLARE @OPENQUERY nvarchar(4000), 
                    @TSQL_LinkServer nvarchar(4000), 
                    @TSQL_Local nvarchar(4000), 
                    @LinkedServer nvarchar(255);

            Declare @ZipCommand nvarchar (1000);
            Declare @DelCommand nvarchar (1000);
            Declare @DelCommand2 nvarchar (1000);
            Declare @ZipName nvarchar (1000);
            Declare @BackupDBName nvarchar (256);
            Declare @BackupFileName nvarchar (1000);
            Declare @BackupFileID bigint;
            Declare @ShareDrive nvarchar (4000);
            Declare @ShareDriveBIN nvarchar (4000);
            Declare @DelCommandBIN nvarchar (4000);
            Declare @MoveSrcFolder nvarchar (4000);
            Declare @MoveDstFolder nvarchar (4000);
            Declare @MoveFile nvarchar (4000);
            Declare @MoveCommand nvarchar (4000);
            Declare @BackupDateTxt nVARCHAR(10);

            Declare @commandResult table (Result nvarchar(max));
            Declare @TargetServers table (ID int, LinkServer nvarchar(50));
            Declare @TargetDatabases table (DatabaseName varchar(128));
            Declare @PrintMsg nvarchar(4000);

            Declare @SearchKeyDel varchar (50),
                    @MonthlyCopyFiles nvarchar (4000),
					@MonthlyCopyFiles2 nvarchar (4000),
                    @MonthlyCopyFolder nvarchar (4000),
					@MonthlyCopyFolder2 nvarchar (4000),
					@LastWeekOfMonth nvarchar(4000);

            --- Mandatory setting to be enabled to perform MS-DOS commands -------------------------
            BEGIN
                exec sp_configure 'show advanced options', 1;
                Reconfigure;

                exec sp_configure 'xp_cmdshell', 1;
                Reconfigure;
            END
            SELECT @PrintMsg = '"xp_cmdshell" has been enabled using sp_configure, value in use = ' + CAST(value_in_use as nvarchar(10)) FROM sys.configurations where name = 'xp_cmdshell';
            print @PrintMsg;
            
            IF (select OBJECT_ID ('Tempdb..##TempBackupLog')) is null
            BEGIN
                Create table #TempBackupLog 
                        (BackupFileName varchar(500) not null,
                        FileID as (CHECKSUM (BackupFileName) ),
                        DatabaseName varchar(128)
                        );
                Create clustered index clu_TempBackupLog on #TempBackupLog (FileID)
            END

                if @BackupDate is null 
                    begin
                        set @BackupDateTxt = (select convert( varchar(10),DATEADD(DAY,-1,GETDATE ()), 112)  );
                    end
                if @BackupDate is not null 
                    begin
                        set @BackupDateTxt = (select convert(varchar(10), @BackupDate, 112));
                    end

				if @WeeklyFullBackupDay is not null 
					begin 
						IF @WeeklyFullBackupDay = 'Monday'
							begin 
								SELECT @LastWeekOfMonth = DATEADD(day,DATEDIFF(day,'19000101',DATEADD(month,DATEDIFF(MONTH,0,GETDATE() /*YourValuehere*/),30))/7*7,'19000101')
							end
						IF @WeeklyFullBackupDay = 'Tuesday'
							begin 
								SELECT @LastWeekOfMonth = DATEADD(day,DATEDIFF(day,'19000102',DATEADD(month,DATEDIFF(MONTH,0,GETDATE() /*YourValuehere*/),30))/7*7,'19000102')
							end
						IF @WeeklyFullBackupDay = 'Wednesday'
							begin 
								SELECT @LastWeekOfMonth = DATEADD(day,DATEDIFF(day,'19000103',DATEADD(month,DATEDIFF(MONTH,0,GETDATE() /*YourValuehere*/),30))/7*7,'19000103')
							end
						IF @WeeklyFullBackupDay = 'Thursday'
							begin 
								SELECT @LastWeekOfMonth = DATEADD(day,DATEDIFF(day,'19000104',DATEADD(month,DATEDIFF(MONTH,0,GETDATE() /*YourValuehere*/),30))/7*7,'19000104')
							end
						IF @WeeklyFullBackupDay = 'Friday'
							begin 
								SELECT @LastWeekOfMonth = DATEADD(day,DATEDIFF(day,'19000105',DATEADD(month,DATEDIFF(MONTH,0,GETDATE() /*YourValuehere*/),30))/7*7,'19000105')
							end
						IF @WeeklyFullBackupDay = 'Saturday'
							begin 
								SELECT @LastWeekOfMonth = DATEADD(day,DATEDIFF(day,'19000106',DATEADD(month,DATEDIFF(MONTH,0,GETDATE() /*YourValuehere*/),30))/7*7,'19000106')
							end
						IF @WeeklyFullBackupDay = 'Sunday'
							begin 
								SELECT @LastWeekOfMonth = DATEADD(day,DATEDIFF(day,'19000107',DATEADD(month,DATEDIFF(MONTH,0,GETDATE() /*YourValuehere*/),30))/7*7,'19000107')
							end
					end 

                SET @TSQL_LinkServer = 	'''' + 
                                                'select		database_name, physical_device_name
                                                    from   (SELECT	b.[database_name], f.physical_device_name
                                                            FROM    [msdb].[dbo].[backupset] AS [b]
                                                            LEFT JOIN  msdb.dbo.backupmediafamily f 
                                                                    ON b.media_set_id = f.media_set_id
                                                            where	CAST(backup_finish_date AS date) = '+'''''' +  @BackupDateTxt + ''''') as t'
                                        + ''')' ;

                SET @TSQL_Local = 		'select database_name, physical_device_name
                                            from   (SELECT	b.[database_name], f.physical_device_name
                                                    FROM    [msdb].[dbo].[backupset] AS [b]
                                                    LEFT JOIN  msdb.dbo.backupmediafamily f 
                                                            ON b.media_set_id = f.media_set_id
                                            where	CAST(backup_finish_date AS date) = '+'''' +  @BackupDateTxt + '''
                                        ) as t';

            ----------------------------------------------------------------------------------------------------------------
            -- Backup scope defination
            ----------------------------------------------------------------------------------------------------------------

                --Inserting Local Server data into TempTable 
                IF NOT EXISTS (select 1 from (select value from string_split(@ExcludeServers, ';')) as t where t.value = @@SERVERNAME)
                BEGIN
                    Insert into #TempBackupLog (DatabaseName, BackupFileName)
                    Exec (@TSQL_Local)
                    Print Format (getdate(), 'yyyy-MMM-dd HH:mm:ss')+ ' Local Server data loaded into TempTable; ';
                END 

                ----- Target Server Grouping based on their destination shared folder ------------
                IF @IncludeServers is not null ---- While there is value in @IncludeServers parameter no other servers processed.
                BEGIN
                    insert into @TargetServers (LinkServer) select value from string_split(@IncludeServers, ';')
                END 


                IF @ExcludeServers is null and @IncludeServers is null 
                BEGIN 
                    Insert into @TargetServers select server_id, name from sys.servers where is_linked = 1 and provider in ('SQLNCLI', 'MSOLEDBSQL', 'SQLOLEDB');
                END 

                IF @ExcludeServers is not null and @IncludeServers is null 
                BEGIN
                    Insert into @TargetServers 
                        select server_id, name 
                        from sys.servers 
                        where is_linked = 1 and provider in ('SQLNCLI', 'MSOLEDBSQL', 'SQLOLEDB')
                                and name not in (select value from string_split(@ExcludeServers, ';'));	
                END 

            ---- Start the Loop to prepare staging data from all target servers 

                While exists (select * from @TargetServers) 
                BEGIN 
                    BEGIN TRY 
                        SET @LinkedServer = (select top 1 LinkServer from @TargetServers)
                        SET @OPENQUERY = 'SELECT * FROM OPENQUERY(['+ @LinkedServer + '],'

                        -- Inserting linked Server data into Temp Table 
                                Insert into #TempBackupLog (DatabaseName, BackupFileName)
                                EXEC (@OPENQUERY + @TSQL_LinkServer);

                        Print Format (getdate(), 'yyyy-MMM-dd HH:mm:ss') + ' Linked Server "'+ @LinkedServer +' " data loaded into TempTable; ';
                        Delete from @TargetServers where LinkServer = @LinkedServer
                    END TRY 
                    
                    BEGIN CATCH 
                        PRINT 'ERROR occured while collecting data from ' + @LinkedServer + ' Error: ' + ERROR_MESSAGE();
                        Delete from @TargetServers where LinkServer = @LinkedServer
                    END CATCH 
                END
            
       --      select * from #TempBackupLog;

            IF @DatabaseName is not null 
                begin
                    Delete from #TempBackupLog where DatabaseName <> @DatabaseName;
                    Print '#TempBackupLog cleaned to keep one database scope: ' + cast (@@ROWCOUNT as varchar(5));
                end

            ------- For 2nd while loop scope -----------------------------------------------------------------------------
            IF @DatabaseName is null 
                begin 
                    insert into @TargetDatabases select distinct DatabaseName from #TempBackupLog;
                end
            IF @DatabaseName is not null 

                begin 
                    insert into @TargetDatabases select @DatabaseName;
                end
            SET @ShareDrive = @BackupDestination;
            ------- For 2nd while loop scope end -----------------------------------------------------------------------------

            ----------------------------------------------------------------------------------------------------------------
            -- END: Backup scope defination
            ----------------------------------------------------------------------------------------------------------------


            ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            -- Moving Files into dated folderes
            ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
			        ----- 1st Loop
                    While exists (select * from #TempBackupLog)
                        BEGIN
                            BEGIN TRY 
                            SET		@BackupFileID = (SELECT top 1 FileID FROM #TempBackupLog order by DatabaseName);
                            SET		@BackupDBName = (SELECT top 1 DatabaseName FROM #TempBackupLog WHERE FileID = @BackupFileID);
                            SET		@BackupFileName =(SELECT TOP 1 BackupFileName FROM #TempBackupLog where FileID = @BackupFileID);
                            SELECT	@MoveSrcFolder = (LEFT (@BackupFileName, (LEN(@BackupFileName)) - CHARINDEX('\', REVERSE(@BackupFileName) ))) ;
                            
                            --- Custom destination location 
                            SELECT	@MoveDstFolder = @BackupDestination + @BackupDBName + '\' + @BackupDateTxt ;
                            SELECT	@MoveFile = RIGHT(@BackupFileName, 
                                                        CASE WHEN	CHARINDEX('\', REVERSE(@BackupFileName) ) IS NULL or 
                                                                    CHARINDEX('\', REVERSE(@BackupFileName) ) = 0 or
                                                                    CHARINDEX('\', REVERSE(@BackupFileName) ) = 1 
                                                            THEN NULL 
                                                            ELSE CHARINDEX('\', REVERSE(@BackupFileName) )-1
                                                        END);
                            
                            SET @MoveCommand = 'robocopy "' + @MoveSrcFolder + '" "' + @MoveDstFolder + '" "' + @MoveFile + '" /MOV';

                            Print CONVERT(VARCHAR(20),GETDATE(),120) + ' executing command: ' +  @MoveCommand +';'
                            EXEC xp_cmdshell @MoveCommand;

                            delete from #TempBackupLog where FileID = @BackupFileID;
                            END TRY 

                        BEGIN CATCH 
                            PRINT 'ERROR occured while moving files into dated folder command "' + @MoveCommand + '",  Error: ' + ERROR_MESSAGE();
                            delete from #TempBackupLog where FileID = @BackupFileID;
                        END CATCH 
                        
                        END 
                ---- 1st while loop end 

            IF @Compress = 1 
            BEGIN  
                ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
                -- Once backups moved into dated folders, the compressing loop (.Zip) starts inside dated folders, and delete after compression
                ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
                ----- 2nd Loop
                While exists (select * from @TargetDatabases)
                    BEGIN 
                        BEGIN TRY 
                            SELECT top 1 @DatabaseName = DatabaseName from @TargetDatabases;
                            SELECT @ZipName = (@ShareDrive + @DatabaseName + '\' + @BackupDateTxt + '\' + @DatabaseName + '_' + @BackupDateTxt + '.zip');
                            SET @ZipCommand = ('"C:\Program Files\7-Zip\7z.exe" a -p7**5=35i$tru# -tzip ' + @ZipName + ' ' + @ShareDrive + @DatabaseName + '\' + @BackupDateTxt + '\*.*  Switch -mm=copy -sdel -mmt=off');

                            Print CONVERT(VARCHAR(20),GETDATE(),120) + ' executing command: ' + replace(@ZipCommand, '-p7**5=35i$tru#', '-p <Password> ') + '; ';
                            Insert into @CommandResult 
                            exec xp_cmdshell @command_string = @ZipCommand;

                            if exists (Select * from @CommandResult where Result like '%Everything is Ok%') 
                                begin
                                    Print FORMAT(GETDATE(), 'yyyy-MMM-dd HH:mm:ss') + ' Everything is Ok:';
                                End
                            else 
                                begin
                                    Print FORMAT(GETDATE(), 'yyyy-MMM-dd HH:mm:ss') + @DatabaseName + '; ';
                                    Select Result 
                                    from @CommandResult where Result not like '%Everything is Ok%'
                                end

                                --------------------------------------------------------------------------------------------------------------------
                                    set @SearchKeyDel = (select	convert (nvarchar (8), DATEADD (DAY, (@RetentionDays * -1), getdate ()), 112) );
                                    select @MonthlyCopyFiles =	 'mkdir "' + @ShareDrive + @DatabaseName +  '\' + FORMAT (DATEADD(MONTH,-1,GETDATE()), 'yyyy-MMM' ) + '\" & ' + 
                                                                'copy "' + @ShareDrive + @DatabaseName +  '\*' + convert(varchar(10), EOMONTH (DATEADD(MONTH,-1,GETDATE())), 112) + '*FULL*.*" "' +
                                                                @ShareDrive + @DatabaseName +  '\' + FORMAT (DATEADD(MONTH,-1,GETDATE()), 'yyyy-MMM' ) + '\"'; --- Copies last month-end FULL backup files into month folder "yyyy-MMM"

                                    select @MonthlyCopyFiles2 =	 'mkdir "' + @ShareDrive + @DatabaseName +  '\' + FORMAT (DATEADD(MONTH,-1,GETDATE()), 'yyyy-MMM' ) + '\" & ' + 
                                                                'copy "' + @ShareDrive + @DatabaseName +  '\*' + convert(varchar(10), @LastWeekOfMonth, 112) + '*FULL*.*" "' +
                                                                @ShareDrive + @DatabaseName +  '\' + FORMAT (DATEADD(MONTH,-1,GETDATE()), 'yyyy-MMM' ) + '\"'; --- Copies last month-end FULL backup files into month folder "yyyy-MMM"

                                    select @MonthlyCopyFolder = 'robocopy "' + @ShareDrive + @DatabaseName +  '\' + convert(varchar(10), EOMONTH (DATEADD(MONTH,-1,GETDATE())), 112) +  '" "' +
                                                                @ShareDrive + @DatabaseName +  '\' + FORMAT (DATEADD(MONTH,-1,GETDATE()), 'yyyy-MMM' ) + '" /MOVE'; --- Copies last month-end backup files into month folder "yyyy-MMM"

                                    select @MonthlyCopyFolder2 = 'robocopy "' + @ShareDrive + @DatabaseName +  '\' + convert(varchar(10), @LastWeekOfMonth, 112) +  '" "' +
                                                                @ShareDrive + @DatabaseName +  '\' + FORMAT (DATEADD(MONTH,-1,GETDATE()), 'yyyy-MMM' ) + '" /MOVE'; --- Copies last month-end backup files into month folder "yyyy-MMM"

                                    Select @DelCommand = 'Del "' + @ShareDrive + @DatabaseName + '\*' + @SearchKeyDel + '*.*" /F /Q'; --- Deletes files that named with older than 30 days or as per Retention parameter value
                                    Select @DelCommand2 = 'rmdir "' + @ShareDrive + @DatabaseName + '\' + @SearchKeyDel + '" /S /Q'; --- Deletes folders that named with older than 30 days or as per Retention parameter value
                            
                                    Print FORMAT(GETDATE(), 'yyyy-MMM-dd HH:mm:ss') + ' Executing: ' + @MonthlyCopyFiles + '; ';
                                    Print @MonthlyCopyFolder;
                                    Exec xp_cmdshell @MonthlyCopyFiles;
                                    Exec xp_cmdshell @MonthlyCopyFolder;

                                    Print FORMAT(GETDATE(), 'yyyy-MMM-dd HH:mm:ss') + ' Executing (Old files cleanup): ' + @DelCommand + '; ';
                                    Print @DelCommand2;
                                    Exec xp_cmdshell @DelCommand;
                                    Exec xp_cmdshell @DelCommand2;

                            DELETE FROM @TargetDatabases WHERE DatabaseName = @DatabaseName;
                            DELETE FROM @CommandResult;
                        END TRY 

                        BEGIN CATCH 
                            PRINT 'ERROR occured " ' + ERROR_MESSAGE() + ' while executing following commands: ' + '
                            ' +
                            'Command1: ' +  @DelCommand + '
                            ' +
                            'Command2: ' +  @DelCommand2 + '
                            ' +
                            'Command3: ' +  @ZipCommand + '
                            '+
                            'Command4: ' +  @MonthlyCopyFiles + '
                            '+
                            'Command5: ' +  @MonthlyCopyFolder 
                            ;
                            DELETE FROM @TargetDatabases WHERE DatabaseName = @DatabaseName;
                            delete from @CommandResult;
                        END CATCH 
                    END 
                ---- 2nd Loop end
            END

        IF @Compress = 0 ---- Without Compressing the Backup files into .zip file, all backup files moved into dated folders and month folders, and deleted as per retention period 
        BEGIN
            While exists (select * from @TargetDatabases)
            BEGIN
                BEGIN TRY 
                    SELECT top 1 @DatabaseName = DatabaseName from @TargetDatabases;
                    --- Daily copy Job: Copies latest backup files into dated folder "yyyyMMdd" --------------
                    select @ZipCommand = ('mkdir "' + @ShareDrive + @DatabaseName + '\' + @BackupDateTxt  + '\" & ') + ---- Copy command DESTINATION folder creation 
                                        ('copy "' + @ShareDrive + @DatabaseName + '\*' + @BackupDateTxt + '*.*" ' ) +  ---- Copy command SOURCE Path 
                                        ('"' + @ShareDrive + @DatabaseName + '\' + @BackupDateTxt  + '\"' ); --- Copy command DESTINATION path 

                    --- Monthly Copy Job --------------
                        set @SearchKeyDel = (select	convert (nvarchar (8), DATEADD (DAY, (@RetentionDays * -1), getdate ()), 112) );
                        select @MonthlyCopyFiles =	 'mkdir "' +  @ShareDrive + @DatabaseName +  '\' + FORMAT (DATEADD(MONTH,-1,GETDATE()), 'yyyy-MMM' ) + '\" & ' + 
                                                    'copy "' + @ShareDrive + @DatabaseName +  '\*' + convert(varchar(10), EOMONTH (DATEADD(MONTH,-1,GETDATE())), 112) + '*.*" "' +
                                                    @ShareDrive + @DatabaseName +  '\' + FORMAT (DATEADD(MONTH,-1,GETDATE()), 'yyyy-MMM' ) + '\"'; --- Copies last month-end backup files into month folder "yyyy-MMM"

                        select @MonthlyCopyFolder = 'robocopy "' + @ShareDrive + @DatabaseName +  '\' + convert(varchar(10), EOMONTH (DATEADD(MONTH,-1,GETDATE())), 112) +  '" "' +
                                                    @ShareDrive + @DatabaseName +  '\' + FORMAT (DATEADD(MONTH,-1,GETDATE()), 'yyyy-MMM' ) + '" /MOVE'; --- Copies last month-end backup files into month folder "yyyy-MMM"

                        Select @DelCommand = 'Del "' + @ShareDrive + @DatabaseName + '\*' + @SearchKeyDel + '*.*" /F /Q'; --- Deletes files that named with older than 30 days or as per Retention parameter value
                        Select @DelCommand2 = 'rmdir "' + @ShareDrive + @DatabaseName + '\' + @SearchKeyDel + '" /S /Q'; --- Deletes folders that named with older than 30 days or as per Retention parameter value
                
                        Print FORMAT(GETDATE(), 'yyyy-MMM-dd HH:mm:ss') + ' Executing: ' + @ZipCommand + '; ';
                        Exec xp_cmdshell @ZipCommand;

                        Print FORMAT(GETDATE(), 'yyyy-MMM-dd HH:mm:ss') + ' Executing: ' + @MonthlyCopyFiles + '; ';
                        Print @MonthlyCopyFolder;
                        Exec xp_cmdshell @MonthlyCopyFiles;
                        Exec xp_cmdshell @MonthlyCopyFolder;

                        Print FORMAT(GETDATE(), 'yyyy-MMM-dd HH:mm:ss') + ' Executing (Old files cleanup): ' + @DelCommand + '; ';
                        Print @DelCommand2;
                        Exec xp_cmdshell @DelCommand;
                        Exec xp_cmdshell @DelCommand2;

                    DELETE FROM @TargetDatabases WHERE DatabaseName = @DatabaseName;
                    DELETE FROM @CommandResult;
                END TRY 

                BEGIN CATCH 
                    PRINT 'ERROR occured " ' + ERROR_MESSAGE() + ' while executing following commands: ' + '
                    '+
                            'Command1: ' +  @DelCommand + '
                            ' +
                            'Command2: ' +  @DelCommand2 + '
                            ' +
                            'Command3: ' +  @ZipCommand + '
                            '+
                            'Command4: ' +  @MonthlyCopyFiles + '
                            '+
                            'Command5: ' +  @MonthlyCopyFolder 
                            ;
                    DELETE FROM @TargetDatabases WHERE DatabaseName = @DatabaseName;
                    DELETE FROM @CommandResult;
                END CATCH
            END  
        END

        ---- Reverting Advanced settings that were enable at the begin...
            exec sp_configure 'xp_cmdshell', 0;
            Reconfigure;

            exec sp_configure 'show advanced options', 0;
            Reconfigure;

        ----Clean Temp Tables 
            Drop table #TempBackupLog;

    END

END
