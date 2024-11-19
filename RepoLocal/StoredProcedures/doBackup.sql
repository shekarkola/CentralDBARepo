USE [DBAClient]
GO
/****** Object:  StoredProcedure [dbo].[DoBackup_test]    Script Date: 4/19/2023 3:03:48 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*---------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Author:			Shekar Kola
-- Create date:		2022-07-15
-- Description:		Backup script

Version: 20220715
	-	Encryption option enabled 
		
Version: 20201117
	-	Naming of backup file changed, this will help sort by Name in Backup shared location, 
		particularly when we left the backup files in their original location (without moving into dated folders)
		o	Old: AXDB_HOPR-SQLSRV01_ENT1_Full_13.0.5830.85_20201116_05.BAK
		o	New: AXDB_2020111805_HOPR-SQLSRV01_ENT1_Full_13.0.5830.85.BAK
	-	QUOTENAME for print messages added.

Version: 20200602
	-	Table variables changed to temp (#) tables 
	-	Included SQL Instance Name part of Backup file name, 
		this will fix the error that occurs when backup job run for system databases in the same server same time.

Version: 20200325
	-	By default all backup types will be performed WITH CHECKSUM option, this will help us to have corruption free backup files, 
		as each page of the database performed through CHECKSUM during backup (verifies read checksum of database page vs written page in backup file), 
		no additional space needed. CHECKSUMs would also be used during RESTORE VERIFYONLY 

Version: 20200119
	-	Differential backup file extension would be created .BAK Instead of .DIF. This is changed since the DIF word already exists within File name
	-	Always Differential backup would be created on Primary replica (same as FULL)
	-	Multiple database names can be pass into @DatabaseName parameter, database names must be separated with comma (,)

Version: 20191218
	-	@BackupLocation parameter added, shared drive or local drive path in which the backup files must be saved, when is null default location used
	-	Changed default exaction behavior to go help, if no parameters specified help section result would be appeared
	-	There would be sub folder created with database name, the naming conversion of backup file remains same (DBNAME_HOSTNAME_VERSION_BACKUPTYPE_TIMESTAMP.ext)

Version: 20191205
	-	Replaced xp_cmdshell with xp_create_subdir to create sub-directories for backups 
	-	Xp_create_subdir doesn’t require to change any configuration (sp_configure). This will reduce SQL LOG entries which used to appear (xp_cmdshell enable and disable) with earlier version of dobackup, 

Version: 20191204
	-	Added new parameter @ExcludeDatabase, it accepts multiple databases (each database must be separated with comma (,) ), all databases mentioned here would be excluded from the scope.
	-	@DatabaseName remain as it as. Accepts either one database or null. When it’s null all databases are considered (except system DBs)

---------------------------------------------------------------------------------------------------------------------------------------------------------------*/

ALTER   PROCEDURE [dbo].[DoBackup]
	-- Add the parameters for the stored procedure here
		@DatabaseName nvarchar (4000) = Null,
		@BackupType nvarchar (25) = Null,
		@ExcludeDatabase nvarchar (4000) = null,
		@BackupLocation nvarchar(4000) = null,
		@Encrypt bit = 0,
		@Help bit = 0
AS

IF @Help = 1 OR (@DatabaseName is null and @BackupType is null)
	BEGIN
		PRINT 
'/****************************************************************************************************************** 
	Perform BACKUP for all user databases by default, newly added databases automatically included by this procedure. 
	Archival databases may not be considered regular backup job, in which case the database can be exluded by 
	adding their names in in @ExcludeDatabase parameter, multiple names can be added with comma separter
******************************************************************************************************************/';

		SELECT '@Help' as ParameterName, 'Data-type bit, accepts either 1 or 0, when the value 1 passed, help messages printes that your reading now (including messages tab)! default value is 0.' as [Description]
		union all
		SELECT '@DatabaseName' as ParameterName, 'Data-type nvarchar(4000), accepts multiple database names with comma separator, when it NULL all user databases considered as backup target scope'
		union all
		SELECT '@BackupType' as ParameterName, 'Data-type nvarchar(25), accepts anyone of (FULL, FULL_COPY, DIF, LOG). if nothing specified, FULL BACKUP WITH COPYONLY would be considered'
		union all
		SELECT '@ExcludeDatabase' as ParameterName, 'Data-type nvarchar(4000), accepts multiple database names, this can be used if there is any particular database that should not be considered even @DatabaseName = NULL which is usually takes all user databases backup'
		union all
		SELECT '@BackupLocation' as ParameterName, 'Data-type nvarchar(4000), shared drive or local drive path in which the backup files must be saved, when is null default location used'
	END 

ELSE
BEGIN

	SET NOCOUNT ON;

	Declare @ShareDrive nvarchar (4000),
			@ServerName nvarchar (128),
			@InstanceName nvarchar (128),
			@BackupPath nvarchar (250),
			@BackupFilePath nvarchar (500),
			@VersionNumber nvarchar (50),
			@BackupDateTime nvarchar (20),
			@FileExtension nvarchar (5),
			@xpcommand varchar (1000),
			@IsPrefReplica bit;

	Declare @CertName nvarchar(300);
	Declare @BackupCmd varchar(8000);

	Declare @is_DB_HADREnabled bit,
			@is_Server_HADREnabled bit,
			@isPrimaryReplica bit;
	IF (select OBJECT_ID ('tempdb..#BackupTargetDB')) IS NULL 
	BEGIN
		Create TABLE #BackupTargetDB (DBName varchar(128));
	END
	
	IF (select OBJECT_ID ('tempdb..#backuplocation')) IS NULL 
	BEGIN
		Create TABLE #backuplocation (val varchar(100), DefaultLocation varchar(1000));
	END

	--backup location setting -----------------------------------------------------------------------------------------------
	IF @BackupLocation is null 
	begin
		insert into #backuplocation
		EXECUTE [master].dbo.xp_instance_regread N'HKEY_LOCAL_MACHINE', N'SOFTWARE\Microsoft\MSSQLServer\MSSQLServer', N'BackupDirectory';

		select @ShareDrive = DefaultLocation from #backuplocation;
		--'\\10.30.31.57\SysBackup$\DatabaseBackups\Production\';
	end
	else 
	begin
		set @ShareDrive = @BackupLocation;
	end

	IF (select right(@ShareDrive, 1)) <> '\'
		begin
			set @ShareDrive = @ShareDrive + '\';
		end
  --backup location setting  end-----------------------------------------------------------------------------------------------

	IF @DatabaseName is null 
		begin
			insert into #BackupTargetDB select name from sys.databases where database_id > 4 and state = 0;
		end

	IF @DatabaseName is not null  
		begin 
			insert into #BackupTargetDB 
			SELECT LTRIM(t.value) FROM string_split (@DatabaseName, ',') as t;
		end

	IF @ExcludeDatabase is not null 
		begin
			DELETE FROM #BackupTargetDB 
			WHERE DBName in (SELECT LTRIM(t.value) FROM string_split (@ExcludeDatabase, ',') as t );
		end 

		SELECT GETDATE() as StartTime, COUNT (*) as TagetDBs from #BackupTargetDB;
-- =============================================================================================================================
--  backup script - loop 
-- =============================================================================================================================

	WHILE EXISTS (select DBName from #BackupTargetDB)
	BEGIN	
		Declare @BackupTypeName varchar (10);
		-- Parameters assignment ----------------------------------------------------------------------------------------------------------
		SET @DatabaseName = (select top 1 DBName from #BackupTargetDB);
		set @BackupPath = (select  @ShareDrive + @DatabaseName + '\' ) ;

		select @IsPrefReplica = sys.fn_hadr_backup_is_preferred_replica (@DatabaseName);
		select @is_DB_HADREnabled = IIF(group_database_id IS NULL,0,1) from sys.databases where [name] = @DatabaseName;
		select @is_Server_HADREnabled = Cast(SERVERPROPERTY ('Ishadrenabled') as int);

		set @ServerName = (select cast (SERVERPROPERTY ('MachineName') as nvarchar (20)) );
		set @InstanceName = (select @@SERVICENAME);
			
		set @VersionNumber = (select cast (SERVERPROPERTY ('productversion') as nvarchar (20)) );
		--set @FileExtension = '.bak';
		set @BackupDateTime = 
							(	select	convert (nvarchar (20), getdate (), 112) +
										cast (datepart (HH,getdate () ) as nvarchar (2)) + 
										cast (datepart (MINUTE, getdate () ) as nvarchar (2))
							);

  -- Validate is the replica preferred for backup.

		BEGIN
			IF EXISTS (select db.name
						from sys.dm_hadr_database_replica_states as hadr
							join sys.databases as db on hadr.group_database_id = db.group_database_id
						where is_local = 1 and is_primary_replica = 1 and db.name = @DatabaseName
						)
				SET @isPrimaryReplica = 1 
			ELSE 
				SET @isPrimaryReplica = 0 
		END
		-- Parameters assignment end ----------------------------------------------------------------------------------------------------------

		IF (@BackupType like 'LOG%')
				BEGIN
					IF exists (select 1 from sys.databases where [name] = @DatabaseName and recovery_model_desc = 'FULL')
						BEGIN
							-- When database not part of Availability Group @IsPrefReplica woulbe become "1", so backup can be execute Non AG databases
							IF @IsPrefReplica = 1 
							BEGIN
									-- ---------------------------------------------------------------------------------------------------------------
									--  Creating sub-directories if not available 
									-- ---------------------------------------------------------------------------------------------------------------
										BEGIN
											Print CONVERT(VARCHAR(20),GETDATE(),120) + ' Creating directories if not available with server name and DB name; '
	
											EXECUTE master.dbo.xp_create_subdir @BackupPath

											Print CONVERT(VARCHAR(20),GETDATE(),120) + ' Creating directories completed; '
										END
								
								-- Actual Backup command Begins---------------------------------------------------------------------------------------------------------------

								set @BackupTypeName = 'Log_';
								set @BackupFilePath = (@BackupPath + @DatabaseName + '_' + @BackupDateTime + '_' +@ServerName + '_'+ @InstanceName+ '_' + @BackupTypeName + @VersionNumber + '.TRN');

								BACKUP LOG @DatabaseName TO DISK = @BackupFilePath WITH COMPRESSION, CHECKSUM;
								Print CONVERT(VARCHAR(20),GETDATE(),120) +  ' Backup Created as '+ @BackupFilePath + '; '
							END
							ELSE Print CONVERT(VARCHAR(20),GETDATE(),120) + ' Its not preffered replica for the backup; '
						END
					 Else 
					Print CONVERT(VARCHAR(20),GETDATE(),120) + ' Database '+ QUOTENAME(@DatabaseName) +' doesn''t exists or not supported for LOG BACKUP; ' 
				End
		IF (@BackupType = 'FULL') 
				BEGIN
				IF exists (select 1 from sys.databases where [name] = @DatabaseName)
						begin
							-- When database is part of Availability Group, the FULL Backup must run on only primary replica so that it makes DIFFERENTIAL Backups valid
							IF @is_DB_HADREnabled = 0 or @isPrimaryReplica = 1
							BEGIN
									-- ---------------------------------------------------------------------------------------
									--  creating directories if not available with server name and DB name
									-- ---------------------------------------------------------------------------------------
										BEGIN
											Print CONVERT(VARCHAR(20),GETDATE(),120) + ' Creating directories if not available with server name and DB name; '
	
											EXECUTE master.dbo.xp_create_subdir @BackupPath

											Print CONVERT(VARCHAR(20),GETDATE(),120) + ' Creating directories completed; '
										END
						-- Actual Backup command Begins---------------------------------------------------------------------------------------------------------------
								set @BackupTypeName = 'Full_';
								set @BackupFilePath = (@BackupPath + @DatabaseName + '_' + @BackupDateTime + '_' +@ServerName + '_'+ @InstanceName+ '_' + @BackupTypeName + @VersionNumber + '.BAK');
							--- Backup with Encryption 
							IF @Encrypt = 1 and exists (select 1 from master.sys.certificates where subject like '%Backup%')

								BEGIN 
									SET @CertName = (select name from master.sys.certificates where subject like '%Backup%');
									select @BackupCmd = 'BACKUP DATABASE ' + @DatabaseName + ' TO DISK = ''' + @BackupFilePath + ''' WITH COMPRESSION, CHECKSUM, ENCRYPTION (ALGORITHM=AES_256, SERVER CERTIFICATE = ' + @CertName + ')';
									PRINT 'Executing: ' + @BackupCmd;
									Exec (@BackupCmd);
									-- BACKUP DATABASE @DatabaseName TO DISK = @BackupFilePath WITH COMPRESSION, CHECKSUM, ENCRYPTION (ALGORITHM=AES_256, SERVER CERTIFICATE = @CertName);
								END 
							ELSE --- Backup without Encryption 
								BEGIN
									BACKUP DATABASE @DatabaseName TO DISK = @BackupFilePath WITH COMPRESSION, CHECKSUM;
								END
							Print CONVERT(VARCHAR(20),GETDATE(),120) + ' Backup Created as '+ @BackupFilePath + '; '
							END
							ELSE Print CONVERT(VARCHAR(20),GETDATE(),120) + ' Backup NOT Possible here! as this is not primary replica, Full Backup recommened to perform on primary to keep further DIFF Backups valid; '
						END
					Else 
					Print CONVERT(VARCHAR(20),GETDATE(),120) + ' Database '+ QUOTENAME(@DatabaseName) +' doesn''t exists; ' 
				End

		IF (@BackupType like 'FULL[_]%' or @BackupType is null)
				BEGIN
				IF exists (select 1 from sys.databases where [name] = @DatabaseName)
						begin
							-- When database is part of Availability Group, the FULL COPY_ONLY Backup can on only Preffered replica to avoid workload on Primary replica
							IF @IsPrefReplica = 1
							BEGIN

									-- ---------------------------------------------------------------------------------------
									--  creating directories if not available with server name and DB name
									-- ---------------------------------------------------------------------------------------
										BEGIN
											Print CONVERT(VARCHAR(20),GETDATE(),120) + ' Creating directories if not available with server name and DB name; '
	
											EXECUTE master.dbo.xp_create_subdir @BackupPath

											Print CONVERT(VARCHAR(20),GETDATE(),120) + ' Creating directories completed; '
										END

						-- Actual Backup command Begins---------------------------------------------------------------------------------------------------------------
								set @BackupTypeName = 'Full_Copy_';
								set @BackupFilePath = (@BackupPath + @DatabaseName + '_' + @BackupDateTime + '_' +@ServerName + '_'+ @InstanceName+ '_' + @BackupTypeName + @VersionNumber + '.BAK');

							--- Backup with Encryption 
							IF @Encrypt = 1 and exists (select 1 from master.sys.certificates where subject like '%Backup%')
								BEGIN 
									SET @CertName = (select name from master.sys.certificates where subject like '%Backup%');
									select @BackupCmd = 'BACKUP DATABASE ' + @DatabaseName + ' TO DISK = ''' + @BackupFilePath + ''' WITH COMPRESSION, CHECKSUM, ENCRYPTION (ALGORITHM=AES_256, SERVER CERTIFICATE = ' + @CertName + ')';
									PRINT 'Executing: ' + @BackupCmd;
									EXEC (@BackupCmd)
									-- BACKUP DATABASE @DatabaseName TO DISK = @BackupFilePath WITH COMPRESSION, CHECKSUM, COPY_ONLY, ENCRYPTION (ALGORITHM=AES_256, SERVER CERTIFICATE = @CertName);
								END 
							ELSE --- Backup without Encryption 
								BEGIN
									BACKUP DATABASE @DatabaseName TO DISK = @BackupFilePath WITH COMPRESSION, CHECKSUM;
								END
							Print CONVERT(VARCHAR(20),GETDATE(),120) + ' Backup Created as '+ @BackupFilePath + '; '
							
							END
							ELSE 
							Print CONVERT(VARCHAR(20),GETDATE(),120) + ' Its not preferred replica for the backup; '
						end
					Else 
					Print CONVERT(VARCHAR(20),GETDATE(),120) + ' Database '+ QUOTENAME(@DatabaseName) +' doesn''t exist; ' 
				End


		IF @BackupType like 'DIF%'

				BEGIN
				IF exists (select 1 from sys.databases where [name] = @DatabaseName)
						begin
							-- When database is part of Availability Group, the FULL and DIFFERENTIAL Backups must run on only primary replica
							IF @is_DB_HADREnabled = 0 or @isPrimaryReplica = 1
							BEGIN
									-- ---------------------------------------------------------------------------------------
									--  creating directories if not available with server name and DB name
									-- ---------------------------------------------------------------------------------------
										BEGIN
											Print CONVERT(VARCHAR(20),GETDATE(),120) + ' Creating directories if not available with server name and DB name; '
	
											EXECUTE master.dbo.xp_create_subdir @BackupPath

											Print CONVERT(VARCHAR(20),GETDATE(),120) + ' Creating directories completed; '
										END

						-- Actual Backup command Begins---------------------------------------------------------------------------------------------------------------
								set @BackupTypeName = 'Dif_';
								set @BackupFilePath = (@BackupPath + @DatabaseName + '_' + @BackupDateTime + '_' +@ServerName + '_'+ @InstanceName+ '_' + @BackupTypeName + @VersionNumber + '.BAK');

								BACKUP DATABASE @DatabaseName TO DISK = @BackupFilePath WITH DIFFERENTIAL, COMPRESSION, CHECKSUM;
								Print CONVERT(VARCHAR(20),GETDATE(),120) + ' Backup Created as '+ @BackupFilePath +'; ' 
							END
							ELSE Print CONVERT(VARCHAR(20),GETDATE(),120) +  ' Its not preferred replica for the backup; '
						end
					Else 
					Print CONVERT(VARCHAR(20),GETDATE(),120) + ' Database '+ QUOTENAME(@DatabaseName) +' doesn''t exist; ' 
				End
	delete from #BackupTargetDB where DBName = @DatabaseName
	End
-- =============================================================================================================================
--  backup script - loop end
-- =============================================================================================================================
	Drop table #backuplocation;
	Drop table #BackupTargetDB;

	SELECT GETDATE() as EndTime;
END
