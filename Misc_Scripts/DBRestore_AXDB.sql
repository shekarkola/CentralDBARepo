USE [master]
GO

declare @backupLocation as nvarchar(4000) = '\\Server\DatabaseBackup\Production\DBName\';
declare @backupfile as nvarchar(255) = ''; --- When it is null the latest FULL backup will be used.

IF @backupfile is null or @backupfile = ''
BEGIN 
	If (Select OBJECT_ID ('tempdb..#File')) is not null
	begin
	drop table #File;
	end

	 CREATE TABLE #File
			(
			FileName    SYSNAME,
			Depth       TINYINT,
			IsFile      TINYINT
			);

	 INSERT INTO #File
			(FileName, Depth, IsFile)

	 EXEC xp_DirTree @backupLocation,1,1
	;

	SELECT TOP 1 
     		@backupfile = @backupLocation + FileName 
	   FROM #File
	  WHERE IsFile = 1
		AND FileName LIKE '%[_]Full[_]%'
		AND FileName LIKE '%.BAK'
	  ORDER BY FileName DESC
END

ELSE 
BEGIN 
	set @backupfile = @backupLocation + @backupfile;
END 

ALTER DATABASE [DBName] SET  SINGLE_USER  WITH ROLLBACK IMMEDIATE  ---- To be changed to Dynamic SQL to Include Database name as parameter

PRINT 'Restore will begin with..' + @backupfile; ---- To be changed to Dynamic SQL to Include Database name as parameter

RESTORE DATABASE [DBName] FROM  DISK = @backupfile WITH  FILE = 1,   ---- To be changed to Dynamic SQL to Include Database name as parameter
MOVE N'DBFile' TO N'D:\SQLData\DEV2\DBFile.mdf', 
MOVE N'DBFile_log' TO N'D:\SQLData\DEV2\DBFile_log.ldf',  
NOUNLOAD,  REPLACE,  STATS = 10;

ALTER DATABASE [DBName] SET  MULTI_USER  WITH ROLLBACK IMMEDIATE ---- To be changed to Dynamic SQL to Include Database name as parameter
Alter database [DBName] set recovery simple; ---- To be changed to Dynamic SQL to Include Database name as parameter

drop table if exists #File;