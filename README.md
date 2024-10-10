# CentralDBARepo
A Central Repository of Administration & Data Engineering Tools ✔

### DBA\StoredProcedures\\`CompressBackups.sql`
#### To perform compress of backup into .zip using 7zip tool or just organize the backups by moving backup files into dated folder, following parameters accepted:   
   `@DatabaseName`: Optional, when no value passed, all databases from All Linked servers will be targeted.   
   `@BackupDate`: Optional, when no value passed, Yesterday date will be the target.   
   `@BackupDestination`: Mandatory, the destination of the backups can be UNC/Local path.   
   `@ExcludeServers`: Optional, by default all Linked Servers will be the target, incase for any reason anyone or multiple servers wanted to be skipped, it can be added here as Semicolon (;) separated value.   
   `@IncludeServers`: Optional, same as **@ExcludeServers**, Targeting specific Linked Server   
   `@RetentionDays`: Required, default value passed as 32, Backups older than 32 days will be deleted from destination backup folder    
   `@Compress`: Required, accepts bit value (1/0), default value passed as 1, when true (1), backup files within dated folder compressed into .zip format using 7zip tool.   
##### Example:
    Use DBName
    go
    
    exec dba.dbo.CompressBackups 
    @BackupDestination = '\\Server\Backup\Prod\'
    ,@BackupDate = 'yyyyMMdd'
    ,@DatabaseName = 'DBName'
    ,@ExcludeServers = 'SRV01\INT1;SRV01\INT2'
    ,@RetentionDays = 30
    ,@Compress = 0;
