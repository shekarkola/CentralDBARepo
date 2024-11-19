USE [DBAClient]
GO


-- =============================================
-- Author:			SHEKAR KOLA
-- Create date:		2019-09-03
-- Modified date:	2019-10-03
-- Description:	
-- =============================================
CREATE OR ALTER PROCEDURE [dbo].[DBCC_CHECK]
	-- Add the parameters for the stored procedure here
	@DBName sysname = null,
	@ExcludeDBs sysname = null
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

    -- Insert statements for procedure here
	
		Declare @DBCCommand nvarchar (500);
		Declare @Databases Table (DBName sysname);

		Declare @is_DB_HADREnabled bit;
		Declare @isPrimaryReplica bit;

	IF (@DBName is null)
	BEGIN
	insert into @Databases
		select name
		from sys.databases
		where name not in ('model','tempdb')
				and source_database_id is null 
				and is_read_only = 0
				and state = 0
				and name not in (select value from string_split(@ExcludeDBs, ','));
	END

	IF @DBName is not null 
	BEGIN
		Insert into @Databases 
		select distinct value from string_split(@DBName, ',')
		where value not in (select value from string_split(@ExcludeDBs, ','));
	END
	

	While exists (select * from @Databases)
	begin 
		set @DBName = (select top 1  DBName from @Databases);
		set @DBCCommand = 'DBCC CHECKDB (' ;
		set @DBCCommand = @DBCCommand + (select QUOTENAME(@DBName) + ') with TableResults, NO_INFOMSGS; ' );

		--Validate if database par of AG -----------------------------------------------------------------------------------------------------
			Select @is_DB_HADREnabled = IIF(group_database_id IS NULL, 0,1) from sys.databases where [name] = @DBName;
			BEGIN
				IF EXISTS (select db.name
							from sys.dm_hadr_database_replica_states as hadr
								join sys.databases as db on hadr.group_database_id = db.group_database_id
							where is_local = 1 and is_primary_replica = 1 and db.name = @DBName
							)
					SET @isPrimaryReplica = 1 
				ELSE 
					SET @isPrimaryReplica = 0 
			END
		--Validate if database par of AG -----------------------------------------------------------------------------------------------------
		IF @is_DB_HADREnabled = 0 or @isPrimaryReplica = 1
			BEGIN
				BEGIN TRY 
					Print FORMAT (GETDATE(), 'yyyy-MM-dd HH:MM:ss') + ' DBCC execution Started for '+ @DBName + '; '; 
					Print FORMAT (GETDATE(), 'yyyy-MM-dd HH:MM:ss') + ' Executing... '+ @DBCCommand ; 
					Insert into DBCC_HISTORY 
								(	[Error] ,
									[Level] ,
									[State] ,
									[MessageText] ,
									[RepairLevel] ,
									[Status] ,
									[DbId] ,
									[DbFragId] ,
									[ObjectId] ,
									[IndexId] ,
									[PartitionID] ,
									[AllocUnitID] ,
									[RidDbId] ,
									[RidPruId] ,
									[File] ,
									[Page] ,
									[Slot] ,
									[RefDbId] ,
									[RefPruId],
									[RefFile] ,
									[RefPage] ,
									[RefSlot] ,
									[Allocation] 
								)
					Exec (@DBCCommand);
				END TRY 
		
				BEGIN CATCH
						SELECT 	 ERROR_NUMBER() AS ErrorNumber
								,ERROR_SEVERITY() as ErrorSeverity
								,ERROR_LINE() AS ErrorLine 
								,ERROR_MESSAGE() AS ErrorMessage;
						DELETE FROM @Databases where dbname = @DBName
				END CATCH
				Update DBCC_HISTORY set InstanceFullName = @@SERVERNAME where DbId = DB_ID (@DBName);
				Print FORMAT (GETDATE(), 'yyyy-MM-dd HH:MM:ss') + ' DBCC execution Completed for ' + @DBName + '; '; 
				DELETE FROM @Databases where dbname = @DBName;
			END
			--Validate/execute database par of AG END-----------------------------------------------------------------------------------------------------
		ELSE
		Print FORMAT (GETDATE(), 'yyyy-MM-dd HH:MM:ss') + ' Database [' + @DBName + '] is part of AG and this is not primary replica; '; 
		DELETE FROM @Databases where dbname = @DBName;
	END

END
GO

