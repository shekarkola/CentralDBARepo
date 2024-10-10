USE [DBAClient]
GO

/*-- ==================================================================================
-- Author:			Shekar Kola
-- Create date:		2019-06-09
-- Modified date:	2024-05-29
-- Description:		Defragment indexes daily basis, this procedure called by SQL Agent

	2024-05-29
		- hobt_id depedency added thus it can only work SQL Server 2016 or + 
-- ==================================================================================*/
CREATE OR ALTER PROCEDURE [dbo].[IndexMaintenance]
		@DatabaseName varchar (128) = null,
		@PageCount varchar(6) = 990,
		@ExcludeDatabases nvarchar(4000) = null,
		@mode tinyint = 1 --- 1 = Extract Only, 2 = Execute
AS
BEGIN
	
	SET NOCOUNT ON;

	Declare @TotalCount int,
		@TableName varchar (256),
		@SchemaName varchar (256),
		@IndexName varchar (256),
		@FragmentPercent float,
		@IndexType tinyint, 
		@IsPrimary bit,
		@AlterIndexCmd varchar (1000);


		Declare @ErrorMessage varchar(1000);

		Declare @is_DB_HADREnabled bit;
		Declare @isPrimaryReplica bit;

		Declare @TargetDb table (DBName varchar (126) );
--------------------------------------------------------------------------------------------------------------------------------
	IF @PageCount IS NULL
		BEGIN
			SET @PageCount = 990;
		END
	IF @DatabaseName is null and @ExcludeDatabases is not null 
		begin
			Insert into @TargetDb
			select	name 
			from	sys.databases 
			where	name not in ('master', 'model') and is_read_only = 0
					and name not in (select value from string_split(@ExcludeDatabases, ',') )
		end

	IF @DatabaseName is null and @ExcludeDatabases is null 
		begin
			Insert into @TargetDb
			select	name 
			from	sys.databases 
			where	name not in ('master', 'model') and is_read_only = 0
		end

	else 

		begin
			Insert into @TargetDb
			select	@DatabaseName 
		end

--------------------------------------------------------------------------------------------------------------------------------
	Begin
		Declare @Tbl table (DatabaseName varchar (126),
						SchemaName varchar (126),
						TableName varchar (250), 
						IndexName varchar (250), 
						FragmentPercent numeric(6,2), 
						IndexType tinyint, 
						IsPrimaryKey bit, 
						Fill_Factor smallint, 
						Page_Count int,
						User_Reads int,
						System_Reads int,
						Write_Percent numeric(6,2),
						Read_Percent numeric(6,2),
						PageSplitsTotal bigint,
						RowLock_Count bigint,
						RowLock_AvgWait_ms bigint,
						PageLockCount bigint,
						PageLock_AvgWait_ms bigint,
						PageLatch_AvgWait_ms bigint
						);

----------- DB Level Loop ------------------------------------------------------------------------------------------------------------------------------
	WHILE exists (SELECT * FROM @TargetDb)
		BEGIN
		SET @DatabaseName = (SELECT TOP 1 DBName FROM @TargetDb);
	
		---------------------------------------------------------------------------Verify Database joined AG---------------------------------
		Select @is_DB_HADREnabled = IIF(group_database_id IS NULL, 0,1) from sys.databases where [name] = @DatabaseName;
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

		IF @is_DB_HADREnabled = 0 or @isPrimaryReplica = 1
		-----------------------------------------------------Verify Database joined AG -----------------------------------------------------------------------------
			BEGIN
				BEGIN TRY 
					Print CONVERT(VARCHAR(20), GETDATE(),120) + ' DB level loop srarted for [' + @DatabaseName + '] ;' ;

				INSERT INTO @Tbl 

				exec ('use ' + @DatabaseName + 
						' select	DB_NAME(s.database_id) as DatabaseName,
									sch.name as SchemaName,
									object_name (s.object_id) as TableName, 
									I.name, 
									s.avg_fragmentation_in_percent, 
									I.type, 
									I.is_primary_key, 
									I.fill_factor,
									S.Page_Count,
									(user_scans + user_seeks + user_lookups) as User_Reads,
									(system_scans + system_seeks + system_lookups) as system_Reads,
									(user_updates + 0.0) / 
									IIF( (user_scans + user_seeks + user_lookups + user_updates)=0, NULL, (user_scans + user_seeks + user_lookups + user_updates) ) * 100 as Write_Percent,
									(user_scans + user_seeks + user_lookups + 0.0) / 
									IIF( (user_scans + user_seeks + user_lookups + user_updates)=0, NULL, (user_scans + user_seeks + user_lookups + user_updates) ) * 100 as Read_Percent

									, (ops.leaf_allocation_count + ops.nonleaf_allocation_count) as PageSplitsTotal
									, (ops.row_lock_count) RowLock_Count
									, (ops.row_lock_wait_in_ms / IIF(ops.row_lock_wait_count = 0, 1, ops.row_lock_wait_count) ) RowLock_AvgWait_ms
									, ops.page_lock_count as PageLockCount
									, (ops.page_lock_wait_in_ms/ IIF(ops.page_lock_wait_count =0,1,ops.page_lock_wait_count) ) as PageLock_AvgWait_ms
									, (ops.page_latch_wait_in_ms/ IIF(ops.page_latch_wait_count=0,1,ops.page_latch_wait_count)) as PageLatch_AvgWait_ms
						from sys.dm_db_index_physical_stats (DB_ID (), NULL, NULL, NULL, ''LIMITED'') as S
							left join sys.internal_partitions as inp 
									on S.object_id = inp.object_id 
									and S.index_id = inp.index_id
									and S.partition_number = inp.partition_number
							join sys.indexes as I on s.object_id = I.object_id and s.index_id = I.index_id
							join sys.objects as O on s.object_id = O.object_id 
							join sys.schemas as sch on O.schema_id = sch.schema_id
							join sys.dm_db_index_operational_stats (DB_ID(), null, null, null) as ops 
								on	s.database_id = ops.database_id 
									and s.object_id = ops.object_id
									and s.index_id = ops.index_id 
									and s.partition_number = ops.partition_number
									and s.hobt_id = ops.hobt_id
							left join sys.dm_db_index_usage_stats as us 
								on s.database_id = us.database_id 
								and s.object_id = us.object_id
								and s.index_id = us.index_id 
						where i.name is not null and s.avg_fragmentation_in_percent > 2 and S.page_count > '+ @PageCount +'
						ORDER BY  avg_fragmentation_in_percent DESC'
							);
					PRINT 'Target Indexes: ' + CAST (@@ROWCOUNT AS VARCHAR(10)) + ';';
				END TRY
				BEGIN CATCH
						select @ErrorMessage = cast(ERROR_MESSAGE () as varchar(1000));
						Select	CONVERT(VARCHAR(20), GETDATE(),120) + 
								' Error occurred at ' + @DatabaseName + ' Error Message: ' + @ErrorMessage + ' ;';
						DELETE FROM @TargetDb WHERE DBName = @DatabaseName;			
				END CATCH

				--DELETE FROM @TargetDb WHERE DBName = @DatabaseName;
			END

		ELSE
			BEGIN 
				Print	CONVERT(VARCHAR(20), GETDATE(),120) + 
						' The Database [' + @DatabaseName + '] is part of Availability Group and this is not primary replica, Index maintenance not performed ;'
				DELETE FROM @TargetDb WHERE DBName = @DatabaseName;
			END

			Delete from @TargetDb where DBName = @DatabaseName;
		END

		IF @mode = 2
			BEGIN --------> INDEXING LOOP Begins... 
						Print	CONVERT(VARCHAR(20), GETDATE(),120) + ' DB level index maintenance loop srarting for [' + @DatabaseName + '] ;' ;						
						while exists (select TableName from @Tbl)
							BEGIN 
								set @TableName =		(select top 1  TableName from @Tbl);
								set @SchemaName =		(select top 1  SchemaName from @Tbl where TableName = @TableName);
								set @IndexName =		(select top 1  IndexName from @Tbl where TableName = @TableName);
								set @FragmentPercent =  (select max(fragmentPercent) from @Tbl where TableName = @TableName and IndexName = @IndexName);
--								PRINT 'Fragment % ' + cast(@FragmentPercent as varchar(15))+ ' for ' + @TableName + '.' + @IndexName;
								set @IndexType =		(select top 1 IndexType from @Tbl where TableName = @TableName and IndexName = @IndexName);
								set @IsPrimary =		(select top 1 IsPrimaryKey from @Tbl where TableName = @TableName and IndexName = @IndexName);

								IF @FragmentPercent >= 30 and (@IndexType not in (5,6))
									begin
										IF @IsPrimary = 1
										begin
											set @AlterIndexCmd = 'Use ' + @DatabaseName + ' alter index ['+ @IndexName + '] on ['+ @SchemaName +'].[' + @TableName + '] Rebuild WITH (ONLINE = OFF, SORT_IN_TEMPDB = ON); ';
										end

										Else 
										begin
											set @AlterIndexCmd = 'Use ' + @DatabaseName + ' alter index ['+ @IndexName + '] on ['+ @SchemaName +'].[' + @TableName + '] Rebuild WITH (SORT_IN_TEMPDB = ON, ONLINE = ON (WAIT_AT_LOW_PRIORITY ( MAX_DURATION = 2 MINUTES,  ABORT_AFTER_WAIT = SELF))); ';
										end
									end
							
								IF @FragmentPercent < 30  and (@IndexType not in (5,6))
									begin 			
										set @AlterIndexCmd = 'Use ' + @DatabaseName + ' alter index ['+ @IndexName + '] on ['+ @SchemaName +'].[' + @TableName + '] REORGANIZE; ';
										--set @AlterIndexCmd = 'Use ' + @DatabaseName + ' alter index ['+ @SchemaName +'].['+ @IndexName + '] on [' + @TableName + '] REORGANIZE; ';
									end
							
							BEGIN TRY
									PRINT CONVERT(VARCHAR(20), GETDATE(),120) + ' Executing: ' + @AlterIndexCmd;
									EXEC (@AlterIndexCmd);
							END TRY
							BEGIN CATCH
							
									select @ErrorMessage = cast(ERROR_MESSAGE () as varchar(1000));
									Print	CONVERT(VARCHAR(20), GETDATE(),120) + 
												' ' + @ErrorMessage + 
												', Error Occured at "Index : '+ @IndexName + ' on table ' + @TableName + '" ;'; 

									DELETE FROM @Tbl where TableName = @TableName and IndexName = @IndexName;
							END CATCH
							
							DELETE FROM @Tbl where TableName = @TableName and IndexName = @IndexName;
						END
						Print	CONVERT(VARCHAR(20), GETDATE(),120) + ' DB level index maintenance loop end for [' + @DatabaseName + '] ;' ;
			END ----------- INDEXING LOOP ends ------------------------------------------------------------------------------------------------------------------------------
		IF @mode = 1
			BEGIN 
				select * from @Tbl;
			END 
		ELSE 
			PRINT 'Please select valid MODE, 1 = Executing INDEX REBUILD, 2 = Read-only'
	End
	--Else Print 'Its not primary replica, Index Maintainance not performed!'
END
GO 
