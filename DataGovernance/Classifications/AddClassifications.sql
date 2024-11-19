/*----------------------------------------------------------------------------------------------------------------------------------------------------------------
	Following example script is for adding new classification in any user/business databasae, follow the comments for explicit classification types and labels
----------------------------------------------------------------------------------------------------------------------------------------------------------------*/

USE SatelliteApp
go

Declare @schema_name varchar(128) = 'dbo',
		@TableName varchar(128) = 'VisitorsFeedback_Form',
		@ColumnName varchar(128) = 'Name',

		@InfoType varchar(128) = 'Names',  --Financial, Payroll, Names, National ID, Contact Info ....
		@SensitiveLabel varchar(128) = 'Pseudonymize', --- Confidential - Masking, Confidential - Pseudonymize, Financial - Pseudonymize,N/A
		@UsageType varchar(128) = 'Master';  ---- Master, Transactions, N/A

Declare	@reference_statement varchar (2000) = 
 ''	---- This is example query for when @UsageType = 'Master'
-- 'UPDATE AXDB.dbo.DOCUVALUE SET ACCESSINFORMATION = ''xxx'';' --- This is Example query for when "@UsageType = Trnasactional"


IF EXISTS (SELECT * FROM SYS.EXTENDED_PROPERTIES 
			WHERE [name] = 'sys_information_type_name'
				and [major_id] = OBJECT_ID(@TableName) 
				AND [minor_id] = (SELECT [column_id]
									FROM SYS.COLUMNS
									WHERE [name] = @ColumnName
									AND [object_id] = OBJECT_ID(@TableName)
									)
    )
	BEGIN 
	EXEC sp_dropextendedproperty @name = N'sys_information_type_name'
	,  @level0type = N'Schema'
	,  @level0name = @schema_name
	,  @level1type = N'Table'
	,  @level1name = @TableName
	,  @level2type = N'Column'
	,  @level2name = @ColumnName;
	END 

EXEC sp_addextendedproperty @name = N'sys_information_type_name'
,  @value = @InfoType
,  @level0type = N'Schema'
,  @level0name = @schema_name
,  @level1type = N'Table'
,  @level1name = @TableName
,  @level2type = N'Column'
,  @level2name = @ColumnName;
---------------------------------------------------------------------------------------

IF EXISTS (SELECT * FROM SYS.EXTENDED_PROPERTIES 
			WHERE [name] = 'sys_sensitivity_label_name'
				and [major_id] = OBJECT_ID(@TableName) 
				AND [minor_id] = (SELECT [column_id]
									FROM SYS.COLUMNS
									WHERE [name] = @ColumnName
									AND [object_id] = OBJECT_ID(@TableName)
									)
    )
	BEGIN 
	EXEC sp_dropextendedproperty @name = N'sys_sensitivity_label_name'
	,  @level0type = N'Schema'
	,  @level0name = @schema_name
	,  @level1type = N'Table'
	,  @level1name = @TableName
	,  @level2type = N'Column'
	,  @level2name = @ColumnName;
	END 

EXEC sp_addextendedproperty @name = N'sys_sensitivity_label_name'
,  @value = @SensitiveLabel
,  @level0type = N'Schema'
,  @level0name = @schema_name
,  @level1type = N'Table'
,  @level1name = @TableName
,  @level2type = N'Column'
,  @level2name = @ColumnName;
-----------------------------------------------------------------------------------------------------

IF EXISTS (SELECT * FROM SYS.EXTENDED_PROPERTIES 
			WHERE [name] = 'information_usage_type'
				and [major_id] = OBJECT_ID(@TableName) 
				AND [minor_id] = (SELECT [column_id]
									FROM SYS.COLUMNS
									WHERE [name] = @ColumnName
									AND [object_id] = OBJECT_ID(@TableName)
									)
    )
	BEGIN 
	EXEC sp_dropextendedproperty @name = N'information_usage_type'
	,  @level0type = N'Schema'
	,  @level0name = @schema_name
	,  @level1type = N'Table'
	,  @level1name = @TableName
	,  @level2type = N'Column'
	,  @level2name = @ColumnName;
	END 

EXEC sp_addextendedproperty @name = N'information_usage_type'
,  @value = @UsageType
,  @level0type = N'Schema'
,  @level0name = @schema_name
,  @level1type = N'Table'
,  @level1name = @TableName
,  @level2type = N'Column'
,  @level2name = @ColumnName;


IF EXISTS (SELECT * FROM SYS.EXTENDED_PROPERTIES 
			WHERE [name] = 'reference_statement'
				and [major_id] = OBJECT_ID(@TableName) 
				AND [minor_id] = (SELECT [column_id]
									FROM SYS.COLUMNS
									WHERE [name] = @ColumnName
									AND [object_id] = OBJECT_ID(@TableName)
									)
    )
	BEGIN 
	EXEC sp_dropextendedproperty @name = N'reference_statement'
	,  @level0type = N'Schema'
	,  @level0name = @schema_name
	,  @level1type = N'Table'
	,  @level1name = @TableName
	,  @level2type = N'Column'
	,  @level2name = @ColumnName;
	END 
EXEC sp_addextendedproperty @name = N'reference_statement'
,  @value = @reference_statement
,  @level0type = N'Schema'
,  @level0name = @schema_name
,  @level1type = N'Table'
,  @level1name = @TableName
,  @level2type = N'Column'
,  @level2name = @ColumnName;
