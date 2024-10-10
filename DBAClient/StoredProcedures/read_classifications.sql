USE [DBAClient]
GO
/****** Object:  StoredProcedure [dbo].[read_classifications]    Script Date: 10/10/2024 12:04:58 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
ALTER PROCEDURE [dbo].[read_classifications] 
		@DatabaseName nvarchar(128)
AS
BEGIN

SET NOCOUNT ON;

BEGIN 

Declare @ReadClassifications nvarchar(max),
		@ReadColumnInfo nvarchar(max);


select @ReadClassifications = 'Use ' + QUOTENAME(@DatabaseName) + '; ';
set @ReadClassifications =  @ReadClassifications + 
'SELECT
    cast(schema_name(O.schema_id) AS nvarchar(128)) schema_name,
    cast(O.NAME as nvarchar(128)) AS table_name,
    cast (C.NAME AS nvarchar(128)) column_name,
    cast(information_type as  nvarchar(128)) as information_type,
    cast (sensitivity_label as nvarchar(128)) sensitivity_label ,
	cast (usage_type as nvarchar(128)) usage_type
FROM
    ( SELECT	IT.major_id, IT.minor_id, IT.information_type, L.sensitivity_label, usage_type
      FROM	(
            SELECT major_id, minor_id, value AS information_type 
            FROM sys.extended_properties 
            WHERE NAME = ''sys_information_type_name''
			) IT 
        FULL OUTER JOIN
        (
            SELECT major_id, minor_id, value AS sensitivity_label 
            FROM sys.extended_properties 
            WHERE NAME = ''sys_sensitivity_label_name''
        ) L 
        ON IT.major_id = L.major_id AND IT.minor_id = L.minor_id

        FULL OUTER JOIN
        (SELECT major_id, minor_id, value AS usage_type 
            FROM sys.extended_properties 
            WHERE NAME = ''information_usage_type''
        ) UT 
        ON IT.major_id = UT.major_id AND IT.minor_id = UT.minor_id
    ) EP
JOIN sys.objects O
ON  EP.major_id = O.object_id 
JOIN sys.columns C 
ON  EP.major_id = C.object_id AND EP.minor_id = C.column_id;';

--print @ReadClassifications

exec sp_executesql @ReadClassifications;
END

END