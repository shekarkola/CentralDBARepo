-- =============================================
-- Author:		Shekar Kola
-- Create date: 2020-11-01
-- Description:	Generates the random names 
-- =============================================
CREATE OR ALTER PROCEDURE generate_random_names
	-- Add the parameters for the stored procedure here
	@RequiredNames int = 100,
    @Help bit = 0
AS
BEGIN

	SET NOCOUNT ON;

IF @help = 1 
BEGIN 
    Print 'This procedure generates the random names based on publicly available open source API, 
pass the parater value with number required names:

Example: 
    exec generate_random_names @RequiredNames = 1000;'
END 

ELSE 
BEGIN
--For more details on API: https://randomuser.me/documentation
	DECLARE @XML xml;
	DECLARE @Obj int ;
	DECLARE @Result int ;
	DECLARE @HTTPStatus int ;
	DECLARE @ErrorMsg varchar(MAX);
	DECLARE @Response table (response varchar(max));
	DECLARE @URL VARCHAR(8000);
	DECLARE @RequiredRows int;
	DECLARE @GeneratedRows int;

---------------------------------------------------------------------------------------------------------
Print 'API request begins...'
---------------------------------------------------------------------------------------------------------

	IF OBJECT_ID('tempdb..#xml') IS NOT NULL 
		begin 
			DROP TABLE #xml;
		end 
	CREATE TABLE #xml ( XMLvalue XML );

	IF OBJECT_ID('tempdb..#xmlFinal') IS NOT NULL 
		begin
			DROP TABLE #xmlFinal;
		end
	CREATE TABLE #xmlFinal ( full_name varchar(100) );

	SET @RequiredRows = @RequiredNames;
	SET @GeneratedRows = 0;

IF (select value_in_use from sys.configurations where name = 'Ole Automation Procedures') = 0
BEGIN 
	exec sp_configure 'show advanced options', 1;
	reconfigure;

	exec sp_configure 'Ole Automation Procedures', 1;
	reconfigure;

	exec sp_configure 'show advanced options', 0;
	reconfigure;
END 

WHILE @GeneratedRows < @RequiredRows
    BEGIN 
        SELECT @URL = 'https://randomuser.me/api/?format=XML&inc=gender,name,location&results=1000';	-- This doesn't as string is too long
        PRINT @URL;

        EXEC @Result = sp_OACreate 'MSXML2.XMLHttp', @Obj OUT 

        EXEC @Result = sp_OAMethod @Obj, 'open', NULL, 'GET', @URL, false
        EXEC @Result = sp_OAMethod @Obj, 'setRequestHeader', NULL, 'Content-Type', 'application/x-www-form-urlencoded'
        EXEC @Result = sp_OAMethod @Obj, send, NULL, ''
        EXEC @Result = sp_OAGetProperty @Obj, 'status', @HTTPStatus OUT 

        INSERT #xml ( XMLvalue )
        EXEC @Result = sp_OAGetProperty @Obj, 'responseXML.xml';

        INSERT INTO #xmlFinal (full_name)
        SELECT	  y.c.value('(name/first)[1]', 'varchar(50)') + ' ' + 
                y.c.value('(name/last)[1]', 'varchar(50)') as full_name
        FROM #xml x
            CROSS APPLY x.XMLvalue.nodes('/user/results') y(c);

        DELETE FROM #xmlFinal WHERE full_name LIKE '%??%';

        SELECT @GeneratedRows = COUNT (*) FROM #xmlFinal;

        EXEC @Result = sp_OADestroy @Obj;
    END

	exec sp_configure 'show advanced options', 1;
	reconfigure;

	exec sp_configure 'Ole Automation Procedures', 0;
	reconfigure;

	exec sp_configure 'show advanced options', 0;
	reconfigure;

    --- return generates names: 
    select * from #xmlFinal; 
    END
END 
GO


EXEC generate_random_names @Help = 1

EXEC generate_random_names @RequiredNames = 500