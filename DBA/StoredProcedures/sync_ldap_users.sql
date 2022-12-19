Create or Alter procedure sync_ldap_users 
@LDAPGroup NVARCHAR(128) = NULL,
@DC1 NVARCHAR(128) = 'pom',  ---- Domain Name 
@DC2 NVARCHAR(128) = 'local' ---- Domain Top Layer

AS 
BEGIN

SET NOCOUNT ON;

DECLARE @SQL NVARCHAR(MAX)
DECLARE @group_dn NVARCHAR(512)
DECLARE @TargetGroups TABLE(name NVARCHAR(512));

IF (select OBJECT_ID ('TempADUsers')) IS NULL 
	BEGIN 
		Create table TempADUsers
		(
		 CN nvarchar(4000),
		 EmployeeID nvarchar(4000),
		 Title nvarchar(4000),
		 SAMAccountName nvarchar(4000),
		 Manager nvarchar(4000),
		 Company nvarchar(4000),
		 Department nvarchar(4000),
		 Email nvarchar(4000),
		 Mobile  nvarchar(4000),
		 WhenCreated  nvarchar(4000),
		 WhenChanged  nvarchar(4000),
		 SAMAccountType  nvarchar(4000),
		 UserAccountControl  nvarchar(4000)
		);
	END 

IF (select OBJECT_ID ('ldap_users')) IS NULL 
	BEGIN 
		CREATE TABLE [dbo].[ldap_users]
		(
			[recid] [int] IDENTITY(1,1) NOT NULL,
			[username] [nvarchar](4000) NULL,
			employee_id nvarchar(100),
			[title] [nvarchar](250) NULL,
			[user_account] [nvarchar](4000) NULL,
			[manager_name] [nvarchar](4000) NULL,
			[user_department] [nvarchar](250) NULL,
			[email] [nvarchar](4000) NULL,
			[mobile_number] [nvarchar](4000) NULL,
			ldap_created_on datetime,
			ldap_modified_on datetime,
			user_status nvarchar(250),
			sma_account_type varchar(250)
		);
	END 

IF @LDAPGroup is null 
	BEGIN 
	SET @SQL = 
	'SELECT distinguishedName
	FROM OPENQUERY
	(ADSI,''SELECT cn, distinguishedName, dc
	FROM ''''LDAP://DC=' + @DC1 + ',DC=' + @DC2 + '''''
	WHERE objectCategory = ''''group'''' '')'
	END 
ELSE 
	BEGIN 
	SET @SQL = 
	'SELECT distinguishedName
	FROM OPENQUERY
	(ADSI,''SELECT cn, distinguishedName, dc
	FROM ''''LDAP://DC=' + @DC1 + ',DC=' + @DC2 + '''''
	WHERE objectCategory = ''''group'''' AND cn = ''''' + @LDAPGroup + ''''''')'
	END 

PRINT @SQL
INSERT @TargetGroups(name)
EXEC sp_executesql @SQL;

TRUNCATE TABLE TempADUsers;

while exists (select 1 from @TargetGroups)

BEGIN 
	SET @group_dn = (SELECT TOP 1 name FROM @TargetGroups);

	SET @SQL =
	'SELECT *
	FROM OPENQUERY (ADSI, ''<LDAP://' + @DC1 + '.' + @DC2 + '>;
	(&(objectCategory=person)(memberOf:1.2.840.113556.1.4.1941:=' + @group_dn + '));
	cn,employeeID,title,sAMAccountName,manager,company,department,mail,mobile,whenCreated,whenChanged,sAMAccountType,userAccountControl;subtree'')
	ORDER BY cn;'

	---- Old
	--'SELECT *
	--FROM OPENQUERY (ADSI, ''<LDAP://' + @DC1 + '.' + @DC2 + '>;
	--(&(objectCategory=person)(memberOf:1.2.840.113556.1.4.1941:=' + @group_dn + '));
	--cn, sAMAccountName, adspath, employeeID, mobile,company,department;subtree'')
	--ORDER BY cn;'

	/*------------------------------------------
	Attribute: userAccountControl
	512 = Enabled
	514 = Disabled
	66048 = Enabled, password never expires
	66050 = Disabled, password never expires
	------------------------------------------*/

	--PRINT @SQL
	INSERT INTO TempADUsers  ----- Ensure the column order of @SQL query is same as "TempADUsers" table 
	EXEC sp_executesql @SQL;

	Delete from @TargetGroups where name = @group_dn;
END 


;WITH adUsers as (
SELECT	distinct
		CN as username 
		,EmployeeID as employee_id
		,Title 
		,SAMAccountName as user_account 
		,REPLACE( LEFT(Manager, ISNULL(CHARINDEX('OU=',Manager,1)-2, 0)), 
				'CN=', ''
				) as Manager
		,Company
		,Department
		,Email
		,Mobile
		,TRY_CAST(WhenCreated as datetime) as created_on 
		,TRY_CAST(WhenChanged as datetime) as modified_on
		,CASE WHEN UserAccountControl = '512' or UserAccountControl = '66048' THEN 'Active'
			  WHEN UserAccountControl = '514' or UserAccountControl = '66050' THEN 'Inactive'
			  ELSE 'Unknown'
		 END as UserStatus
		 ,SAMAccountType
FROM TempADUsers
)

insert into ldap_users ([username], [employee_id], [title], [user_account], [manager_name], [user_department], [email], [mobile_number], [ldap_created_on], [ldap_modified_on], [user_status], [sma_account_type])
select [username], [employee_id], [title], [user_account], [manager], [department], [email], [mobile], [created_on], [modified_on], UserStatus, SAMAccountType
from adUsers as t1
where not exists ( select 1 from ldap_users as t2 where t1.user_account = t2.user_account);


WITH adUsers2 as (
SELECT	distinct
		CN as username 
		,EmployeeID as employee_id
		,Title 
		,SAMAccountName as user_account 
		,REPLACE( LEFT(Manager, ISNULL(CHARINDEX('OU=',Manager,1)-2, 0)), 
				'CN=', ''
				) as Manager
		,Company
		,Department
		,Email
		,Mobile
		,TRY_CAST(WhenCreated as datetime) as created_on 
		,TRY_CAST(WhenChanged as datetime) as modified_on
		,CASE WHEN UserAccountControl = '512' or UserAccountControl = '66048' THEN 'Active'
			  WHEN UserAccountControl = '514' or UserAccountControl = '66050' THEN 'Inactive'
			  ELSE 'Unknown'
		 END as UserStatus
		 ,SAMAccountType
FROM TempADUsers
)

update t1
		set email = t2.Email,
			username = t2.username,
			[employee_id] = t2.employee_id, 
			[title] = t2.Title, 
			[manager_name] = t2.Manager, 
			[user_department] = t2.Department, 
			[mobile_number] = t2.Mobile, 
			[ldap_created_on] = t2.created_on, 
			[ldap_modified_on] = t2.modified_on, 
			[user_status] = t2.UserStatus, 
			[sma_account_type] = t2.SAMAccountType
from ldap_users as t1
join adUsers2 as t2 on t1.user_account = t2.user_account

END
go
