 SELECT 
   @@SERVERNAME AS ServerName
 , name AS LoginName
 , LOGINPROPERTY(name, 'PasswordLastSetTime') AS PasswordLastSetTime
 , ISNULL(LOGINPROPERTY(name, 'DaysUntilExpiration'), 'Never Expire') AS DaysUntilExpiration
 , ISNULL(CONVERT(VARCHAR(10), DATEADD(DAY, CONVERT(int, LOGINPROPERTY(name, 'DaysUntilExpiration'))
 , CONVERT(DATE, LOGINPROPERTY(name, 'PasswordLastSetTime'))), 101), 'Never Expire') AS PasswordExpirationDate
 , CASE WHEN is_expiration_checked = 1 THEN 'TRUE' ELSE 'FALSE' END AS PasswordExpireChecked
 FROM sys.sql_logins as l
 WHERE l.name not like '##%'
 ORDER BY PasswordLastSetTime DESC;