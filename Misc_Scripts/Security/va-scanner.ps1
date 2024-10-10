# Pre-requisites:------------------------------------------------------------------------------------------------------------------------------------------------------
####  to install Powershell Package/Module 
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

##### Install the module, use "A" (Yes to all) when it prompt for confirmation
Install-Module -Name SqlServer 

##### Following may need to be executed before using "sqlserver" module in PowerShell, incase if there is error "running scripts not allowed in this computer" 
Set-ExecutionPolicy RemoteSigned

##### To revert policy settings back 
Set-ExecutionPolicy Restricted
# --------------------------------------------------------------------------------------------------------------------------------------------------------------------




# VA Scanner:-------------------------------------------------------------------------------------------------------------------------------------------------------
$ResultLocation = "C:\MyData\DB Admin\Production\Security\VulnerabilityAssessment\scans\" 
$CurDate = (Get-Date).ToString('yyyyMMdd') + "_"

[System.IO.Directory]::CreateDirectory($ResultLocation) 

@('SERVER01', 'SERVER02', 'SERVER02\INSTANCE1') |
Get-SqlDatabase |
where-Object { ($_.Status -Like "Normal")} | 
ForEach-Object  {
	#Create directory with SQL Server name if it's not exist already  
	#Execute VA Scan....
	Invoke-SqlVulnerabilityAssessmentScan -ServerInstance $_.Parent -Database $_.Name |
	#Save VA Scan results 
	Export-SqlVulnerabilityAssessmentScan -FolderPath "$ResultLocation$(($_.Parent).Name -replace '\\', '_' )\$CurDate$($_.Name)_ScanResult.xlsx"
}
