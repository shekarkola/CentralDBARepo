use joget_mbh
go 

--ADD SENSITIVITY CLASSIFICATION to dbo.app_fd_leads.c_title 
--WITH (LABEL = 'Confidential', LABEL_ID = '331f0b13-76b5-2f1b-a77b-def5a73c73c2', INFORMATION_TYPE = 'Name', INFORMATION_TYPE_ID='57845286-7598-22f5-9659-15b24aeb125e')

--ADD SENSITIVITY CLASSIFICATION to dbo.app_fd_leads.c_description 
--WITH (LABEL = 'Confidential', LABEL_ID = '331f0b13-76b5-2f1b-a77b-def5a73c73c2', INFORMATION_TYPE = 'Name', INFORMATION_TYPE_ID='57845286-7598-22f5-9659-15b24aeb125e')

--ADD SENSITIVITY CLASSIFICATION to dbo.app_fd_leads.c_lead 
--WITH (LABEL = 'Confidential', LABEL_ID = '331f0b13-76b5-2f1b-a77b-def5a73c73c2', INFORMATION_TYPE = 'Name', INFORMATION_TYPE_ID='57845286-7598-22f5-9659-15b24aeb125e')

--ADD SENSITIVITY CLASSIFICATION to dbo.app_fd_leads.c_budget 
--WITH (LABEL = 'Confidential', LABEL_ID = '331f0b13-76b5-2f1b-a77b-def5a73c73c2', INFORMATION_TYPE = 'Financial')

--ADD SENSITIVITY CLASSIFICATION to dbo.app_fd_leads.c_est_investment 
--WITH (LABEL = 'Confidential', LABEL_ID = '331f0b13-76b5-2f1b-a77b-def5a73c73c2', INFORMATION_TYPE = 'Financial')


select * from app_fd_led_partners as ipart

ADD SENSITIVITY CLASSIFICATION to dbo.app_fd_led_partners.c_organization 
WITH (LABEL = 'Confidential', LABEL_ID = '331f0b13-76b5-2f1b-a77b-def5a73c73c2', INFORMATION_TYPE = 'Name')

ADD SENSITIVITY CLASSIFICATION to dbo.app_fd_led_partners.c_contact 
WITH (LABEL = 'Confidential', LABEL_ID = '331f0b13-76b5-2f1b-a77b-def5a73c73c2', INFORMATION_TYPE = 'Contact Info');

ADD SENSITIVITY CLASSIFICATION to dbo.app_fd_led_partners.c_name 
WITH (LABEL = 'Confidential', LABEL_ID = '331f0b13-76b5-2f1b-a77b-def5a73c73c2', INFORMATION_TYPE = 'Name')

ADD SENSITIVITY CLASSIFICATION to dbo.app_fd_led_partners.c_email 
WITH (LABEL = 'Confidential', LABEL_ID = '331f0b13-76b5-2f1b-a77b-def5a73c73c2', INFORMATION_TYPE = 'Contact Info')


select * from app_fd_sub_holding;
ADD SENSITIVITY CLASSIFICATION to dbo.app_fd_sub_holding.c_name 
WITH (LABEL = 'Confidential', LABEL_ID = '331f0b13-76b5-2f1b-a77b-def5a73c73c2', INFORMATION_TYPE = 'Name')

select * from app_fd_invest_opportunity;
ADD SENSITIVITY CLASSIFICATION to dbo.app_fd_invest_opportunity.c_description 
WITH (LABEL = 'Confidential', LABEL_ID = '331f0b13-76b5-2f1b-a77b-def5a73c73c2', INFORMATION_TYPE = 'Contact Info');

ADD SENSITIVITY CLASSIFICATION to dbo.app_fd_invest_opportunity.c_holding 
WITH (LABEL = 'Confidential', LABEL_ID = '331f0b13-76b5-2f1b-a77b-def5a73c73c2', INFORMATION_TYPE = 'Name')


----> Execute the output of follwoing script after restoring database, this cleans the data from the apps other than SYE Invest
select 'TRUNCATE TABLE '+ QUOTENAME(name) + ';' 
from sys.tables 
where name not like 'app[_]fd[_]invest%' ---> Invest App
and name not like 'dir%' ---> Core Joget Tables
and name not like 'app[_]fd[_]cmp%' ---> Invest App
and name not like 'app[_]fd[_]io%' ---> Invest App
and name not like 'app[_]fd[_]lead%' ---> Invest App
and name not like 'app[_]fd[_]led%' ---> Invest App
order by name;

select * from sys.sensitivity_classifications