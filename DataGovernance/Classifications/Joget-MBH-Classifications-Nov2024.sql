/* Classifications based on Microsoft Information Protection (MIP) sensitivity labels */


-- Title: Contains descriptive information about the lead or IO
ADD SENSITIVITY CLASSIFICATION TO dbo.app_fd_leads.c_title
  WITH (LABEL = 'Confidential', INFORMATION_TYPE = 'Project Information', RANK = Medium);

-- IO_Description: Describes the opportunity or project
ADD SENSITIVITY CLASSIFICATION TO dbo.app_fd_leads.c_description
  WITH (LABEL = 'Confidential', INFORMATION_TYPE = 'Business Process', RANK = Medium);

-- Vertical Holding: Potentially organizational information
ADD SENSITIVITY CLASSIFICATION TO dbo.app_fd_leads.c_vertical
  WITH (LABEL = 'Internal', INFORMATION_TYPE = 'Business Information', RANK = Low);

-- Lead Person: Name (PII) as it references an individual
ADD SENSITIVITY CLASSIFICATION TO dbo.app_fd_leads.c_lead_owner
  WITH (LABEL = 'Confidential', INFORMATION_TYPE = 'Name', RANK = High);

-- Priority: Typically organizational context
ADD SENSITIVITY CLASSIFICATION TO dbo.app_fd_leads.c_priority
  WITH (LABEL = 'Internal', INFORMATION_TYPE = 'Business Information', RANK = Low);

-- Budget: Financial information
ADD SENSITIVITY CLASSIFICATION TO dbo.app_fd_leads.c_budget
  WITH (LABEL = 'Confidential', INFORMATION_TYPE = 'Financial', RANK = High);

-- Estimated Investment: Financial estimate
ADD SENSITIVITY CLASSIFICATION TO dbo.app_fd_leads.c_est_invest_usd_m
  WITH (LABEL = 'Confidential', INFORMATION_TYPE = 'Financial', RANK = High);

-- Closing Date: Operational information, not typically sensitive
ADD SENSITIVITY CLASSIFICATION TO dbo.app_fd_leads.c_complete_date
  WITH (LABEL = 'Internal', INFORMATION_TYPE = 'Operational Data', RANK = Low);

-- Approval Date: Operational information
ADD SENSITIVITY CLASSIFICATION TO dbo.app_fd_leads.c_approval_date
  WITH (LABEL = 'Internal', INFORMATION_TYPE = 'Operational Data', RANK = Low);

-- Creation Date: Basic metadata
ADD SENSITIVITY CLASSIFICATION TO dbo.app_fd_leads.dateCreated
  WITH (LABEL = 'Internal', INFORMATION_TYPE = 'Operational Data', RANK = Low);


-- Company Visibility: Organizational classification level
ADD SENSITIVITY CLASSIFICATION TO dbo.app_fd_leads.c_com_visibility
  WITH (LABEL = 'Internal', INFORMATION_TYPE = 'Access Control', RANK = Low);

-- EBITDA: Financial performance metric
ADD SENSITIVITY CLASSIFICATION TO dbo.app_fd_leads.c_repo_ltm_ebitda
  WITH (LABEL = 'Confidential', INFORMATION_TYPE = 'Financial', RANK = High);

-- Lead Type: Category of lead
ADD SENSITIVITY CLASSIFICATION TO dbo.app_fd_leads.c_lead_type
  WITH (LABEL = 'Internal', INFORMATION_TYPE = 'Category', RANK = Low);

-- Estimated Closing Date: Future operational planning data
ADD SENSITIVITY CLASSIFICATION TO dbo.app_fd_leads.c_complete_date_est
  WITH (LABEL = 'Internal', INFORMATION_TYPE = 'Operational Data', RANK = Low);

-- Investment Size: Financial data
ADD SENSITIVITY CLASSIFICATION TO dbo.app_fd_leads.c_est_invest_usd_m
  WITH (LABEL = 'Confidential', INFORMATION_TYPE = 'Financial', RANK = High);

go 






-- Date Created: Basic metadata indicating when the entry was created
ADD SENSITIVITY CLASSIFICATION TO dbo.app_fd_led_reporting_lead.dateCreated
  WITH (LABEL = 'Internal', INFORMATION_TYPE = 'Operational Data', RANK = Low);

-- Date Modified: Basic metadata indicating when the entry was last modified
ADD SENSITIVITY CLASSIFICATION TO dbo.app_fd_led_reporting_lead.dateModified
  WITH (LABEL = 'Internal', INFORMATION_TYPE = 'Operational Data', RANK = Low);

-- Created By: User who created the entry (PII)
ADD SENSITIVITY CLASSIFICATION TO dbo.app_fd_led_reporting_lead.createdBy
  WITH (LABEL = 'Confidential', INFORMATION_TYPE = 'Name', RANK = High);

-- Created By Name: Name of the user who created the entry (PII)
ADD SENSITIVITY CLASSIFICATION TO dbo.app_fd_led_reporting_lead.createdByName
  WITH (LABEL = 'Confidential', INFORMATION_TYPE = 'Name', RANK = High);

-- Modified By: User who last modified the entry (PII)
ADD SENSITIVITY CLASSIFICATION TO dbo.app_fd_led_reporting_lead.modifiedBy
  WITH (LABEL = 'Confidential', INFORMATION_TYPE = 'Name', RANK = High);

-- Modified By Name: Name of the user who last modified the entry (PII)
ADD SENSITIVITY CLASSIFICATION TO dbo.app_fd_led_reporting_lead.modifiedByName
  WITH (LABEL = 'Confidential', INFORMATION_TYPE = 'Name', RANK = High);

-- Week Number: Non-sensitive organizational information
ADD SENSITIVITY CLASSIFICATION TO dbo.app_fd_led_reporting_lead.c_week_num
  WITH (LABEL = 'Internal', INFORMATION_TYPE = 'Business Information', RANK = Low);

-- Comments: General comments that may contain sensitive information
ADD SENSITIVITY CLASSIFICATION TO dbo.app_fd_led_reporting_lead.c_comments
  WITH (LABEL = 'Confidential', INFORMATION_TYPE = 'Business Process', RANK = Medium);

-- Decision: Information about decisions made, potentially sensitive
ADD SENSITIVITY CLASSIFICATION TO dbo.app_fd_led_reporting_lead.c_decision
  WITH (LABEL = 'Confidential', INFORMATION_TYPE = 'Business Process', RANK = Medium);

-- Next Step: Information about the next actions, could be sensitive
ADD SENSITIVITY CLASSIFICATION TO dbo.app_fd_led_reporting_lead.c_next_step
  WITH (LABEL = 'Confidential', INFORMATION_TYPE = 'Business Process', RANK = Low);

-- Foreign Key: Reference to another entity, typically internal
ADD SENSITIVITY CLASSIFICATION TO dbo.app_fd_led_reporting_lead.c_fk
  WITH (LABEL = 'Internal', INFORMATION_TYPE = 'Identifier', RANK = Low);

-- Lead ID: References the associated lead
ADD SENSITIVITY CLASSIFICATION TO dbo.app_fd_led_reporting_lead.c_lead_id
  WITH (LABEL = 'Internal', INFORMATION_TYPE = 'Identifier', RANK = Low);

-- Reporting Date: Date of reporting, usually not sensitive
ADD SENSITIVITY CLASSIFICATION TO dbo.app_fd_led_reporting_lead.c_date_reporting
  WITH (LABEL = 'Internal', INFORMATION_TYPE = 'Operational Data', RANK = Low);

-- Company Lead ID: Identifier for the company lead
ADD SENSITIVITY CLASSIFICATION TO dbo.app_fd_led_reporting_lead.c_com_lead_id
  WITH (LABEL = 'Internal', INFORMATION_TYPE = 'Identifier', RANK = Low);
go 




-- ID: Unique identifier for the reporting entry
ADD SENSITIVITY CLASSIFICATION TO dbo.app_fd_led_reporting.id
  WITH (LABEL = 'Internal', INFORMATION_TYPE = 'Identifier', RANK = Medium);

-- Date Created: Basic metadata indicating when the entry was created
ADD SENSITIVITY CLASSIFICATION TO dbo.app_fd_led_reporting.dateCreated
  WITH (LABEL = 'Internal', INFORMATION_TYPE = 'Operational Data', RANK = Low);

-- Date Modified: Basic metadata indicating when the entry was last modified
ADD SENSITIVITY CLASSIFICATION TO dbo.app_fd_led_reporting.dateModified
  WITH (LABEL = 'Internal', INFORMATION_TYPE = 'Operational Data', RANK = Low);

-- Created By: User who created the entry (PII)
ADD SENSITIVITY CLASSIFICATION TO dbo.app_fd_led_reporting.createdBy
  WITH (LABEL = 'Confidential', INFORMATION_TYPE = 'Name', RANK = High);

-- Created By Name: Name of the user who created the entry (PII)
ADD SENSITIVITY CLASSIFICATION TO dbo.app_fd_led_reporting.createdByName
  WITH (LABEL = 'Confidential', INFORMATION_TYPE = 'Name', RANK = High);

-- Modified By: User who last modified the entry (PII)
ADD SENSITIVITY CLASSIFICATION TO dbo.app_fd_led_reporting.modifiedBy
  WITH (LABEL = 'Confidential', INFORMATION_TYPE = 'Name', RANK = High);

-- Modified By Name: Name of the user who last modified the entry (PII)
ADD SENSITIVITY CLASSIFICATION TO dbo.app_fd_led_reporting.modifiedByName
  WITH (LABEL = 'Confidential', INFORMATION_TYPE = 'Name', RANK = High);

-- Comments: General comments that may contain sensitive information
ADD SENSITIVITY CLASSIFICATION TO dbo.app_fd_led_reporting.c_comments
  WITH (LABEL = 'Confidential', INFORMATION_TYPE = 'Business Process', RANK = Medium);

-- Decision: Information about decisions made, potentially sensitive
ADD SENSITIVITY CLASSIFICATION TO dbo.app_fd_led_reporting.c_decision
  WITH (LABEL = 'Confidential', INFORMATION_TYPE = 'Business Process', RANK = Medium);

-- Next Step: Information about the next actions, could be sensitive
ADD SENSITIVITY CLASSIFICATION TO dbo.app_fd_led_reporting.c_next_step
  WITH (LABEL = 'Confidential', INFORMATION_TYPE = 'Business Process', RANK = Low);

-- Lead ID: References the associated lead
ADD SENSITIVITY CLASSIFICATION TO dbo.app_fd_led_reporting.c_lead_id
  WITH (LABEL = 'Internal', INFORMATION_TYPE = 'Identifier', RANK = Low);

-- Reporting Date: Date of reporting, usually not sensitive
ADD SENSITIVITY CLASSIFICATION TO dbo.app_fd_led_reporting.c_date_reporting
  WITH (LABEL = 'Internal', INFORMATION_TYPE = 'Operational Data', RANK = Low);

-- Week Number: Non-sensitive organizational information
ADD SENSITIVITY CLASSIFICATION TO dbo.app_fd_led_reporting.c_week_num
  WITH (LABEL = 'Internal', INFORMATION_TYPE = 'Business Information', RANK = Low);

-- IO Title: Title of the IO, could be sensitive
ADD SENSITIVITY CLASSIFICATION TO dbo.app_fd_led_reporting.c_io_title
  WITH (LABEL = 'Confidential', INFORMATION_TYPE = 'Business Process', RANK = Medium);

-- Company Lead ID: Identifier for the company lead
ADD SENSITIVITY CLASSIFICATION TO dbo.app_fd_led_reporting.c_com_lead_id
  WITH (LABEL = 'Internal', INFORMATION_TYPE = 'Identifier', RANK = Low);
