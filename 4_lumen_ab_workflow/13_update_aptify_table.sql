-- Deleting previous temp Aptify table
TRUNCATE TABLE org_lumen.aptify_temp;
-- TRUNCATE TABLE org_lumen.aptify_contact_temp;

-- Transferring df to Civis table
insert into org_lumen.aptify_temp select * from org_lumen.aptify_refresh;
-- insert into org_lumen.aptify_contact_temp select * from org_lumen.aptify_contact_refresh;