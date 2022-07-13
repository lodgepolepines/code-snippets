insert into org_lumen.barg_membership
with current_file as (select distinct file from org_lumen.barg order by file desc limit 1)
SELECT to_date(substring(barg.file, 1, 4), 'YYMM') as date, barg.firstname, barg.lastname, barg.unionlocal, barg.company, barg.worktourtype, barg.workstreet, barg.workcity, barg.workstate_acronym, t.id, t.statusname, CASE
	WHEN (t.statusname ilike 'Agency Fee Payer%' and (t.statusname ilike '%- Active -%' or t.statusname ilike '%On Leave%')) THEN 'AFP'
	WHEN (t.statusname ilike 'Non Member%' and (t.statusname ilike '%- Active -%' or t.statusname ilike '%On Leave%')) THEN 'Non-Member'
        WHEN (t.statusname ilike 'Member%' and (t.statusname ilike '%- Active -%' or t.statusname ilike '%On Leave%')) THEN 'Member'
        ELSE NULL END AS "member_status",
    barg.empid
FROM org_lumen.barg
LEFT JOIN vwTim_Org_Hash t ON barg.empid = t.clockid
WHERE file = (select * from current_file)