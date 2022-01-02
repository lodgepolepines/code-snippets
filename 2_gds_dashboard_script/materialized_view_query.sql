CREATE MATERIALIZED VIEW cwa_employers.employments
AS SELECT date,
    'Employer2'::text AS subsidiary,
        CASE
            WHEN state.district IS NULL THEN '(null)'::text
            ELSE state.district
        END AS district,
        CASE
            WHEN dues.employer_local IS NULL THEN '(null)'::text
            ELSE COALESCE(ltrim(dues.employer_local, '0'::text), merged_locals.merged_local::text)
        END AS local,
        CASE
            WHEN dues.building_city IS NULL THEN '(null)'::text
            ELSE dues.building_city
        END AS work_city,
        CASE
            WHEN dues.building_state IS NULL THEN '(null)'::text
            ELSE dues.building_state
        END AS work_state,
        CASE
            WHEN dues.building_zip5 IS NULL THEN '(null)'::text
            ELSE dues.building_zip5
        END AS work_zip,
        CASE
            WHEN contracts.bu IS NULL THEN '(null)'::text
            ELSE contracts.bu
        END AS bu,
        CASE
            WHEN job_type.job_type_name IS NULL THEN '(null)'::text
            ELSE job_type.job_type_name
        END AS work_function,
        CASE
            WHEN dues.job_title IS NULL THEN '(null)'::text
            ELSE dues.job_title
        END AS job_title,
        CASE
            WHEN cbsa.cbsa_title IS NULL THEN '(null)'::text
            ELSE cbsa.cbsa_title
        END AS cbsa_title,
    count(DISTINCT dues.uuid) AS count
   FROM cwa_employer2.dues
     LEFT JOIN cwa_cwa.merged_locals ON merged_locals.local::text = ltrim(dues.employer_local, '0'::text)
     LEFT JOIN cwa_cwa.state ON state.state::text = dues.building_state
     LEFT JOIN cwa_employer.contracts ON contracts.contract = dues.cwa_contract
     LEFT JOIN cwa_employer.jobs ON jobs.job_title_code = dues.job_title_code
     LEFT JOIN cwa_employer.job_type ON job_type.job_type = jobs.job_type
     LEFT JOIN cwa_employer.fmc ON fmc.fmc_code::text = dues.fmc_code AND fmc.fmc_reason_type::text = dues.fmc_reason_type
     LEFT JOIN cwa_employer.cbsa_zip ON dues.building_zip5 = cbsa_zip.zip
     LEFT JOIN cwa_employer.cbsa ON cbsa_zip.cbsa = cbsa.cbsa_code
  WHERE (fmc.in_barg_unit = true OR fmc.in_barg_unit IS NULL OR dues.fmc_code IS NULL OR dues.fmc_reason_type IS NULL) AND dues.date >= '2017-07-01'::date
  GROUP BY date, 'Employer2'::text, (
        CASE
            WHEN state.district IS NULL THEN '(null)'::text
            ELSE state.district
        END), (
        CASE
            WHEN dues.employer_local IS NULL THEN '(null)'::text
            ELSE COALESCE(ltrim(dues.employer_local, '0'::text), merged_locals.merged_local::text)
        END), (
        CASE
            WHEN dues.building_city IS NULL THEN '(null)'::text
            ELSE dues.building_city
        END), (
        CASE
            WHEN dues.building_state IS NULL THEN '(null)'::text
            ELSE dues.building_state
        END), (
        CASE
            WHEN dues.building_zip5 IS NULL THEN '(null)'::text
            ELSE dues.building_zip5
        END), (
        CASE
            WHEN contracts.bu IS NULL THEN '(null)'::text
            ELSE contracts.bu
        END), (
        CASE
            WHEN job_type.job_type_name IS NULL THEN '(null)'::text
            ELSE job_type.job_type_name
        END), (
        CASE
            WHEN dues.job_title IS NULL THEN '(null)'::text
            ELSE dues.job_title
        END), (
        CASE
            WHEN cbsa.cbsa_title IS NULL THEN '(null)'::text
            ELSE cbsa.cbsa_title
        END)
UNION
 SELECT date,
    'Employer1'::text AS subsidiary,
        CASE
            WHEN state.district IS NULL THEN '(null)'::text
            ELSE state.district
        END AS district,
        CASE
            WHEN dues.employer_local IS NULL THEN '(null)'::text
            ELSE COALESCE(ltrim(dues.employer_local, '0'::text), merged_locals.merged_local::text)
        END AS local,
        CASE
            WHEN dues.building_city IS NULL THEN '(null)'::text
            ELSE dues.building_city
        END AS work_city,
        CASE
            WHEN dues.building_state IS NULL THEN '(null)'::text
            ELSE dues.building_state
        END AS work_state,
        CASE
            WHEN dues.building_zip5 IS NULL THEN '(null)'::text
            ELSE dues.building_zip5
        END AS work_zip,
        CASE
            WHEN contracts.bu IS NULL THEN '(null)'::text
            ELSE contracts.bu
        END AS bu,
        CASE
            WHEN job_type.job_type_name IS NULL THEN '(null)'::text
            ELSE job_type.job_type_name
        END AS work_function,
        CASE
            WHEN dues.job_title IS NULL THEN '(null)'::text
            ELSE dues.job_title
        END AS job_title,
        CASE
            WHEN cbsa.cbsa_title IS NULL THEN '(null)'::text
            ELSE cbsa.cbsa_title
        END AS cbsa_title,
    count(DISTINCT dues.uuid) AS count
   FROM cwa_employer1.dues
     LEFT JOIN cwa_cwa.merged_locals ON merged_locals.local::text = ltrim(dues.employer_local, '0'::text)
     LEFT JOIN cwa_cwa.state ON state.state::text = dues.building_state
     LEFT JOIN cwa_employer.contracts ON contracts.contract = dues.cwa_contract
     LEFT JOIN cwa_employer.jobs ON jobs.job_title_code = dues.job_title_code
     LEFT JOIN cwa_employer.job_type ON job_type.job_type = jobs.job_type
     LEFT JOIN cwa_employer.fmc ON fmc.fmc_code::text = dues.fmc_code AND fmc.fmc_reason_type::text = dues.fmc_reason_type
     LEFT JOIN cwa_employer.cbsa_zip ON dues.building_zip5 = cbsa_zip.zip
     LEFT JOIN cwa_employer.cbsa ON cbsa_zip.cbsa = cbsa.cbsa_code
  WHERE (fmc.in_barg_unit = true OR fmc.in_barg_unit IS NULL OR dues.fmc_code IS NULL OR dues.fmc_reason_type IS NULL) AND dues.date >= '2017-07-01'::date
  GROUP BY date, 'Employer1'::text, (
        CASE
            WHEN state.district IS NULL THEN '(null)'::text
            ELSE state.district
        END), (
        CASE
            WHEN dues.employer_local IS NULL THEN '(null)'::text
            ELSE COALESCE(ltrim(dues.employer_local, '0'::text), merged_locals.merged_local::text)
        END), (
        CASE
            WHEN dues.building_city IS NULL THEN '(null)'::text
            ELSE dues.building_city
        END), (
        CASE
            WHEN dues.building_state IS NULL THEN '(null)'::text
            ELSE dues.building_state
        END), (
        CASE
            WHEN dues.building_zip5 IS NULL THEN '(null)'::text
            ELSE dues.building_zip5
        END), (
        CASE
            WHEN contracts.bu IS NULL THEN '(null)'::text
            ELSE contracts.bu
        END), (
        CASE
            WHEN job_type.job_type_name IS NULL THEN '(null)'::text
            ELSE job_type.job_type_name
        END), (
        CASE
            WHEN dues.job_title IS NULL THEN '(null)'::text
            ELSE dues.job_title
        END), (
        CASE
            WHEN cbsa.cbsa_title IS NULL THEN '(null)'::text
            ELSE cbsa.cbsa_title
        END);