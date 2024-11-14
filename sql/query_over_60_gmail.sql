SELECT COUNT(*)
FROM persons
WHERE email_provider LIKE '%gmail.com'
AND CAST(REGEXP_EXTRACT(age_group, '\[(\d+)', 1) AS INTEGER) >= 60;