SELECT country, COUNT(*) as count
FROM persons
WHERE email_provider LIKE '%gmail.com'
GROUP BY country
ORDER BY count DESC
LIMIT 3;