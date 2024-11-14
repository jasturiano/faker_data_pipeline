SELECT 
    ROUND(100.0 * COUNT(*) / NULLIF((SELECT COUNT(*) FROM persons), 0), 2) as percentage
FROM persons
WHERE country = 'Germany' 
AND email_provider LIKE '%gmail.com';
