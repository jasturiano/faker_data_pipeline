SELECT 
    age_group,
    COUNT(*) as total_seniors,
    COUNT(CASE WHEN email_provider = 'gmail.com' THEN 1 END) as gmail_users,
    ROUND(CAST(COUNT(CASE WHEN email_provider = 'gmail.com' THEN 1 END) AS FLOAT) / 
          NULLIF(CAST(COUNT(*) AS FLOAT), 0) * 100, 2) as gmail_percentage
FROM {{ ref('stg_persons') }}
WHERE age_group = '[61-80]'
GROUP BY age_group