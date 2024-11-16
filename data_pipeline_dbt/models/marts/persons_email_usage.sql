WITH total_german_users AS (
    SELECT COUNT(*) as total
    FROM {{ ref('stg_persons') }}
    WHERE country = 'Germany'
),
german_gmail_users AS (
    SELECT COUNT(*) as gmail_count
    FROM {{ ref('stg_persons') }}
    WHERE country = 'Germany' 
    AND email_provider = 'gmail.com'
)
SELECT 
    'Germany' as country,
    gmail_count as gmail_users,
    total as total_users,
    ROUND(CAST(gmail_count AS FLOAT) / NULLIF(CAST(total AS FLOAT), 0) * 100, 2) as gmail_percentage
FROM total_german_users, german_gmail_users