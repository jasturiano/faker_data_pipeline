WITH gmail_by_country AS (
    SELECT 
        country,
        COUNT(*) as gmail_users
    FROM {{ ref('stg_persons') }}
    WHERE email_provider = 'gmail.com'
    GROUP BY country
)
SELECT 
    country,
    gmail_users,
    DENSE_RANK() OVER (ORDER BY gmail_users DESC) as rank
FROM gmail_by_country
QUALIFY rank <= 3
ORDER BY gmail_users DESC