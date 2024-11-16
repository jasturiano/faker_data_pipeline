WITH source AS (
    SELECT * FROM {{ source('raw', 'persons') }}
),

staged AS (
    SELECT
        CASE 
            WHEN age >= 18 AND age <= 20 THEN '[18-20]'
            WHEN age > 20 AND age <= 30 THEN '[21-30]'
            WHEN age > 30 AND age <= 40 THEN '[31-40]'
            WHEN age > 40 AND age <= 50 THEN '[41-50]'
            WHEN age > 50 AND age <= 60 THEN '[51-60]'
            WHEN age > 60 AND age <= 80 THEN '[61-80]'
            ELSE NULL
        END as age_group,
        email_provider,
        country,
    FROM source
    WHERE age >= 18 AND age <= 80  -- Filter for valid age range
)

SELECT * FROM staged