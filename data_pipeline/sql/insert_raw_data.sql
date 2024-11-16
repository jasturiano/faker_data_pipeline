INSERT INTO persons
WITH ranked_data AS (
    SELECT 
        id,
        --CAST(hash(id::VARCHAR || email || birthday) % 1000000000 AS INTEGER) as id,
        CAST(strftime('%Y', CURRENT_DATE) AS INTEGER) - 
            CAST(strftime('%Y', CAST(birthday AS DATE)) AS INTEGER) AS age,
        CASE 
            WHEN CAST(strftime('%Y', CURRENT_DATE) AS INTEGER) - 
                 CAST(strftime('%Y', CAST(birthday AS DATE)) AS INTEGER) >= 60 THEN '[60+]'
            ELSE '[' || 
                 CAST(
                     ((CAST(strftime('%Y', CURRENT_DATE) AS INTEGER) - 
                       CAST(strftime('%Y', CAST(birthday AS DATE)) AS INTEGER)) / 10 * 10
                 ) AS INTEGER) || 
                 '-' || 
                 CAST(
                     ((CAST(strftime('%Y', CURRENT_DATE) AS INTEGER) - 
                       CAST(strftime('%Y', CAST(birthday AS DATE)) AS INTEGER)) / 10 * 10 + 9
                 ) AS INTEGER) || 
                 ']'
        END AS age_group,
        SPLIT_PART(email, '@', 2) AS email_provider,
        '****' AS masked_name,
        '****' AS masked_contact,
        address['country'] AS country,
        '****' AS masked_city,
        '****' AS masked_address,
        CONCAT(LEFT(address['zipcode'], 2), '***') AS masked_zipcode,
        TRUE AS location_masked,
        ROW_NUMBER() OVER (PARTITION BY id) as rn
        --ROW_NUMBER() OVER (PARTITION BY hash(id::VARCHAR || email || birthday) % 1000000000) as rn
    FROM read_parquet('raw_data/persons.parquet')
)
SELECT 
    id,
    age,
    age_group,
    email_provider,
    masked_name,
    masked_contact,
    country,
    masked_city,
    masked_address,
    masked_zipcode,
    location_masked
FROM ranked_data
WHERE rn = 1 