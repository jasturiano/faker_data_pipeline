CREATE TABLE IF NOT EXISTS persons (
    id INTEGER PRIMARY KEY,
    age INTEGER,
    age_group VARCHAR,
    email_provider VARCHAR,
    masked_name VARCHAR,
    masked_contact VARCHAR,
    country VARCHAR,
    masked_city VARCHAR,
    masked_address VARCHAR,
    masked_zipcode VARCHAR,
    location_masked BOOLEAN
);

-- Create indexes for commonly queried fields
CREATE INDEX IF NOT EXISTS idx_email_provider ON persons(email_provider);
CREATE INDEX IF NOT EXISTS idx_country ON persons(country);
CREATE INDEX IF NOT EXISTS idx_age_group ON persons(age_group);
