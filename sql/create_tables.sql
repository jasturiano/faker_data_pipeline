CREATE TABLE IF NOT EXISTS persons (
    id INTEGER PRIMARY KEY,
    firstname VARCHAR,
    lastname VARCHAR,
    email_provider VARCHAR,
    phone VARCHAR,
    age_group VARCHAR,
    gender VARCHAR,
    country VARCHAR,
    city VARCHAR,
    street VARCHAR,
    zipcode VARCHAR,
    location_masked BOOLEAN
);

-- Create indexes for commonly queried fields
CREATE INDEX IF NOT EXISTS idx_email_provider ON persons(email_provider);
CREATE INDEX IF NOT EXISTS idx_country ON persons(country);
CREATE INDEX IF NOT EXISTS idx_age_group ON persons(age_group);
