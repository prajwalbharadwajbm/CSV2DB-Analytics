CREATE TABLE IF NOT EXISTS regions (
    region_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    CONSTRAINT uk_region_name UNIQUE (name)
);