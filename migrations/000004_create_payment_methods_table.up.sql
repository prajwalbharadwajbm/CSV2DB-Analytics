CREATE TABLE IF NOT EXISTS payment_methods (
    payment_method_id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    CONSTRAINT uk_payment_method_name UNIQUE (name)
);