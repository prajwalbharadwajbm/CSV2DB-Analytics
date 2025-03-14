CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    region_id INT NOT NULL,
    sale_date DATE NOT NULL,
    shipping_cost DECIMAL(10, 2) NOT NULL,
    payment_method_id INT NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (region_id) REFERENCES regions(region_id),
    FOREIGN KEY (payment_method_id) REFERENCES payment_methods(payment_method_id)
);

CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_orders_sale_date ON orders(sale_date);
CREATE INDEX idx_orders_region_id ON orders(region_id);