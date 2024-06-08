CREATE TABLE customer_dimension (
    customer_id VARCHAR PRIMARY KEY,
    customer_name VARCHAR,
    segment VARCHAR
);

CREATE TABLE product_dimension (
    product_id VARCHAR PRIMARY KEY,
    category VARCHAR,
    sub_category VARCHAR,
    product_name VARCHAR
);

CREATE TABLE supplier_dimension (
    supplier_id INT PRIMARY KEY,
    supplier_name VARCHAR,
    contact_name VARCHAR,
    address VARCHAR,
    city VARCHAR,
    country VARCHAR
);

CREATE TABLE time_dimension (
    time_id SERIAL PRIMARY KEY,
    order_date DATE,
    day INT,
    month INT,
    year INT,
    weekday INT
);

CREATE TABLE location_dimension (
    location_id SERIAL PRIMARY KEY,
    city VARCHAR,
    state VARCHAR,
    postal_code INT,
    region VARCHAR,
    country VARCHAR
);

CREATE TABLE sales_fact (
    sales_id VARCHAR PRIMARY KEY,
    customer_id VARCHAR,
    product_id VARCHAR,
    supplier_id INT,
    time_id INT,
	location_id INT,
    sales DECIMAL(10,2),
    quantity INT,
    discount DECIMAL(5,2),
    profit DECIMAL(10,2),
    FOREIGN KEY (customer_id) REFERENCES customer_dimension(customer_id),
    FOREIGN KEY (product_id) REFERENCES product_dimension(product_id),
    FOREIGN KEY (supplier_id) REFERENCES supplier_dimension(supplier_id),
    FOREIGN KEY (time_id) REFERENCES time_dimension(time_id)
);

ALTER TABLE sales_fact ADD FOREIGN KEY (location_id) REFERENCES location_dimension (location_id);