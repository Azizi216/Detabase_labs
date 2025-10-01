


CREATE TABLE menu_items (
    item_id SERIAL PRIMARY KEY,
    item_name VARCHAR(100),
    category VARCHAR(50),
    base_price DECIMAL(10,2),
    is_available BOOLEAN,
    prep_time_minutes INT
);

CREATE TABLE customer_orders (
    order_id SERIAL PRIMARY KEY,
    customer_name VARCHAR(100),
    order_date DATE,
    total_amount DECIMAL(10,2),
    payment_status VARCHAR(20) DEFAULT 'Pending',
    table_number INT
);

CREATE TABLE order_details (
    detail_id SERIAL PRIMARY KEY,
    order_id INT REFERENCES customer_orders(order_id),
    item_id INT REFERENCES menu_items(item_id),
    quantity INT,
    special_instructions TEXT
);


-- A1: Insert menu item with calculation
INSERT INTO menu_items (item_name, category, base_price, is_available, prep_time_minutes)
VALUES ('Chef Special Burger', 'Main Course', 12.00 * 1.25, TRUE, 20);

-- A2: Insert three customer orders in one statement
INSERT INTO customer_orders (customer_name, order_date, total_amount, payment_status, table_number)
VALUES 
('John Smith', CURRENT_DATE, 45.50, 'Paid', 5),
('Mary Johnson', CURRENT_DATE, 32.00, 'Pending', 8),
('Bob Wilson', CURRENT_DATE, 28.75, 'Paid', 3);

-- A3: Insert order with DEFAULT values
INSERT INTO customer_orders (customer_name, order_date, table_number)
VALUES ('Walk-in Customer', CURRENT_DATE, NULL);




UPDATE menu_items
SET base_price = base_price * 1.08
WHERE category = 'Appetizers';

-- B2: Update category using CASE
UPDATE menu_items
SET category = CASE
    WHEN base_price > 20 THEN 'Premium'
    WHEN base_price BETWEEN 10 AND 20 THEN 'Standard'
    ELSE 'Budget'
END;


UPDATE customer_orders
SET total_amount = total_amount * 0.9,
    payment_status = 'Discounted'
WHERE payment_status = 'Pending';


UPDATE menu_items
SET is_available = FALSE
WHERE item_id IN (
    SELECT item_id
    FROM order_details
    WHERE quantity > 10
);


DELETE FROM menu_items
WHERE is_available = FALSE AND base_price < 5.00;

-- C2: Delete old cancelled orders
DELETE FROM customer_orders
WHERE order_date < '2024-01-01'
  AND payment_status = 'Cancelled';


DELETE FROM order_details
WHERE order_id NOT IN (
    SELECT order_id FROM customer_orders
);


UPDATE menu_items
SET prep_time_minutes = NULL
WHERE category IS NULL
RETURNING item_id, item_name;


INSERT INTO customer_orders (customer_name, order_date, total_amount, payment_status, table_number)
VALUES ('Test Customer', CURRENT_DATE, NULL, DEFAULT, 2)
RETURNING order_id, customer_name;
