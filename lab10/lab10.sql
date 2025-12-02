DROP TABLE IF EXISTS accounts CASCADE;
DROP TABLE IF EXISTS products CASCADE;

CREATE TABLE accounts (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    balance DECIMAL (10, 2) DEFAULT 0.00
);

CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    shop VARCHAR(100) NOT NULL,
    product VARCHAR(100) NOT NULL,
    price DECIMAL (10, 2) NOT NULL
);

INSERT INTO accounts (name, balance) VALUES
('Alice', 1000.00),
('Bob', 500.00),
('Wally', 750.00);

INSERT INTO products (shop, product, price) VALUES
('Joe''s Shop', 'Coke', 2.50),
('Joe''s Shop', 'Pepsi', 3.00);

SELECT * FROM accounts;
SELECT * FROM products;

---
-- 3.2 Task 1: Basic Transaction with COMMIT

BEGIN;

UPDATE accounts SET balance = balance - 100.00
WHERE name = 'Alice';

UPDATE accounts SET balance = balance + 100.00
WHERE name = 'Bob';

COMMIT;

SELECT name, balance FROM accounts WHERE name IN ('Alice', 'Bob');

---
-- 3.3 Task 2: Using ROLLBACK

BEGIN;

UPDATE accounts SET balance = 500.00
WHERE name = 'Alice';

SELECT * FROM accounts WHERE name = 'Alice';

ROLLBACK;

SELECT * FROM accounts WHERE name = 'Alice';

---
-- 3.4 Task 3: Working with SAVEPOINTS

BEGIN;

UPDATE accounts SET balance = balance - 100.00
WHERE name = 'Alice';

SAVEPOINT my_savepoint;

UPDATE accounts SET balance = balance + 100.00
WHERE name = 'Bob';

ROLLBACK TO my_savepoint;

UPDATE accounts SET balance = balance + 100.00
WHERE name = 'Wally';

COMMIT;

SELECT name, balance FROM accounts WHERE name IN ('Alice', 'Bob', 'Wally');

---
-- 3.5 Task 4: Isolation Level Demonstration (Concurrent sessions needed)

-- Terminal 1 - SCENARIO A: READ COMMITTED
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
SELECT * FROM products WHERE shop = 'Joe''s Shop';
-- Wait for Terminal 2's COMMIT (User Action Required)
SELECT * FROM products WHERE shop = 'Joe''s Shop';
COMMIT;

-- Terminal 1 - SCENARIO B: SERIALIZABLE
BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;
SELECT * FROM products WHERE shop = 'Joe''s Shop';
-- Wait for Terminal 2's COMMIT (User Action Required)
SELECT * FROM products WHERE shop = 'Joe''s Shop';
COMMIT;


-- Terminal 2 (Execute this while Terminal 1 is waiting)
BEGIN;
DELETE FROM products WHERE shop = 'Joe''s Shop';
INSERT INTO products (shop, product, price)
VALUES ('Joe''s Shop', 'Fanta', 3.50);
COMMIT;


---
-- 3.6 Task 5: Phantom Read Demonstration (Concurrent sessions needed)

-- Terminal 1
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
SELECT MAX(price), MIN(price) FROM products WHERE shop = 'Joe''s Shop';
-- Wait for Terminal 2's COMMIT (User Action Required)
SELECT MAX(price), MIN(price) FROM products WHERE shop = 'Joe''s Shop';
COMMIT;


-- Terminal 2 (Execute this while Terminal 1 is waiting)
BEGIN;
INSERT INTO products (shop, product, price)
VALUES ('Joe''s Shop', 'Sprite', 4.00);
COMMIT;


---
-- 3.7 Task 6: Dirty Read Demonstration (Concurrent sessions needed)

-- Terminal 1
BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SELECT * FROM products WHERE shop = 'Joe''s Shop';
-- Wait for Terminal 2's UPDATE but NOT COMMIT (User Action Required)
SELECT * FROM products WHERE shop = 'Joe''s Shop';
-- Wait for Terminal 2's ROLLBACK (User Action Required)
SELECT * FROM products WHERE shop = 'Joe''s Shop';
COMMIT;


-- Terminal 2 (Execute this while Terminal 1 is waiting)
BEGIN;
UPDATE products SET price = 99.99
WHERE product = 'Fanta';
-- Wait (User Action Required: Terminal 1 reads the uncommitted data)
ROLLBACK;


---
-- 4. Independent Exercises

-- Exercise 1: Conditional Transfer ($200 from Bob to Wally)

BEGIN;

UPDATE accounts SET balance = balance - 200.00
WHERE name = 'Bob' AND balance >= 200.00;

UPDATE accounts SET balance = balance + 200.00
WHERE name = 'Wally'
AND EXISTS (
    SELECT 1 FROM accounts WHERE name = 'Bob' AND balance >= 200.00
);

COMMIT;

SELECT name, balance FROM accounts WHERE name IN ('Bob', 'Wally');


-- Exercise 2: Transaction with Multiple SAVEPOINTs

DELETE FROM products WHERE shop = 'Joe''s Shop';
INSERT INTO products (shop, product, price) VALUES
('Joe''s Shop', 'Coke', 2.50),
('Joe''s Shop', 'Pepsi', 3.00);

BEGIN;

INSERT INTO products (shop, product, price) VALUES ('Joe''s Shop', 'Hot Dog', 5.00);

SAVEPOINT sp1;

UPDATE products SET price = 6.00 WHERE product = 'Hot Dog';

SAVEPOINT sp2;

DELETE FROM products WHERE product = 'Hot Dog';

ROLLBACK TO sp1;

COMMIT;

SELECT * FROM products WHERE shop = 'Joe''s Shop';

-- Exercises 3 & 4 require a setup of two simultaneous transactions and are best demonstrated
-- in separate session windows, so they cannot be fully represented as a single batch script.