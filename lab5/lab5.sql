CREATE TABLE employees (
    employee_id INTEGER,
    first_name TEXT,
    last_name TEXT,
    age INTEGER CHECK (age >= 18 AND age <= 65),
    salary NUMERIC CHECK (salary > 0)
);

INSERT INTO employees VALUES (1, 'John', 'Doe', 30, 50000);
INSERT INTO employees VALUES (2, 'Jane', 'Smith', 25, 60000);
INSERT INTO employees VALUES (3, 'Bob', 'Johnson', 17, 40000);
INSERT INTO employees VALUES (4, 'Alice', 'Brown', 35, -1000);

CREATE TABLE products_catalog (
    product_id INTEGER,
    product_name TEXT,
    regular_price NUMERIC,
    discount_price NUMERIC,
    CONSTRAINT valid_discount CHECK (regular_price > 0 AND discount_price > 0 AND discount_price < regular_price)
);

INSERT INTO products_catalog VALUES (1, 'Laptop', 1000, 800);
INSERT INTO products_catalog VALUES (2, 'Mouse', 50, 40);
INSERT INTO products_catalog VALUES (3, 'Keyboard', 100, 120);
INSERT INTO products_catalog VALUES (4, 'Monitor', -200, 150);

CREATE TABLE bookings (
    booking_id INTEGER,
    check_in_date DATE,
    check_out_date DATE,
    num_guests INTEGER CHECK (num_guests >= 1 AND num_guests <= 10),
    CHECK (check_out_date > check_in_date)
);

INSERT INTO bookings VALUES (1, '2023-01-01', '2023-01-05', 2);
INSERT INTO bookings VALUES (2, '2023-02-01', '2023-02-10', 4);
INSERT INTO bookings VALUES (3, '2023-03-01', '2023-02-28', 3);
INSERT INTO bookings VALUES (4, '2023-04-01', '2023-04-05', 0);

CREATE TABLE customers (
    customer_id INTEGER NOT NULL,
    email TEXT NOT NULL,
    phone TEXT,
    registration_date DATE NOT NULL
);

INSERT INTO customers VALUES (1, 'customer1@example.com', '123-456-7890', '2023-01-01');
INSERT INTO customers VALUES (2, 'customer2@example.com', NULL, '2023-01-02');
INSERT INTO customers VALUES (NULL, 'customer3@example.com', '555-555-5555', '2023-01-03');

CREATE TABLE inventory (
    item_id INTEGER NOT NULL,
    item_name TEXT NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity >= 0),
    unit_price NUMERIC NOT NULL CHECK (unit_price > 0),
    last_updated TIMESTAMP NOT NULL
);

INSERT INTO inventory VALUES (1, 'Item A', 100, 10.99, '2023-01-01 10:00:00');
INSERT INTO inventory VALUES (2, 'Item B', 50, 5.99, '2023-01-01 11:00:00');
INSERT INTO inventory VALUES (NULL, 'Item C', 200, 15.99, '2023-01-01 12:00:00');
INSERT INTO inventory VALUES (3, NULL, 150, 8.99, '2023-01-01 13:00:00');

CREATE TABLE users (
    user_id INTEGER,
    username TEXT UNIQUE,
    email TEXT UNIQUE,
    created_at TIMESTAMP
);

INSERT INTO users VALUES (1, 'user1', 'user1@example.com', '2023-01-01 10:00:00');
INSERT INTO users VALUES (2, 'user2', 'user2@example.com', '2023-01-01 11:00:00');
INSERT INTO users VALUES (3, 'user1', 'user3@example.com', '2023-01-01 12:00:00');

CREATE TABLE course_enrollments (
    enrollment_id INTEGER,
    student_id INTEGER,
    course_code TEXT,
    semester TEXT,
    UNIQUE (student_id, course_code, semester)
);

INSERT INTO course_enrollments VALUES (1, 101, 'CS101', 'Fall2023');
INSERT INTO course_enrollments VALUES (2, 102, 'MATH101', 'Fall2023');
INSERT INTO course_enrollments VALUES (3, 101, 'CS101', 'Fall2023');

ALTER TABLE users ADD CONSTRAINT unique_username UNIQUE (username);
ALTER TABLE users ADD CONSTRAINT unique_email UNIQUE (email);

CREATE TABLE departments (
    dept_id INTEGER PRIMARY KEY,
    dept_name TEXT NOT NULL,
    location TEXT
);

INSERT INTO departments VALUES (1, 'HR', 'New York');
INSERT INTO departments VALUES (2, 'IT', 'San Francisco');
INSERT INTO departments VALUES (3, 'Finance', 'Chicago');
INSERT INTO departments VALUES (1, 'Marketing', 'Boston');
INSERT INTO departments VALUES (NULL, 'Sales', 'Los Angeles');

CREATE TABLE student_courses (
    student_id INTEGER,
    course_id INTEGER,
    enrollment_date DATE,
    grade TEXT,
    PRIMARY KEY (student_id, course_id)
);

INSERT INTO student_courses VALUES (101, 1, '2023-01-01', 'A');
INSERT INTO student_courses VALUES (101, 2, '2023-01-01', 'B');
INSERT INTO student_courses VALUES (101, 1, '2023-01-01', 'C');

CREATE TABLE employees_dept (
    emp_id INTEGER PRIMARY KEY,
    emp_name TEXT NOT NULL,
    dept_id INTEGER REFERENCES departments,
    hire_date DATE
);

INSERT INTO employees_dept VALUES (1, 'John Doe', 1, '2023-01-01');
INSERT INTO employees_dept VALUES (2, 'Jane Smith', 2, '2023-01-02');
INSERT INTO employees_dept VALUES (3, 'Bob Johnson', 5, '2023-01-03');

CREATE TABLE authors (
    author_id INTEGER PRIMARY KEY,
    author_name TEXT NOT NULL,
    country TEXT
);

CREATE TABLE publishers (
    publisher_id INTEGER PRIMARY KEY,
    publisher_name TEXT NOT NULL,
    city TEXT
);

CREATE TABLE books (
    book_id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    author_id INTEGER REFERENCES authors,
    publisher_id INTEGER REFERENCES publishers,
    publication_year INTEGER,
    isbn TEXT UNIQUE
);

INSERT INTO authors VALUES (1, 'Author A', 'USA');
INSERT INTO authors VALUES (2, 'Author B', 'UK');
INSERT INTO publishers VALUES (1, 'Publisher X', 'New York');
INSERT INTO publishers VALUES (2, 'Publisher Y', 'London');
INSERT INTO books VALUES (1, 'Book 1', 1, 1, 2020, 'ISBN001');
INSERT INTO books VALUES (2, 'Book 2', 2, 2, 2021, 'ISBN002');

CREATE TABLE categories (
    category_id INTEGER PRIMARY KEY,
    category_name TEXT NOT NULL
);

CREATE TABLE products_fk (
    product_id INTEGER PRIMARY KEY,
    product_name TEXT NOT NULL,
    category_id INTEGER REFERENCES categories ON DELETE RESTRICT
);

CREATE TABLE orders (
    order_id INTEGER PRIMARY KEY,
    order_date DATE NOT NULL
);

CREATE TABLE order_items (
    item_id INTEGER PRIMARY KEY,
    order_id INTEGER REFERENCES orders ON DELETE CASCADE,
    product_id INTEGER REFERENCES products_fk,
    quantity INTEGER CHECK (quantity > 0)
);

INSERT INTO categories VALUES (1, 'Electronics');
INSERT INTO categories VALUES (2, 'Books');
INSERT INTO products_fk VALUES (1, 'Laptop', 1);
INSERT INTO products_fk VALUES (2, 'Novel', 2);
INSERT INTO orders VALUES (1, '2023-01-01');
INSERT INTO orders VALUES (2, '2023-01-02');
INSERT INTO order_items VALUES (1, 1, 1, 2);
INSERT INTO order_items VALUES (2, 1, 2, 1);
INSERT INTO order_items VALUES (3, 2, 1, 3);

DELETE FROM categories WHERE category_id = 1;
DELETE FROM orders WHERE order_id = 1;

CREATE TABLE customers_ecom (
    customer_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    phone TEXT,
    registration_date DATE NOT NULL
);

CREATE TABLE products_ecom (
    product_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    price NUMERIC CHECK (price >= 0),
    stock_quantity INTEGER CHECK (stock_quantity >= 0)
);

CREATE TABLE orders_ecom (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER REFERENCES customers_ecom,
    order_date DATE NOT NULL,
    total_amount NUMERIC CHECK (total_amount >= 0),
    status TEXT CHECK (status IN ('pending', 'processing', 'shipped', 'delivered', 'cancelled'))
);

CREATE TABLE order_details_ecom (
    order_detail_id INTEGER PRIMARY KEY,
    order_id INTEGER REFERENCES orders_ecom,
    product_id INTEGER REFERENCES products_ecom,
    quantity INTEGER CHECK (quantity > 0),
    unit_price NUMERIC CHECK (unit_price >= 0)
);

INSERT INTO customers_ecom VALUES (1, 'Customer One', 'customer1@example.com', '111-111-1111', '2023-01-01');
INSERT INTO customers_ecom VALUES (2, 'Customer Two', 'customer2@example.com', '222-222-2222', '2023-01-02');
INSERT INTO customers_ecom VALUES (3, 'Customer Three', 'customer3@example.com', '333-333-3333', '2023-01-03');
INSERT INTO customers_ecom VALUES (4, 'Customer Four', 'customer4@example.com', '444-444-4444', '2023-01-04');
INSERT INTO customers_ecom VALUES (5, 'Customer Five', 'customer5@example.com', '555-555-5555', '2023-01-05');

INSERT INTO products_ecom VALUES (1, 'Product A', 'Description A', 19.99, 100);
INSERT INTO products_ecom VALUES (2, 'Product B', 'Description B', 29.99, 50);
INSERT INTO products_ecom VALUES (3, 'Product C', 'Description C', 9.99, 200);
INSERT INTO products_ecom VALUES (4, 'Product D', 'Description D', 49.99, 25);
INSERT INTO products_ecom VALUES (5, 'Product E', 'Description E', 14.99, 150);

INSERT INTO orders_ecom VALUES (1, 1, '2023-01-10', 39.98, 'pending');
INSERT INTO orders_ecom VALUES (2, 2, '2023-01-11', 59.98, 'processing');
INSERT INTO orders_ecom VALUES (3, 3, '2023-01-12', 29.97, 'shipped');
INSERT INTO orders_ecom VALUES (4, 4, '2023-01-13', 99.96, 'delivered');
INSERT INTO orders_ecom VALUES (5, 5, '2023-01-14', 14.99, 'cancelled');

INSERT INTO order_details_ecom VALUES (1, 1, 1, 2, 19.99);
INSERT INTO order_details_ecom VALUES (2, 2, 2, 2, 29.99);
INSERT INTO order_details_ecom VALUES (3, 3, 3, 3, 9.99);
INSERT INTO order_details_ecom VALUES (4, 4, 4, 2, 49.99);
INSERT INTO order_details_ecom VALUES (5, 5, 5, 1, 14.99);