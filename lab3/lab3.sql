drop database advanced_lb;
CREATE DATABASE advanced_lb;


CREATE TABLE employees(
    emp_id SERIAL PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    department TEXT,
    salary INTEGER,
    hire_date DATE,
    status TEXT DEFAULT 'Active'
);

CREATE TABLE departments(
    dept_id SERIAL PRIMARY KEY,
    dept_name TEXT,
    budget INTEGER,
    manager_id INTEGER
);

CREATE TABLE projects(
    project_id SERIAL PRIMARY KEY,
    project_name TEXT,
    dept_id INTEGER,
    start_date DATE,
    end_date DATE,
    budget INTEGER
);

INSERT INTO employees (emp_id, first_name, last_name, department) VALUES (
'1', 'Hashmatullah', 'Azizi', 'IT'
                                                                         );
SELECT * FROM employees;

--INSERTS WITH DEFAULT VALUES
INSERT INTO employees VALUES (
 '2',
 'Naweedullah',
'Azizi',
'ARG',
DEFAULT,
'2024-4-4',
DEFAULT
 );

SELECT * FROM employees;
INSERT INTO employees
VALUES
    (3, 'Asadullah', 'Azizi', 'it', 12000, '2024-04-09', 'Active'),
    (4, 'Waheedullah', 'Razayee', 'IT', 14000, '2024-06-04', 'Active'),
    (5, 'Karim', 'Ahmadi', 'OIL_GAS',1000, '2024-3-12', 'Active' );

SELECT * FROM employees;

INSERT INTO employees  (emp_id, first_name, last_name, department, salary, hire_date, status)
VALUES(6,
       'Samira',
       'Azizi',
       'Economic',
       50000 * 1.1,
       CURRENT_DATE,
       'Active'

      );

SELECT * FROM employees;

-- CREATING TEMPORARY_TABLE
CREATE TABLE temp_employees AS
    SELECT * FROM employees WHERE department = 'IT';
SELECT * FROM temp_employees;

--setting the salary for all employees

UPDATE employees SET salary = salary * 1.10;

--UPDATE with WHERE clauses and multiple conditions

UPDATE employees
SET status = 'Senior'
WHERE salary > 60000
AND hire_date < '2024-02-02';

SELECT * FROM employees;

-- UPDATE WITH CASE expression

UPDATE employees
SET department = CASE
    WHEN salary > 80000 THEN 'Management'
    WHEN salary BETWEEN 50000 AND 80000 THEN 'Senior'
    ELSE 'Junior'
END;

SELECT *FROM employees;

--update with default value
--setting the department to DEFAULT value for employees where status equals 'Inactive'.

UPDATE employees
SET department = DEFAULT
WHERE status = 'Inactive';

-- UPDATE with subquery

UPDATE departments d
SET budget = (
    SELECT AVG(salary) * 1.2
    FROM employees e
    WHERE e.department = d.dept_name
);

SELECT * FROM departments;

-- update multiple columns

UPDATE employees
SET salary = salary * 1.15,
    status = 'Promoted'
WHERE department = 'Sales';

-- Delete with simple where condition
DELETE FROM employees
WHERE status = 'Terminated';

--Delete with complex Where cluse

DELETE FROM employees
WHERE salary < 40000
AND hire_date > '2024-06-04'
AND department IS NULL;

-- DELETE with subquery
DELETE FROM departments
WHERE dept_id NOT IN (
    SELECT DISTINCT department
    FROM employees
    WHERE department IS NOT NULL
);

INSERT INTO projects (project_id,project_name, dept_id, start_date, end_date, budget)
VALUES (1,
        'ICCR',
        '1',
        '2023-01-01',
        '2023-01-02',
        2000
);
-- DELETE with RETURNING clause

DELETE FROM projects
WHERE end_date < '2023-01-04'
RETURNING *;

INSERT INTO employees (emp_id,first_name, last_name, department, salary, hire_date, status)
VALUES(
       7,
       'Ali',
       'Ahmadi',
       NULL,
       NULL,
       '2024-3-4',
       'ACTIVE'
      ) ;

SELECT * FROM employees;

UPDATE employees SET department = 'Unassigned' WHERE department IS NULL;

SELECT * FROM employees;

DELETE FROM employees WHERE salary IS NULL || department IS NULL;

SELECT * FROM employees;

--PART F: RETURNING Clause Operations

INSERT INTO employees(emp_id,first_name, last_name, department, salary, hire_date, status)
VALUES (8,
        'Tamana',
        'Alizada',
        'IT',
        4000000,
        '2025-02-03',
        'single'
       );

--INSERT with RETURNING

INSERT INTO employees (emp_id,first_name, last_name, department, salary, hire_date, status)
VALUES (
        9,
        'John',
        'Doe',
        'IT',
        600000,
        CURRENT_DATE,
        'Active')
        RETURNING
                emp_id,
                CONCAT(employees.first_name, ' ', last_name) AS full_name;

---UPDATE with RETURNING

UPDATE employees
SET salary = salary + 5000
WHERE department = 'IT'
RETURNING
    emp_id,
    (salary -5000) AS old_salary,
    salary AS new_salary;

-- DELETE WITH RETURNING all columns

DELETE FROM employees
WHERE hire_date < '2020-01-01'
RETURNING *;

-- INSERTING A NEW EMPLOYEE IF THE SAME FIRST_NAME AND LAST_NAME EXIST BEFORE
INSERT INTO employees(first_name, last_name, department, salary, hire_date)
SELECT 'John', 'Doe', 'IT', '600000', CURRENT_DATE
WHERE NOT EXISTS(
    SELECT 1
    FROM employees
    WHERE first_name = 'John'
    AND last_name = 'Doe'
);

--UPDATE WITH JOIN LOGICAL USING SUBQUERIES
UPDATE employees
SET salary  = CASE
    WHEN department IN (
        SELECT dept_name
        FROM departments
        WHERE budget > 100000
        ) THEN salary * 1.10
        ELSE salary * 1.05
END;




