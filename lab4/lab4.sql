CREATE TABLE employees (
 employee_id SERIAL PRIMARY KEY,
 first_name VARCHAR(50),
 last_name VARCHAR(50),
 department VARCHAR(50),
 salary NUMERIC(10,2),
 hire_date DATE,
 manager_id INTEGER,
 email VARCHAR(100)
);
CREATE TABLE projects (
 project_id SERIAL PRIMARY KEY,
 project_name VARCHAR(100),
 budget NUMERIC(12,2),
 start_date DATE,
 end_date DATE,
 status VARCHAR(20)
);
CREATE TABLE assignments (
 assignment_id SERIAL PRIMARY KEY,
 employee_id INTEGER REFERENCES employees(employee_id),
 project_id INTEGER REFERENCES projects(project_id),
 hours_worked NUMERIC(5,1),
 assignment_date DATE
);
-- Insert sample data
INSERT INTO employees (first_name, last_name, department,
salary, hire_date, manager_id, email) VALUES
('John', 'Smith', 'IT', 75000, '2020-01-15', NULL,
'john.smith@company.com'),
('Sarah', 'Johnson', 'IT', 65000, '2020-03-20', 1,
'sarah.j@company.com'),
('Michael', 'Brown', 'Sales', 55000, '2019-06-10', NULL,
'mbrown@company.com'),
('Emily', 'Davis', 'HR', 60000, '2021-02-01', NULL,
'emily.davis@company.com'),
('Robert', 'Wilson', 'IT', 70000, '2020-08-15', 1, NULL),
('Lisa', 'Anderson', 'Sales', 58000, '2021-05-20', 3,
'lisa.a@company.com');
INSERT INTO projects (project_name, budget, start_date,
end_date, status) VALUES
('Website Redesign', 150000, '2024-01-01', '2024-06-30',
'Active'),
('CRM Implementation', 200000, '2024-02-15', '2024-12-31',
'Active'),
('Marketing Campaign', 80000, '2024-03-01', '2024-05-31',
'Completed'),
('Database Migration', 120000, '2024-01-10', NULL, 'Active');
INSERT INTO assignments (employee_id, project_id,
hours_worked, assignment_date) VALUES
(1, 1, 120.5, '2024-01-15'),
(2, 1, 95.0, '2024-01-20'),
(1, 4, 80.0, '2024-02-01'),
(3, 3, 60.0, '2024-03-05'),
(5, 2, 110.0, '2024-02-20'),
(6, 3, 75.5, '2024-03-10');

-- task 1: select all employees with full name, department, and salary

SELECT
    first_name || ' ' || last_name AS full_name,
    department,
    salary
FROM employees;

SELECT * FROM employees;

-- Task 1.2 Unique departments

SELECT DISTINCT department
FROM employees

-- Task 1.3: projects with budget category

SELECT
    project_name,
    budget,
    CASE
        WHEN budget > 15000000 THEN 'large'
        WHEN budget BETWEEN 100000 AND 1500000 THEN 'Medium'
        ELSE 'small'
    END AS budget_category
FROM projects;

--task 1.4 Employees with email or placeholder
SELECT
    first_name,
    last_name,
    COALESCE(email, 'No email provided') As email
FROM employees;

--PART 2 WHERE clause and Comparison Operators
-- Task 2.1 Employees
-- Task 2.1: Employees hired after 2020-01-01
SELECT *
FROM employees
WHERE hire_date > '2020-01-01'
--task 2.3
SELECT *
FROM employees
WHERE salary BETWEEN 60000 AND 70000;

-- task 2.4
SELECT
FROM employees
WHERE last_name LIKE 'S%' OR last_name LIKE 'J%';

-- task 2.4 Employees with a manager in IT departmnet

SELECT *
FROM employees
WHERE manager_id IS NOT NULL
    AND department = 'IT';

--part 3: String and Mathematical functions

SELECT
    upper(first_name || ' ' || last_name) As full_name_upper,
    LENGTH(last_name) AS last_name_length,
    SUBSTRING(email FROM 1 FOR 3) AS emil_prefix
FROM employees;

-- task 3.2 salary calculations

SELECT
    first_name,
    last_name,
    salary AS annual_salary,
    ROUND(salary / 12, 2) AS monthly_salary,
    salary * 0.10 AS raise_amount
FROM employees;

--task 3.3 formatted project string

SELECT
    FORMAT('project: %s - Budget: $%s - Status: %s',
    project_name, budget, status) AS project_info
FROM projects;

--task 3.4: years with company

SELECT
    first_name,
    last_name,
    EXTRACT(YEAR FROM AGE(CURRENT_DATE, hire_date)) AS years_with_compamy
FROM employees;

--part 4

SELECT department,
       ROUND(AVG(salay), 2) AS avg_salary
FROM employees
GROUP BY department;

-- task 4.2 Total hour per project

SELECT
    p.project_name,
    SUM(a.hours_worked) AS total_hours
FROM projects p
JOIN assignments a ON p.project_id = a.project_id
GROUP BY P.project_name;

--task 4.3 department with more than 1 employees

SELECT
    department,
    COUNT(*) AS employee_count
FROM employees
GROUP BY department
HAVING COUNT(*) > 1;

-- Task 4.4: Salary summary
SELECT
    MAX(salary) AS max_salary,
    MIN(salary) AS min_salary,
    SUM(salary) AS total_payroll
FROM employees;

-- Task 5.1: UNION of high salary and recent hires
SELECT employee_id, first_name || ' ' || last_name AS full_name, salary
FROM employees
WHERE salary > 65000
UNION
SELECT employee_id, first_name || ' ' || last_name AS full_name, salary
FROM employees
WHERE hire_date > '2020-01-01';

-- Task 5.2: INTERSECT IT and high salary
SELECT employee_id, first_name, last_name
FROM employees
WHERE department = 'IT'
INTERSECT
SELECT employee_id, first_name, last_name
FROM employees
WHERE salary > 65000;

-- Task 5.3: EXCEPT employees not assigned to projects
SELECT employee_id, first_name, last_name
FROM employees
EXCEPT
SELECT e.employee_id, e.first_name, e.last_name
FROM employees e
JOIN assignments a ON e.employee_id = a.employee_id;


-- Task 6.1: Employees with at least one assignment
SELECT *
FROM employees e
WHERE EXISTS (
    SELECT 1 FROM assignments a WHERE a.employee_id = e.employee_id
);

-- Task 6.2: Employees on active projects
SELECT *
FROM employees
WHERE employee_id IN (
    SELECT a.employee_id
    FROM assignments a
    JOIN projects p ON a.project_id = p.project_id
    WHERE p.status = 'Active'
);

-- Task 6.3: Salary greater than any Sales employee
SELECT *
FROM employees
WHERE salary > ANY (
    SELECT salary FROM employees WHERE department = 'Sales'
);

-- Task 7.1: Employee summary with rank
SELECT
    first_name || ' ' || last_name AS employee_name,
    department,
    ROUND(AVG(a.hours_worked), 1) AS avg_hours_worked,
    RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS salary_rank
FROM employees e
LEFT JOIN assignments a ON e.employee_id = a.employee_id
GROUP BY e.employee_id, employee_name, department, salary
ORDER BY department, salary_rank;

-- Task 7.2: Projects with > 150 total hours
SELECT
    p.project_name,
    SUM(a.hours_worked) AS total_hours,
    COUNT(DISTINCT a.employee_id) AS employee_count
FROM projects p
JOIN assignments a ON p.project_id = a.project_id
GROUP BY p.project_name
HAVING SUM(a.hours_worked) > 150;

-- Task 7.3: Department report with GREATEST/LEAST
SELECT
    department,
    COUNT(*) AS total_employees,
    ROUND(AVG(salary), 2) AS avg_salary,
    MAX(first_name || ' ' || last_name) AS highest_paid_employee,
    GREATEST(MAX(salary), 0) AS highest_salary,  -- Example of GREATEST
    LEAST(MIN(salary), 100000) AS lowest_salary_capped  -- Example of LEAST
FROM employees
GROUP BY department;
