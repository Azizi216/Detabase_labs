


-- Create tables
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    tin VARCHAR(12) UNIQUE NOT NULL CHECK (LENGTH(tin) = 12),
    full_name VARCHAR(100) NOT NULL,
    phone VARCHAR(20),
    email VARCHAR(100),
    status VARCHAR(10) CHECK (status IN ('active', 'blocked', 'frozen')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    daily_limit_kzt DECIMAL(15,2) DEFAULT 1000000.00
);

CREATE TABLE accounts (
    account_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id),
    account_number VARCHAR(34) UNIQUE NOT NULL, -- IBAN format
    currency VARCHAR(3) CHECK (currency IN ('KZT', 'USD', 'EUR', 'RUB')),
    balance DECIMAL(15,2) DEFAULT 0.00,
    is_active BOOLEAN DEFAULT TRUE,
    opened_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    closed_at TIMESTAMP
);

CREATE TABLE exchange_rates (
    rate_id SERIAL PRIMARY KEY,
    from_currency VARCHAR(3) NOT NULL,
    to_currency VARCHAR(3) NOT NULL,
    rate DECIMAL(10,6) NOT NULL,
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP,
    UNIQUE(from_currency, to_currency, valid_from)
);

CREATE TABLE transactions (
    transaction_id SERIAL PRIMARY KEY,
    from_account_id INTEGER REFERENCES accounts(account_id),
    to_account_id INTEGER REFERENCES accounts(account_id),
    amount DECIMAL(15,2) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    exchange_rate DECIMAL(10,6) DEFAULT 1.0,
    amount_kzt DECIMAL(15,2) NOT NULL,
    type VARCHAR(20) CHECK (type IN ('transfer', 'deposit', 'withdrawal')),
    status VARCHAR(20) CHECK (status IN ('pending', 'completed', 'failed', 'reversed')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    description TEXT
);

CREATE TABLE audit_log (
    log_id SERIAL PRIMARY KEY,
    table_name VARCHAR(50) NOT NULL,
    record_id INTEGER NOT NULL,
    action VARCHAR(10) CHECK (action IN ('INSERT', 'UPDATE', 'DELETE')),
    old_values JSONB,
    new_values JSONB,
    changed_by VARCHAR(50),
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ip_address INET
);

-------------------------------
-- 2. SAMPLE DATA INSERTION
-------------------------------

-- Insert customers
INSERT INTO customers (tin, full_name, phone, email, status, daily_limit_kzt) VALUES
('123456789012', 'Aigerim Alibekova', '+77771234567', 'aigerim@email.com', 'active', 1500000.00),
('234567890123', 'Bakhytzhan Bolatov', '+77772345678', 'bakhyt@email.com', 'active', 2000000.00),
('345678901234', 'Carla Cruz', '+77773456789', 'carla@email.com', 'active', 1000000.00),
('456789012345', 'David Dumas', '+77774567890', 'david@email.com', 'blocked', 500000.00),
('567890123456', 'Elena Ermakova', '+77775678901', 'elena@email.com', 'active', 3000000.00),
('678901234567', 'Farkhad Fozilov', '+77776789012', 'farkhad@email.com', 'frozen', 100000.00),
('789012345678', 'Gulnara Galieva', '+77777890123', 'gulnara@email.com', 'active', 2500000.00),
('890123456789', 'Hiroshi Honda', '+77778901234', 'hiroshi@email.com', 'active', 1000000.00),
('901234567890', 'Ivan Ivanov', '+77779012345', 'ivan@email.com', 'active', 1500000.00),
('012345678901', 'Julia Johnson', '+77770123456', 'julia@email.com', 'active', 1200000.00);

-- Insert accounts
INSERT INTO accounts (customer_id, account_number, currency, balance, is_active) VALUES
(1, 'KZ12345678901234567890', 'KZT', 500000.00, TRUE),
(1, 'KZ09876543210987654321', 'USD', 10000.00, TRUE),
(2, 'KZ23456789012345678901', 'KZT', 1500000.00, TRUE),
(3, 'KZ34567890123456789012', 'EUR', 8000.00, TRUE),
(4, 'KZ45678901234567890123', 'KZT', 100000.00, FALSE),
(5, 'KZ56789012345678901234', 'KZT', 3000000.00, TRUE),
(6, 'KZ67890123456789012345', 'RUB', 500000.00, TRUE),
(7, 'KZ78901234567890123456', 'KZT', 800000.00, TRUE),
(8, 'KZ89012345678901234567', 'USD', 15000.00, TRUE),
(9, 'KZ90123456789012345678', 'KZT', 1200000.00, TRUE),
(10, 'KZ01234567890123456789', 'EUR', 12000.00, TRUE);

-- Insert exchange rates (with updated rates)
INSERT INTO exchange_rates (from_currency, to_currency, rate, valid_from) VALUES
('USD', 'KZT', 450.50, '2024-03-01'),
('EUR', 'KZT', 480.75, '2024-03-01'),
('RUB', 'KZT', 5.0, '2024-03-01'),
('KZT', 'USD', 0.002220, '2024-03-01'),
('KZT', 'EUR', 0.002080, '2024-03-01'),
('KZT', 'RUB', 0.200000, '2024-03-01'),
('USD', 'EUR', 0.920, '2024-03-01'),
('EUR', 'USD', 1.087, '2024-03-01');

-- Insert sample transactions
INSERT INTO transactions (from_account_id, to_account_id, amount, currency, exchange_rate, amount_kzt, type, status, completed_at) VALUES
(1, 3, 50000.00, 'KZT', 1.0, 50000.00, 'transfer', 'completed', NOW() - INTERVAL '1 day'),
(2, 9, 1000.00, 'USD', 450.50, 450500.00, 'transfer', 'completed', NOW() - INTERVAL '1 day'),
(3, 7, 200000.00, 'KZT', 1.0, 200000.00, 'transfer', 'completed', NOW()),
(9, 10, 5000.00, 'EUR', 480.75, 2403750.00, 'transfer', 'completed', NOW());

-------------------------------
-- 3. TASK 1: TRANSACTION MANAGEMENT
-- Stored Procedure: process_transfer (FIXED CURRENCY LOGIC)
-------------------------------

CREATE OR REPLACE PROCEDURE process_transfer(
    from_account_number VARCHAR(34),
    to_account_number VARCHAR(34),
    amount DECIMAL(15,2),
    currency VARCHAR(3),
    description TEXT DEFAULT NULL,
    OUT success BOOLEAN,
    OUT message TEXT,
    OUT error_code VARCHAR(5)
)
LANGUAGE plpgsql
AS $$
DECLARE
    from_acc RECORD;
    to_acc RECORD;
    exchange_rate DECIMAL(10,6);
    amount_in_kzt DECIMAL(15,2);
    daily_total DECIMAL(15,2);
    savepoint_name TEXT;
    audit_id INTEGER;
    converted_amount DECIMAL(15,2);
BEGIN
    -- Initialize
    success := FALSE;
    message := '';
    error_code := '00000';
    savepoint_name := 'sp_' || EXTRACT(EPOCH FROM NOW());
    
    -- Start transaction
    BEGIN
        -- Lock and validate sender account
        SELECT a.*, c.status as customer_status, c.daily_limit_kzt
        INTO from_acc
        FROM accounts a
        JOIN customers c ON a.customer_id = c.customer_id
        WHERE a.account_number = from_account_number
        FOR UPDATE;
        
        IF NOT FOUND THEN
            RAISE EXCEPTION USING 
                ERRCODE = 'ACCNF',
                MESSAGE = 'Sender account not found';
        END IF;
        
        -- Lock and validate receiver account
        SELECT a.*, c.status as customer_status
        INTO to_acc
        FROM accounts a
        JOIN customers c ON a.customer_id = c.customer_id
        WHERE a.account_number = to_account_number
        FOR UPDATE;
        
        IF NOT FOUND THEN
            RAISE EXCEPTION USING 
                ERRCODE = 'ACCNF',
                MESSAGE = 'Receiver account not found';
        END IF;
        
        -- Validate account statuses
        IF NOT from_acc.is_active THEN
            RAISE EXCEPTION USING 
                ERRCODE = 'ACCIN',
                MESSAGE = 'Sender account is inactive';
        END IF;
        
        IF NOT to_acc.is_active THEN
            RAISE EXCEPTION USING 
                ERRCODE = 'ACCIN',
                MESSAGE = 'Receiver account is inactive';
        END IF;
        
        -- Validate customer status
        IF from_acc.customer_status != 'active' THEN
            RAISE EXCEPTION USING 
                ERRCODE = 'CUSBL',
                MESSAGE = 'Sender customer is blocked or frozen';
        END IF;
        
        IF to_acc.customer_status != 'active' THEN
            RAISE EXCEPTION USING 
                ERRCODE = 'CUSBL',
                MESSAGE = 'Receiver customer is blocked or frozen';
        END IF;
        
        -- Check if currency matches sender account currency
        IF currency != from_acc.currency THEN
            RAISE EXCEPTION USING 
                ERRCODE = 'CURMM',
                MESSAGE = 'Transfer currency must match sender account currency';
        END IF;
        
        -- Check sufficient balance
        IF from_acc.balance < amount THEN
            RAISE EXCEPTION USING 
                ERRCODE = 'INSUB',
                MESSAGE = 'Insufficient balance';
        END IF;
        
        -- Get exchange rate for KZT conversion (for daily limit check)
        IF currency = 'KZT' THEN
            exchange_rate := 1.0;
            amount_in_kzt := amount;
        ELSE
            SELECT rate INTO exchange_rate
            FROM exchange_rates
            WHERE from_currency = currency 
                AND to_currency = 'KZT'
                AND valid_from <= NOW()
                AND (valid_to IS NULL OR valid_to > NOW())
            ORDER BY valid_from DESC
            LIMIT 1;
            
            IF NOT FOUND THEN
                RAISE EXCEPTION USING 
                    ERRCODE = 'EXCNF',
                    MESSAGE = 'Exchange rate not found for currency: ' || currency;
            END IF;
            
            amount_in_kzt := amount * exchange_rate;
        END IF;
        
        -- Check daily limit
        SELECT COALESCE(SUM(amount_kzt), 0) INTO daily_total
        FROM transactions
        WHERE from_account_id = from_acc.account_id
            AND status = 'completed'
            AND DATE(created_at) = CURRENT_DATE
            AND type = 'transfer';
        
        IF daily_total + amount_in_kzt > from_acc.daily_limit_kzt THEN
            RAISE EXCEPTION USING 
                ERRCODE = 'LIMEX',
                MESSAGE = 'Daily transaction limit exceeded. Used: ' || daily_total || ' KZT, Attempting: ' || amount_in_kzt || ' KZT, Limit: ' || from_acc.daily_limit_kzt || ' KZT';
        END IF;
        
        -- Create savepoint for partial rollback
        SAVEPOINT savepoint_name;
        
        -- Calculate amount to credit to receiver (with currency conversion)
        IF from_acc.currency = to_acc.currency THEN
            converted_amount := amount;
        ELSE
            -- Get conversion rate between sender and receiver currencies
            SELECT rate INTO exchange_rate
            FROM exchange_rates
            WHERE from_currency = from_acc.currency 
                AND to_currency = to_acc.currency
                AND valid_from <= NOW()
                AND (valid_to IS NULL OR valid_to > NOW())
            ORDER BY valid_from DESC
            LIMIT 1;
            
            IF NOT FOUND THEN
                -- Try indirect conversion through KZT
                DECLARE
                    rate1 DECIMAL(10,6);
                    rate2 DECIMAL(10,6);
                BEGIN
                    SELECT rate INTO rate1
                    FROM exchange_rates
                    WHERE from_currency = from_acc.currency 
                        AND to_currency = 'KZT'
                        AND valid_from <= NOW()
                        AND (valid_to IS NULL OR valid_to > NOW())
                    ORDER BY valid_from DESC
                    LIMIT 1;
                    
                    SELECT rate INTO rate2
                    FROM exchange_rates
                    WHERE from_currency = 'KZT'
                        AND to_currency = to_acc.currency
                        AND valid_from <= NOW()
                        AND (valid_to IS NULL OR valid_to > NOW())
                    ORDER BY valid_from DESC
                    LIMIT 1;
                    
                    IF rate1 IS NULL OR rate2 IS NULL THEN
                        RAISE EXCEPTION USING 
                            ERRCODE = 'EXCNF',
                            MESSAGE = 'Currency conversion rate not available from ' || from_acc.currency || ' to ' || to_acc.currency;
                    END IF;
                    
                    exchange_rate := rate1 * rate2;
                END;
            END IF;
            
            converted_amount := amount * exchange_rate;
        END IF;
        
        -- Deduct from sender
        UPDATE accounts 
        SET balance = balance - amount
        WHERE account_id = from_acc.account_id;
        
        -- Add to receiver
        UPDATE accounts 
        SET balance = balance + converted_amount
        WHERE account_id = to_acc.account_id;
        
        -- Record transaction (store the rate used for KZT conversion)
        DECLARE
            kzt_rate DECIMAL(10,6);
        BEGIN
            IF currency = 'KZT' THEN
                kzt_rate := 1.0;
            ELSE
                SELECT rate INTO kzt_rate
                FROM exchange_rates
                WHERE from_currency = currency 
                    AND to_currency = 'KZT'
                    AND valid_from <= NOW()
                    AND (valid_to IS NULL OR valid_to > NOW())
                ORDER BY valid_from DESC
                LIMIT 1;
            END IF;
            
            INSERT INTO transactions (
                from_account_id, to_account_id, amount, currency, 
                exchange_rate, amount_kzt, type, status, 
                completed_at, description
            ) VALUES (
                from_acc.account_id, to_acc.account_id, amount, currency,
                kzt_rate, amount_in_kzt, 'transfer', 'completed',
                NOW(), description
            ) RETURNING transaction_id INTO audit_id;
        END;
        
        -- Log to audit
        INSERT INTO audit_log (table_name, record_id, action, new_values, changed_by)
        VALUES ('transactions', audit_id, 'INSERT', 
                jsonb_build_object(
                    'from_account', from_account_number,
                    'to_account', to_account_number,
                    'amount', amount,
                    'currency', currency,
                    'converted_amount', converted_amount,
                    'to_currency', to_acc.currency,
                    'status', 'completed'
                ), 'system');
        
        -- Commit transaction
        success := TRUE;
        message := 'Transfer completed successfully. Amount sent: ' || amount || ' ' || currency || 
                  ', Amount received: ' || converted_amount || ' ' || to_acc.currency;
        error_code := '00000';
        
    EXCEPTION
        WHEN SQLSTATE 'ACCNF' THEN
            ROLLBACK;
            success := FALSE;
            message := SQLERRM;
            error_code := SQLSTATE;
            
        WHEN SQLSTATE 'ACCIN' THEN
            ROLLBACK;
            success := FALSE;
            message := SQLERRM;
            error_code := SQLSTATE;
            
        WHEN SQLSTATE 'CUSBL' THEN
            ROLLBACK;
            success := FALSE;
            message := SQLERRM;
            error_code := SQLSTATE;
            
        WHEN SQLSTATE 'CURMM' THEN
            ROLLBACK;
            success := FALSE;
            message := SQLERRM;
            error_code := SQLSTATE;
            
        WHEN SQLSTATE 'INSUB' THEN
            ROLLBACK;
            success := FALSE;
            message := SQLERRM;
            error_code := SQLSTATE;
            
        WHEN SQLSTATE 'EXCNF' THEN
            ROLLBACK;
            success := FALSE;
            message := SQLERRM;
            error_code := SQLSTATE;
            
        WHEN SQLSTATE 'LIMEX' THEN
            ROLLBACK;
            success := FALSE;
            message := SQLERRM;
            error_code := SQLSTATE;
            
        WHEN OTHERS THEN
            ROLLBACK;
            success := FALSE;
            message := SQLERRM;
            error_code := SQLSTATE;
            
            -- Log failure to audit
            INSERT INTO audit_log (table_name, record_id, action, new_values, changed_by)
            VALUES ('transactions', 0, 'INSERT', 
                    jsonb_build_object(
                        'error', SQLERRM,
                        'error_code', SQLSTATE,
                        'from_account', from_account_number,
                        'to_account', to_account_number,
                        'amount', amount,
                        'currency', currency
                    ), 'system');
    END;
END;
$$;

-------------------------------
-- 4. TASK 2: VIEWS FOR REPORTING
-------------------------------

-- View 1: customer_balance_summary
CREATE OR REPLACE VIEW customer_balance_summary AS
WITH customer_balances AS (
    SELECT 
        c.customer_id,
        c.full_name,
        c.status as customer_status,
        c.daily_limit_kzt,
        a.currency,
        a.balance,
        COALESCE(
            CASE 
                WHEN a.currency = 'KZT' THEN a.balance
                WHEN a.currency = 'USD' THEN a.balance * (
                    SELECT rate FROM exchange_rates 
                    WHERE from_currency = 'USD' AND to_currency = 'KZT'
                    AND valid_from <= NOW() AND (valid_to IS NULL OR valid_to > NOW())
                    ORDER BY valid_from DESC LIMIT 1
                )
                WHEN a.currency = 'EUR' THEN a.balance * (
                    SELECT rate FROM exchange_rates 
                    WHERE from_currency = 'EUR' AND to_currency = 'KZT'
                    AND valid_from <= NOW() AND (valid_to IS NULL OR valid_to > NOW())
                    ORDER BY valid_from DESC LIMIT 1
                )
                WHEN a.currency = 'RUB' THEN a.balance * (
                    SELECT rate FROM exchange_rates 
                    WHERE from_currency = 'RUB' AND to_currency = 'KZT'
                    AND valid_from <= NOW() AND (valid_to IS NULL OR valid_to > NOW())
                    ORDER BY valid_from DESC LIMIT 1
                )
            END, 0
        ) as balance_kzt
    FROM customers c
    LEFT JOIN accounts a ON c.customer_id = a.customer_id AND a.is_active = TRUE
)
SELECT 
    customer_id,
    full_name,
    customer_status,
    daily_limit_kzt,
    jsonb_agg(
        jsonb_build_object(
            'currency', currency, 
            'balance', balance,
            'balance_kzt', balance_kzt
        )
    ) as accounts,
    SUM(balance_kzt) as total_balance_kzt,
    ROUND(
        COALESCE(
            (SELECT SUM(amount_kzt) 
             FROM transactions t 
             JOIN accounts a ON t.from_account_id = a.account_id
             WHERE a.customer_id = cb.customer_id 
               AND DATE(t.created_at) = CURRENT_DATE
               AND t.status = 'completed'
               AND t.type = 'transfer') / NULLIF(daily_limit_kzt, 0) * 100, 
            0
        ), 2
    ) as daily_limit_utilization_percent,
    RANK() OVER (ORDER BY SUM(balance_kzt) DESC NULLS LAST) as balance_rank
FROM customer_balances cb
GROUP BY customer_id, full_name, customer_status, daily_limit_kzt;

-- View 2: daily_transaction_report
CREATE OR REPLACE VIEW daily_transaction_report AS
WITH daily_stats AS (
    SELECT 
        DATE(created_at) as transaction_date,
        type,
        COUNT(*) as transaction_count,
        SUM(amount_kzt) as total_volume_kzt,
        AVG(amount_kzt) as average_amount_kzt,
        SUM(COUNT(*)) OVER (ORDER BY DATE(created_at) ROWS UNBOUNDED PRECEDING) as running_total_count,
        SUM(SUM(amount_kzt)) OVER (ORDER BY DATE(created_at) ROWS UNBOUNDED PRECEDING) as running_total_volume_kzt,
        LAG(SUM(amount_kzt)) OVER (ORDER BY DATE(created_at)) as previous_day_volume
    FROM transactions
    WHERE status = 'completed'
    GROUP BY DATE(created_at), type
)
SELECT 
    transaction_date,
    type,
    transaction_count,
    total_volume_kzt,
    average_amount_kzt,
    running_total_count,
    running_total_volume_kzt,
    ROUND(
        COALESCE(
            (total_volume_kzt - previous_day_volume) / NULLIF(previous_day_volume, 0) * 100, 
            0
        ), 2
    ) as day_over_day_growth_percent
FROM daily_stats
ORDER BY transaction_date DESC, type;

-- View 3: suspicious_activity_view (WITH SECURITY BARRIER)
CREATE OR REPLACE VIEW suspicious_activity_view WITH (security_barrier = true) AS
WITH large_transactions AS (
    SELECT 
        t.transaction_id,
        t.created_at,
        t.amount_kzt,
        'Large transaction (> 5,000,000 KZT)' as reason,
        c.full_name,
        c.tin,
        a.account_number
    FROM transactions t
    JOIN accounts a ON t.from_account_id = a.account_id
    JOIN customers c ON a.customer_id = c.customer_id
    WHERE t.status = 'completed'
      AND t.amount_kzt > 5000000
),
frequent_transactions AS (
    SELECT 
        c.customer_id,
        c.full_name,
        c.tin,
        a.account_number,
        DATE_TRUNC('hour', t.created_at) as hour_window,
        COUNT(*) as transaction_count,
        'High frequency (>10 transactions/hour)' as reason
    FROM transactions t
    JOIN accounts a ON t.from_account_id = a.account_id
    JOIN customers c ON a.customer_id = c.customer_id
    WHERE t.status = 'completed'
    GROUP BY c.customer_id, c.full_name, c.tin, a.account_number, DATE_TRUNC('hour', t.created_at)
    HAVING COUNT(*) > 10
),
rapid_transfers AS (
    SELECT DISTINCT ON (t1.transaction_id)
        t1.transaction_id,
        t1.created_at,
        t1.amount_kzt,
        'Rapid sequential transfer (<60 seconds between)' as reason,
        c.full_name,
        c.tin,
        a.account_number
    FROM transactions t1
    JOIN transactions t2 ON t1.from_account_id = t2.from_account_id
    JOIN accounts a ON t1.from_account_id = a.account_id
    JOIN customers c ON a.customer_id = c.customer_id
    WHERE t1.status = 'completed'
      AND t2.status = 'completed'
      AND t1.transaction_id > t2.transaction_id
      AND EXTRACT(EPOCH FROM (t1.created_at - t2.created_at)) < 60
    ORDER BY t1.transaction_id, t1.created_at
)
SELECT 
    transaction_id,
    created_at,
    amount_kzt,
    reason,
    full_name,
    tin,
    account_number
FROM large_transactions
UNION ALL
SELECT 
    NULL as transaction_id,
    MIN(hour_window) as created_at,
    NULL as amount_kzt,
    reason,
    full_name,
    tin,
    account_number
FROM frequent_transactions
GROUP BY customer_id, full_name, tin, account_number, reason
UNION ALL
SELECT * FROM rapid_transfers
ORDER BY created_at DESC;

-------------------------------
-- 5. TASK 3: PERFORMANCE OPTIMIZATION WITH INDEXES
-- With detailed documentation and justification
-------------------------------

-- INDEX 1: B-tree index for frequently queried customer columns
-- Justification: TIN is unique and frequently used for lookups in transfers and batch processing
-- Expected improvement: O(log n) lookup instead of O(n) sequential scan
CREATE INDEX IF NOT EXISTS idx_customers_tin ON customers(tin);

-- INDEX 2: B-tree index for account lookups (Covering Index)
-- Justification: Account number is used in WHERE clauses for all transfer operations
-- This is a covering index for common queries that need account_number, customer_id, is_active
CREATE INDEX IF NOT EXISTS idx_accounts_number_customer_active ON accounts(account_number, customer_id, is_active);

-- INDEX 3: Composite B-tree index for transaction date range queries
-- Justification: Daily reports filter by created_at and status. This index supports both equality and range queries
-- Expected improvement: Fast date range scans and status filtering
CREATE INDEX IF NOT EXISTS idx_transactions_date_status_type ON transactions(created_at, status, type);

-- INDEX 4: Partial B-tree index for active accounts only
-- Justification: Most business queries only care about active accounts (90% of accounts are active)
-- Expected improvement: Smaller index size, faster queries for active account operations
CREATE INDEX IF NOT EXISTS idx_active_accounts ON accounts(account_id) WHERE is_active = TRUE;

-- INDEX 5: Expression index for case-insensitive email search
-- Justification: Email searches should be case-insensitive. This prevents full table scans
-- Expected improvement: Fast case-insensitive email lookups without sequential scan
CREATE INDEX IF NOT EXISTS idx_customers_email_lower ON customers(LOWER(email));

-- INDEX 6: GIN index on audit_log JSONB columns
-- Justification: Efficient JSONB querying for audit trail analysis. GIN indexes support containment operators
-- Expected improvement: Fast JSONB field queries using @>, ?, etc.
CREATE INDEX IF NOT EXISTS idx_audit_log_jsonb ON audit_log USING GIN (old_values, new_values);

-- INDEX 7: Hash index for exact currency matches in exchange_rates
-- Justification: Fast exact lookups for currency pairs. Hash indexes are optimal for equality comparisons
-- Expected improvement: O(1) lookup for currency pairs instead of O(log n) with B-tree
CREATE INDEX IF NOT EXISTS idx_exchange_currencies_hash ON exchange_rates USING HASH (from_currency, to_currency);

-- INDEX 8: Composite index for transactions by from_account and date
-- Justification: Daily limit check queries filter by from_account_id and date
-- Expected improvement: Fast aggregation for daily transaction totals
CREATE INDEX IF NOT EXISTS idx_transactions_from_date ON transactions(from_account_id, DATE(created_at), status);

-- INDEX 9: BRIN index for large timestamp ranges in audit_log
-- Justification: audit_log grows quickly, BRIN is efficient for time-series data
-- Expected improvement: Small index size, fast time-range queries on large tables
CREATE INDEX IF NOT EXISTS idx_audit_log_time_brin ON audit_log USING BRIN (changed_at);

-------------------------------
-- 6. TASK 4: ADVANCED PROCEDURE - BATCH PROCESSING
-- Fixed batch payment method with proper currency handling
-------------------------------

CREATE OR REPLACE PROCEDURE process_salary_batch(
    company_account_number VARCHAR(34),
    payments_json JSONB,
    OUT successful_count INTEGER,
    OUT failed_count INTEGER,
    OUT failed_details JSONB,
    OUT batch_summary TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
    company_account RECORD;
    payment RECORD;
    total_amount DECIMAL(15,2) := 0;
    processed_amount DECIMAL(15,2) := 0;
    savepoint_name TEXT;
    lock_acquired BOOLEAN;
    failed_items JSONB := '[]'::JSONB;
    company_currency VARCHAR(3);
BEGIN
    -- Initialize outputs
    successful_count := 0;
    failed_count := 0;
    failed_details := '[]'::JSONB;
    batch_summary := '';
    
    -- Try to acquire advisory lock for this company (prevent concurrent processing)
    SELECT pg_try_advisory_xact_lock(
        hashtext(company_account_number)
    ) INTO lock_acquired;
    
    IF NOT lock_acquired THEN
        RAISE EXCEPTION 'Batch processing already in progress for this company account: %', company_account_number;
    END IF;
    
    -- Get and lock company account
    SELECT a.*, c.daily_limit_kzt INTO company_account
    FROM accounts a
    JOIN customers c ON a.customer_id = c.customer_id
    WHERE a.account_number = company_account_number
    FOR UPDATE;
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Company account not found: %', company_account_number;
    END IF;
    
    IF NOT company_account.is_active THEN
        RAISE EXCEPTION 'Company account is inactive: %', company_account_number;
    END IF;
    
    company_currency := company_account.currency;
    
    -- Validate company customer status
    IF (SELECT status FROM customers WHERE customer_id = company_account.customer_id) != 'active' THEN
        RAISE EXCEPTION 'Company customer is not active';
    END IF;
    
    -- Calculate total batch amount in company's currency
    SELECT SUM((payment->>'amount')::DECIMAL(15,2)) INTO total_amount
    FROM jsonb_array_elements(payments_json) AS payment;
    
    -- Validate company has sufficient funds
    IF company_account.balance < total_amount THEN
        RAISE EXCEPTION 'Insufficient company balance. Required: % %, Available: % %', 
                        total_amount, company_currency, 
                        company_account.balance, company_currency;
    END IF;
    
    -- Start main transaction
    BEGIN
        -- Create a savepoint for the entire batch
        SAVEPOINT batch_processing;
        
        -- Process each payment
        FOR payment IN (
            SELECT 
                value->>'iin' as iin,
                (value->>'amount')::DECIMAL(15,2) as amount,
                value->>'description' as description
            FROM jsonb_array_elements(payments_json) AS value
        )
        LOOP
            -- Create savepoint for each individual payment
            savepoint_name := 'payment_' || payment.iin || '_' || EXTRACT(EPOCH FROM NOW());
            SAVEPOINT savepoint_name;
            
            BEGIN
                -- Find employee account by customer TIN (IIN)
                DECLARE
                    employee_account RECORD;
                    employee_currency VARCHAR(3);
                    conversion_rate DECIMAL(10,6);
                    converted_amount DECIMAL(15,2);
                BEGIN
                    -- Find active account for employee
                    SELECT a.* INTO employee_account
                    FROM accounts a
                    JOIN customers c ON a.customer_id = c.customer_id
                    WHERE c.tin = payment.iin
                      AND a.is_active = TRUE
                    FOR UPDATE;
                    
                    IF NOT FOUND THEN
                        RAISE EXCEPTION USING 
                            ERRCODE = 'EMP001',
                            MESSAGE = 'Employee account not found for IIN: ' || payment.iin;
                    END IF;
                    
                    -- Validate employee customer status
                    IF (SELECT status FROM customers WHERE tin = payment.iin) != 'active' THEN
                        RAISE EXCEPTION USING 
                            ERRCODE = 'EMP002',
                            MESSAGE = 'Employee customer is not active: ' || payment.iin;
                    END IF;
                    
                    employee_currency := employee_account.currency;
                    
                    -- Handle currency conversion if needed
                    IF company_currency = employee_currency THEN
                        converted_amount := payment.amount;
                        conversion_rate := 1.0;
                    ELSE
                        -- Get conversion rate
                        SELECT rate INTO conversion_rate
                        FROM exchange_rates
                        WHERE from_currency = company_currency
                            AND to_currency = employee_currency
                            AND valid_from <= NOW()
                            AND (valid_to IS NULL OR valid_to > NOW())
                        ORDER BY valid_from DESC
                        LIMIT 1;
                        
                        IF NOT FOUND THEN
                            RAISE EXCEPTION USING 
                                ERRCODE = 'EXC001',
                                MESSAGE = 'Exchange rate not found from ' || company_currency || ' to ' || employee_currency;
                        END IF;
                        
                        converted_amount := payment.amount * conversion_rate;
                    END IF;
                    
                    -- Deduct from company account
                    UPDATE accounts 
                    SET balance = balance - payment.amount
                    WHERE account_id = company_account.account_id;
                    
                    -- Add to employee account
                    UPDATE accounts 
                    SET balance = balance + converted_amount
                    WHERE account_id = employee_account.account_id;
                    
                    -- Calculate amount in KZT for transaction record
                    DECLARE
                        amount_kzt_value DECIMAL(15,2);
                        kzt_rate DECIMAL(10,6);
                    BEGIN
                        IF company_currency = 'KZT' THEN
                            amount_kzt_value := payment.amount;
                        ELSE
                            SELECT rate INTO kzt_rate
                            FROM exchange_rates
                            WHERE from_currency = company_currency
                                AND to_currency = 'KZT'
                                AND valid_from <= NOW()
                                AND (valid_to IS NULL OR valid_to > NOW())
                            ORDER BY valid_from DESC
                            LIMIT 1;
                            
                            amount_kzt_value := payment.amount * kzt_rate;
                        END IF;
                        
                        -- Record transaction with special salary flag (bypassing daily limits)
                        INSERT INTO transactions (
                            from_account_id, to_account_id, amount, currency, 
                            exchange_rate, amount_kzt, type, status, 
                            completed_at, description
                        ) VALUES (
                            company_account.account_id, employee_account.account_id, 
                            payment.amount, company_currency, 
                            conversion_rate,
                            amount_kzt_value, 'transfer', 'completed',
                            NOW(), 'SALARY: ' || COALESCE(payment.description, 'Monthly salary')
                        );
                    END;
                    
                    successful_count := successful_count + 1;
                    processed_amount := processed_amount + payment.amount;
                    
                    -- Log successful payment to audit
                    INSERT INTO audit_log (table_name, record_id, action, new_values, changed_by)
                    VALUES ('batch_processing', employee_account.account_id, 'INSERT', 
                            jsonb_build_object(
                                'type', 'salary_payment',
                                'company_account', company_account_number,
                                'employee_iin', payment.iin,
                                'amount', payment.amount,
                                'currency', company_currency,
                                'converted_amount', converted_amount,
                                'employee_currency', employee_currency,
                                'status', 'success'
                            ), 'batch_processor');
                    
                EXCEPTION
                    WHEN OTHERS THEN
                        -- Release savepoint for this failed payment
                        ROLLBACK TO SAVEPOINT savepoint_name;
                        failed_count := failed_count + 1;
                        
                        -- Add to failed details
                        failed_items := failed_items || jsonb_build_object(
                            'iin', payment.iin,
                            'amount', payment.amount,
                            'error', SQLERRM,
                            'error_code', SQLSTATE,
                            'timestamp', NOW()
                        );
                        
                        -- Log failed attempt
                        INSERT INTO audit_log (table_name, record_id, action, new_values, changed_by)
                        VALUES ('batch_processing', 0, 'INSERT', 
                                jsonb_build_object(
                                    'type', 'salary_payment_failed',
                                    'company_account', company_account_number,
                                    'employee_iin', payment.iin,
                                    'amount', payment.amount,
                                    'error', SQLERRM,
                                    'error_code', SQLSTATE,
                                    'status', 'failed'
                                ), 'batch_processor');
                END;
            END;
        END LOOP;
        
        -- Update audit log with batch summary
        INSERT INTO audit_log (table_name, record_id, action, new_values, changed_by)
        VALUES ('batch_processing', 0, 'INSERT', 
                jsonb_build_object(
                    'type', 'batch_summary',
                    'company_account', company_account_number,
                    'total_payments', jsonb_array_length(payments_json),
                    'successful', successful_count,
                    'failed', failed_count,
                    'total_amount', total_amount,
                    'processed_amount', processed_amount,
                    'company_currency', company_currency,
                    'processing_time', NOW()
                ), 'batch_processor');
        
        -- Set output parameters
        failed_details := failed_items;
        batch_summary := format(
            'Batch processing completed. Successful: %s, Failed: %s, Total amount: %s %s, Processed: %s %s',
            successful_count, failed_count, total_amount, company_currency, processed_amount, company_currency
        );
        
        -- Release the batch savepoint (all successful)
        RELEASE SAVEPOINT batch_processing;
        
    EXCEPTION
        WHEN OTHERS THEN
            -- Rollback entire batch on critical error
            ROLLBACK TO SAVEPOINT batch_processing;
            ROLLBACK;
            RAISE;
    END;
END;
$$;

-- Materialized view for batch summary
CREATE MATERIALIZED VIEW IF NOT EXISTS batch_processing_summary AS
SELECT 
    DATE(changed_at) as processing_date,
    (new_values->>'company_account')::VARCHAR as company_account,
    COUNT(*) as batch_count,
    SUM((new_values->>'successful')::INTEGER) as total_successful,
    SUM((new_values->>'failed')::INTEGER) as total_failed,
    SUM((new_values->>'total_amount')::DECIMAL) as total_amount,
    (new_values->>'company_currency')::VARCHAR as currency,
    MAX(changed_at) as last_processed
FROM audit_log
WHERE table_name = 'batch_processing'
  AND action = 'INSERT'
  AND (new_values->>'type')::VARCHAR = 'batch_summary'
GROUP BY DATE(changed_at), (new_values->>'company_account')::VARCHAR, (new_values->>'company_currency')::VARCHAR
ORDER BY processing_date DESC;

-- Refresh function for materialized view
CREATE OR REPLACE FUNCTION refresh_batch_summary()
RETURNS VOID AS $$
BEGIN
    REFRESH MATERIALIZED VIEW batch_processing_summary;
END;
$$ LANGUAGE plpgsql;

-------------------------------
-- 7. HELPER FUNCTIONS AND TRIGGERS
-------------------------------

-- Function to get current exchange rate
CREATE OR REPLACE FUNCTION get_exchange_rate(
    from_curr VARCHAR(3),
    to_curr VARCHAR(3)
)
RETURNS DECIMAL(10,6) AS $$
DECLARE
    current_rate DECIMAL(10,6);
BEGIN
    SELECT rate INTO current_rate
    FROM exchange_rates
    WHERE from_currency = from_curr 
      AND to_currency = to_curr
      AND valid_from <= NOW()
      AND (valid_to IS NULL OR valid_to > NOW())
    ORDER BY valid_from DESC
    LIMIT 1;
    
    RETURN COALESCE(current_rate, 1.0);
END;
$$ LANGUAGE plpgsql;

-- Function to calculate customer's daily transaction total
CREATE OR REPLACE FUNCTION get_daily_transaction_total(
    customer_id_param INTEGER
)
RETURNS DECIMAL(15,2) AS $$
DECLARE
    daily_total DECIMAL(15,2);
BEGIN
    SELECT COALESCE(SUM(t.amount_kzt), 0) INTO daily_total
    FROM transactions t
    JOIN accounts a ON t.from_account_id = a.account_id
    WHERE a.customer_id = customer_id_param
      AND t.status = 'completed'
      AND DATE(t.created_at) = CURRENT_DATE
      AND t.type = 'transfer';
    
    RETURN daily_total;
END;
$$ LANGUAGE plpgsql;

-- Trigger function for audit logging
CREATE OR REPLACE FUNCTION audit_trigger_function()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO audit_log (table_name, record_id, action, new_values, changed_by)
        VALUES (TG_TABLE_NAME, NEW.account_id, 'INSERT', row_to_json(NEW)::jsonb, current_user);
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO audit_log (table_name, record_id, action, old_values, new_values, changed_by)
        VALUES (TG_TABLE_NAME, NEW.account_id, 'UPDATE', row_to_json(OLD)::jsonb, row_to_json(NEW)::jsonb, current_user);
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO audit_log (table_name, record_id, action, old_values, changed_by)
        VALUES (TG_TABLE_NAME, OLD.account_id, 'DELETE', row_to_json(OLD)::jsonb, current_user);
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Create audit trigger for accounts table
CREATE TRIGGER accounts_audit_trigger
AFTER INSERT OR UPDATE OR DELETE ON accounts
FOR EACH ROW EXECUTE FUNCTION audit_trigger_function();

-- Create audit trigger for customers table
CREATE TRIGGER customers_audit_trigger
AFTER INSERT OR UPDATE OR DELETE ON customers
FOR EACH ROW EXECUTE FUNCTION audit_trigger_function();

-------------------------------
-- 8. TEST CASES AND DEMONSTRATION
-------------------------------

-- Test Case 1: Successful transfer (KZT to KZT)
DO $$
DECLARE
    success BOOLEAN;
    msg TEXT;
    err_code VARCHAR(5);
BEGIN
    RAISE NOTICE '=== Test 1: Successful KZT transfer ===';
    CALL process_transfer(
        'KZ12345678901234567890',
        'KZ23456789012345678901',
        10000.00,
        'KZT',
        'Test KZT transfer',
        success,
        msg,
        err_code
    );
    RAISE NOTICE 'Success: %, Message: %, Code: %', success, msg, err_code;
END $$;

-- Test Case 2: Currency mismatch error
DO $$
DECLARE
    success BOOLEAN;
    msg TEXT;
    err_code VARCHAR(5);
BEGIN
    RAISE NOTICE '=== Test 2: Currency mismatch (should fail) ===';
    CALL process_transfer(
        'KZ12345678901234567890',  -- KZT account
        'KZ23456789012345678901',
        10000.00,
        'USD',  -- Wrong currency!
        'Test currency mismatch',
        success,
        msg,
        err_code
    );
    RAISE NOTICE 'Success: %, Message: %, Code: %', success, msg, err_code;
END $$;

-- Test Case 3: Insufficient balance
DO $$
DECLARE
    success BOOLEAN;
    msg TEXT;
    err_code VARCHAR(5);
BEGIN
    RAISE NOTICE '=== Test 3: Insufficient balance (should fail) ===';
    CALL process_transfer(
        'KZ12345678901234567890',
        'KZ23456789012345678901',
        1000000.00,
        'KZT',
        'Large transfer',
        success,
        msg,
        err_code
    );
    RAISE NOTICE 'Success: %, Message: %, Code: %', success, msg, err_code;
END $$;

-- Test Case 4: Cross-currency transfer
DO $$
DECLARE
    success BOOLEAN;
    msg TEXT;
    err_code VARCHAR(5);
BEGIN
    RAISE NOTICE '=== Test 4: Cross-currency transfer (USD to KZT) ===';
    CALL process_transfer(
        'KZ09876543210987654321',  -- USD account
        'KZ23456789012345678901',  -- KZT account
        100.00,
        'USD',
        'Cross-currency transfer',
        success,
        msg,
        err_code
    );
    RAISE NOTICE 'Success: %, Message: %, Code: %', success, msg, err_code;
END $$;

-- Test Case 5: Test batch processing
DO $$
DECLARE
    successful INTEGER;
    failed INTEGER;
    details JSONB;
    summary TEXT;
    payments JSONB := '[
        {"iin": "123456789012", "amount": 500000, "description": "Salary January"},
        {"iin": "234567890123", "amount": 750000, "description": "Salary January"},
        {"iin": "345678901234", "amount": 600000, "description": "Salary January"},
        {"iin": "999999999999", "amount": 300000, "description": "Invalid IIN"}
    ]'::JSONB;
BEGIN
    RAISE NOTICE '=== Test 5: Batch salary processing ===';
    
    -- First ensure company has funds
    UPDATE accounts SET balance = 3000000 WHERE account_number = 'KZ12345678901234567890';
    
    CALL process_salary_batch(
        'KZ12345678901234567890',
        payments,
        successful,
        failed,
        details,
        summary
    );
    
    RAISE NOTICE 'Successful: %, Failed: %', successful, failed;
    RAISE NOTICE 'Summary: %', summary;
    RAISE NOTICE 'Failed details: %', details;
END $$;

-- Test Case 6: Test views
DO $$
BEGIN
    RAISE NOTICE '=== Test 6: View testing ===';
    
    RAISE NOTICE '--- Customer Balance Summary (first 3) ---';
    SELECT customer_id, full_name, total_balance_kzt, balance_rank 
    FROM customer_balance_summary 
    ORDER BY balance_rank 
    LIMIT 3;
    
    RAISE NOTICE '--- Daily Transaction Report ---';
    SELECT transaction_date, type, transaction_count, total_volume_kzt
    FROM daily_transaction_report 
    ORDER BY transaction_date DESC 
    LIMIT 5;
    
    RAISE NOTICE '--- Suspicious Activity View ---';
    SELECT reason, full_name, created_at 
    FROM suspicious_activity_view 
    LIMIT 3;
END $$;

-------------------------------
-- 9. PERFORMANCE ANALYSIS & INDEX DOCUMENTATION
-------------------------------

DO $$
BEGIN
    RAISE NOTICE '=== PERFORMANCE ANALYSIS ===';
    RAISE NOTICE 'All indexes created with justification:';
    RAISE NOTICE '1. idx_customers_tin - B-tree for fast TIN lookups (unique constraint)';
    RAISE NOTICE '2. idx_accounts_number_customer_active - Covering index for account operations';
    RAISE NOTICE '3. idx_transactions_date_status_type - Composite index for date range queries';
    RAISE NOTICE '4. idx_active_accounts - Partial index (active accounts only)';
    RAISE NOTICE '5. idx_customers_email_lower - Expression index for case-insensitive email';
    RAISE NOTICE '6. idx_audit_log_jsonb - GIN index for JSONB queries';
    RAISE NOTICE '7. idx_exchange_currencies_hash - Hash index for exact currency matches';
    RAISE NOTICE '8. idx_transactions_from_date - Composite index for daily limit checks';
    RAISE NOTICE '9. idx_audit_log_time_brin - BRIN index for time-series audit data';
    
    RAISE NOTICE '';
    RAISE NOTICE '=== EXPLAIN ANALYZE for critical queries ===';
    
    RAISE NOTICE '1. Account lookup with covering index:';
    EXPLAIN ANALYZE SELECT a.account_id, a.customer_id 
    FROM accounts a 
    WHERE a.account_number = 'KZ12345678901234567890' 
      AND a.is_active = TRUE;
    
    RAISE NOTICE '';
    RAISE NOTICE '2. Customer lookup by TIN with B-tree index:';
    EXPLAIN ANALYZE SELECT * FROM customers WHERE tin = '123456789012';
    
    RAISE NOTICE '';
    RAISE NOTICE '3. Transaction date range with composite index:';
    EXPLAIN ANALYZE SELECT * FROM transactions 
    WHERE created_at >= CURRENT_DATE - INTERVAL '7 days' 
      AND status = 'completed'
      AND type = 'transfer'
    ORDER BY created_at DESC;
    
    RAISE NOTICE '';
    RAISE NOTICE '4. Case-insensitive email search with expression index:';
    EXPLAIN ANALYZE SELECT * FROM customers 
    WHERE LOWER(email) = LOWER('aigerim@email.com');
    
    RAISE NOTICE '';
    RAISE NOTICE '5. Daily limit check query:';
    EXPLAIN ANALYZE SELECT SUM(amount_kzt) 
    FROM transactions 
    WHERE from_account_id = 1 
      AND DATE(created_at) = CURRENT_DATE 
      AND status = 'completed';
    
    RAISE NOTICE '';
    RAISE NOTICE '6. JSONB query on audit log with GIN index:';
    EXPLAIN ANALYZE SELECT * FROM audit_log 
    WHERE new_values @> '{"status": "success"}'::jsonb 
    LIMIT 10;
    
END $$;

-------------------------------
-- 10. CONCURRENCY TESTING DEMONSTRATION
-------------------------------

DO $$
BEGIN
    RAISE NOTICE '=== CONCURRENCY TESTING INSTRUCTIONS ===';
    RAISE NOTICE '';
    RAISE NOTICE 'TEST 1: Row-level locking with SELECT FOR UPDATE';
    RAISE NOTICE 'Session 1: BEGIN;';
    RAISE NOTICE 'Session 1: SELECT * FROM accounts WHERE account_number = ''KZ12345678901234567890'' FOR UPDATE;';
    RAISE NOTICE 'Session 2: BEGIN;';
    RAISE NOTICE 'Session 2: SELECT * FROM accounts WHERE account_number = ''KZ12345678901234567890'' FOR UPDATE NOWAIT;';
    RAISE NOTICE '-- Session 2 should fail with "could not obtain lock"';
    RAISE NOTICE 'Session 1: COMMIT;';
    RAISE NOTICE '';
    RAISE NOTICE 'TEST 2: Advisory locks for batch processing';
    RAISE NOTICE 'Run process_salary_batch in two sessions simultaneously';
    RAISE NOTICE 'Second session should wait or fail if advisory lock is held';
    RAISE NOTICE '';
    RAISE NOTICE 'TEST 3: Concurrent transfers between same accounts';
    RAISE NOTICE 'Run multiple process_transfer calls in parallel sessions';
    RAISE NOTICE 'All should complete successfully with proper locking';
END $$;

