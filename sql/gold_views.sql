-- Reusable Gold analytics views. Replace __PROJECT__ before submitting to BigQuery.

CREATE OR REPLACE VIEW `__PROJECT__.bank_analytics.kpi_top_5_customers` AS
SELECT c.CustomerID, CONCAT(COALESCE(c.FirstName, ''), ' ', COALESCE(c.LastName, '')) AS customer_name,
       COUNT(*) AS transaction_count, SUM(ABS(t.Amount)) AS transaction_volume
FROM `__PROJECT__.bank_analytics.fact_transaction` t
JOIN `__PROJECT__.bank_analytics.dim_customer` c USING (customer_sk)
GROUP BY c.CustomerID, customer_name
ORDER BY transaction_volume DESC
LIMIT 5;

CREATE OR REPLACE VIEW `__PROJECT__.bank_analytics.kpi_product_revenue` AS
SELECT p.ProductID, p.ProductName, p.ProductCategory, acct_type.AccountTypeID,
       COUNT(*) AS transaction_count, SUM(ABS(t.Amount)) AS total_transaction_value
FROM `__PROJECT__.bank_analytics.fact_transaction` t
JOIN `__PROJECT__.bank_analytics.dim_account_type` acct_type USING (account_type_sk)
JOIN `__PROJECT__.bank_analytics.dim_product` p USING (product_sk)
GROUP BY p.ProductID, p.ProductName, p.ProductCategory, acct_type.AccountTypeID;

CREATE OR REPLACE VIEW `__PROJECT__.bank_analytics.kpi_transaction_category_channel` AS
SELECT tt.Category, tt.TransactionTypeName, t.Channel,
       COUNT(*) AS transaction_count, SUM(ABS(t.Amount)) AS transaction_volume
FROM `__PROJECT__.bank_analytics.fact_transaction` t
JOIN `__PROJECT__.bank_analytics.dim_transaction_type` tt USING (transaction_type_sk)
GROUP BY tt.Category, tt.TransactionTypeName, t.Channel;

CREATE OR REPLACE VIEW `__PROJECT__.bank_analytics.kpi_branch_transaction_trends` AS
SELECT b.BranchID, b.BranchName, b.Region, DATE_TRUNC(t.TransactionDate, MONTH) AS transaction_month,
       COUNT(*) AS transaction_count, SUM(ABS(t.Amount)) AS transaction_volume
FROM `__PROJECT__.bank_analytics.fact_transaction` t
JOIN `__PROJECT__.bank_analytics.dim_branch` b USING (branch_sk)
GROUP BY b.BranchID, b.BranchName, b.Region, transaction_month;

CREATE OR REPLACE VIEW `__PROJECT__.bank_analytics.kpi_inactive_accounts` AS
WITH activity AS (
  SELECT AccountID, MAX(TransactionDate) AS last_transaction_date
  FROM `__PROJECT__.bank_analytics.fact_transaction`
  GROUP BY AccountID
), analysis_date AS (
  SELECT COALESCE(MAX(TransactionDate), CURRENT_DATE()) AS as_of_date
  FROM `__PROJECT__.bank_analytics.fact_transaction`
), rule AS (
  SELECT COALESCE(SAFE_CAST(rule_value AS INT64), 3) AS inactive_months
  FROM `__PROJECT__.bank_analytics.metadata_validation_rules`
  WHERE rule_name = 'inactive_months'
  LIMIT 1
)
SELECT a.*, activity.last_transaction_date, analysis_date.as_of_date
FROM `__PROJECT__.bank_analytics.dim_account` a
LEFT JOIN activity USING (AccountID)
CROSS JOIN analysis_date
CROSS JOIN rule
WHERE a._is_current
  AND (activity.last_transaction_date IS NULL OR
       activity.last_transaction_date < DATE_SUB(analysis_date.as_of_date, INTERVAL rule.inactive_months MONTH));

CREATE OR REPLACE VIEW `__PROJECT__.bank_analytics.kpi_suspicious_transactions` AS
WITH rule AS (
  SELECT COALESCE(SAFE_CAST(rule_value AS NUMERIC), 100000) AS suspicious_amount_gbp
  FROM `__PROJECT__.bank_analytics.metadata_validation_rules`
  WHERE rule_name = 'suspicious_amount_gbp'
  LIMIT 1
)
SELECT t.*
FROM `__PROJECT__.bank_analytics.fact_transaction` t
CROSS JOIN rule
WHERE ABS(t.Amount) >= rule.suspicious_amount_gbp;

CREATE OR REPLACE VIEW `__PROJECT__.bank_analytics.kpi_loan_portfolio_repayment_health` AS
WITH payments AS (
  SELECT LoanID,
         SUM(IF(UPPER(Status) IN ('PAID', 'POSTED', 'SETTLED'), Amount, 0)) AS paid_amount,
         COUNTIF(UPPER(Status) NOT IN ('PAID', 'POSTED', 'SETTLED')) AS non_paid_payment_count
  FROM `__PROJECT__.bank_analytics.silver_loan_payments`
  GROUP BY LoanID
)
SELECT l.LoanID, l.CustomerID, l.AccountID, l.BranchID, b.BranchName, l.LoanType,
       l.Principal, l.Status AS loan_status, COALESCE(p.paid_amount, 0) AS paid_amount,
       GREATEST(l.Principal - COALESCE(p.paid_amount, 0), 0) AS estimated_remaining_principal,
       SAFE_DIVIDE(COALESCE(p.paid_amount, 0), l.Principal) AS paid_to_principal_ratio,
       COALESCE(p.non_paid_payment_count, 0) AS non_paid_payment_count
FROM `__PROJECT__.bank_analytics.silver_loans` l
LEFT JOIN payments p USING (LoanID)
LEFT JOIN `__PROJECT__.bank_analytics.silver_branches` b USING (BranchID);

CREATE OR REPLACE VIEW `__PROJECT__.bank_analytics.kpi_card_product_adoption` AS
SELECT c.CustomerID, CONCAT(COALESCE(c.FirstName, ''), ' ', COALESCE(c.LastName, '')) AS customer_name,
       a.AccountID, acct_type.AccountTypeName, p.ProductName,
       COUNTIF(UPPER(card.Status) = 'ACTIVE') AS active_card_count,
       COUNT(card.CardID) AS total_card_count
FROM `__PROJECT__.bank_analytics.dim_customer` c
JOIN `__PROJECT__.bank_analytics.dim_account` a ON c.CustomerID = a.CustomerID AND a._is_current
LEFT JOIN `__PROJECT__.bank_analytics.silver_cards` card USING (AccountID)
LEFT JOIN `__PROJECT__.bank_analytics.silver_account_types` acct_type USING (AccountTypeID)
LEFT JOIN `__PROJECT__.bank_analytics.silver_products` p USING (ProductID)
WHERE c._is_current
GROUP BY c.CustomerID, customer_name, a.AccountID, acct_type.AccountTypeName, p.ProductName;

CREATE OR REPLACE VIEW `__PROJECT__.bank_analytics.kpi_customer_scd2_history` AS
SELECT CustomerID, FirstName, LastName, Email, Phone, _effective_from, _effective_to, _is_current
FROM `__PROJECT__.bank_analytics.dim_customer`;

CREATE OR REPLACE VIEW `__PROJECT__.bank_analytics.kpi_account_scd2_history` AS
SELECT AccountID, CustomerID, BranchID, AccountTypeID, Currency, Balance,
       _effective_from, _effective_to, _is_current
FROM `__PROJECT__.bank_analytics.dim_account`;

CREATE OR REPLACE VIEW `__PROJECT__.bank_analytics.kpi_data_quality_summary` AS
SELECT _entity AS entity, _dq_reason AS reason, _dq_severity AS severity, SUM(row_count) AS row_count
FROM `__PROJECT__.bank_analytics.data_quality_summary`
GROUP BY entity, reason, severity;

CREATE OR REPLACE VIEW `__PROJECT__.bank_analytics.kpi_transaction_profile` AS
SELECT COUNT(*) AS bronze_source_rows,
       COUNT(DISTINCT TransactionID) AS distinct_transaction_ids,
       COUNTIF(TransactionID IS NULL OR TRIM(TransactionID) = '') AS missing_transaction_ids,
       COUNTIF(SAFE_CAST(Amount AS NUMERIC) IS NULL) AS invalid_amount_rows,
       COUNTIF(SAFE_CAST(TransactionDate AS DATE) IS NULL) AS invalid_transaction_date_rows,
       MIN(SAFE_CAST(TransactionDate AS DATE)) AS earliest_transaction_date,
       MAX(SAFE_CAST(TransactionDate AS DATE)) AS latest_transaction_date,
       COUNTIF(ABS(SAFE_CAST(Amount AS NUMERIC)) >= 100000) AS high_value_transactions
FROM `__PROJECT__.bank_analytics.bronze_transactions`;

CREATE OR REPLACE VIEW `__PROJECT__.bank_analytics.kpi_customer_profile` AS
SELECT COUNT(*) AS bronze_source_rows,
       COUNT(DISTINCT CustomerID) AS distinct_customer_ids,
       COUNTIF(CustomerID IS NULL OR TRIM(CustomerID) = '') AS missing_customer_ids,
       COUNTIF(Email IS NULL OR TRIM(Email) = '') AS missing_emails,
       COUNTIF(Email IS NOT NULL AND NOT REGEXP_CONTAINS(Email, r'^[^@\s]+@[^@\s]+\.[^@\s]+$')) AS invalid_emails,
       COUNTIF(Phone IS NULL OR TRIM(Phone) = '') AS missing_phones
FROM `__PROJECT__.bank_analytics.bronze_customers`;
