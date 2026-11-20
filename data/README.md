# Synthetic demo extracts

These pipe-delimited records are generated demonstration data, not source files supplied by the hackathon and not real bank/customer data. Each file uses interleaved `#ENTITY:<name>` blocks with a header row per block.

- `day0/day0.txt` is the baseline with ten entities: Customers, Accounts, Transactions, Branches, AccountTypes, TransactionTypes, Loans, LoanPayments, Cards, and Products.
- `day1/day1.txt` contains only Customer and Account changes; it updates existing keys and inserts customers 369/370 and accounts 481/482.
- `day2/day2.txt` contains only Customer and Account changes; it updates existing keys and inserts customers 371/372 and accounts 483/484.

Day 0 deliberately exercises the brief's DQ and anomaly cases: invalid email, missing phone, conflicting customer identity, exact duplicate transaction, currency mismatch, transaction before account open, branch sort-code prefix mismatch, overdraft breach, repeated near-threshold deposit activity via ATM, and a dormant-account reactivation burst. These cases validate the pipeline's quarantine/findings behavior; their counts are not results for actual BankEvalDB data.
