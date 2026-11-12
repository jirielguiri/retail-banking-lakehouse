# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# ============================================================================
# Phase 2: Synthetic Retail Banking Data Generator (Bronze Landing Volume)
# Writes realistic Day 1 & Day 2 CSV/JSON batches into Unity Catalog Volume
# ============================================================================

import os
import json
import csv
import random
from datetime import datetime, timedelta

# Ensure Catalog, Schemas, and Volume exist before writing files
spark.sql("CREATE CATALOG IF NOT EXISTS retail_banking_dw")
spark.sql("CREATE SCHEMA IF NOT EXISTS retail_banking_dw.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS retail_banking_dw.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS retail_banking_dw.gold")
spark.sql("CREATE VOLUME IF NOT EXISTS retail_banking_dw.bronze.landing")

LANDING_BASE_PATH = "/Volumes/retail_banking_dw/bronze/landing"
DIRS = ["customers", "products", "contracts", "transactions"]

for d in DIRS:
    os.makedirs(f"{LANDING_BASE_PATH}/{d}", exist_ok=True)

random.seed(42)

def write_jsonl(filepath: str, records: list):
    with open(filepath, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
    print(f"Wrote {len(records)} records to {filepath}")

def write_csv(filepath: str, fieldnames: list, records: list):
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)
    print(f"Wrote {len(records)} records to {filepath}")

# ----------------------------------------------------------------------------
# 1. Generate Reference Products (products_batch_01.json)
# ----------------------------------------------------------------------------
products = [
    {
        "product_code": "MORT_FIX_5Y",
        "product_name": "Hypoteka 5Y Fix",
        "product_domain": "LENDING",
        "product_category": "MORTGAGE",
        "base_interest_rate": 0.0489,
        "max_term_months": 360,
        "collateral_required": True,
        "updated_at": "2026-01-01T00:00:00Z"
    },
    {
        "product_code": "AUTO_LOAN_3Y",
        "product_name": "Uver na Auto 3Y",
        "product_domain": "LENDING",
        "product_category": "AUTO_LOAN",
        "base_interest_rate": 0.0590,
        "max_term_months": 60,
        "collateral_required": True,
        "updated_at": "2026-01-01T00:00:00Z"
    },
    {
        "product_code": "CONS_UNSEC",
        "product_name": "Spotrebitelsky Uver Expres",
        "product_domain": "LENDING",
        "product_category": "CONSUMER_LOAN",
        "base_interest_rate": 0.0890,
        "max_term_months": 96,
        "collateral_required": False,
        "updated_at": "2026-01-01T00:00:00Z"
    },
    {
        "product_code": "CURR_ACCT_STD",
        "product_name": "Bezny Ucet Standard",
        "product_domain": "DEPOSITS",
        "product_category": "CURRENT_ACCOUNT",
        "base_interest_rate": 0.0010,
        "max_term_months": 0,
        "collateral_required": False,
        "updated_at": "2026-01-01T00:00:00Z"
    }
]
write_jsonl(f"{LANDING_BASE_PATH}/products/products_batch_01.json", products)

# ----------------------------------------------------------------------------
# 2. Generate Customers: Batch 1 (Initial Load) & Batch 2 (SCD2 + Dirty Data)
# ----------------------------------------------------------------------------
first_names = ["Jan", "Eva", "Petr", "Lucie", "Tomas", "Marie", "Jiri", "Katerina", "Martin", "Veronika"]
last_names = ["Novak", "Svobodova", "Dvorak", "Cerna", "Prochazka", "Kucerova", "Vesely", "Horakova", "Nemec", "Markova"]
regions_cities = [
    ("PRAGUE", "Praha"),
    ("PRAGUE", "Jesenice"),
    ("BRNO", "Brno"),
    ("OSTRAVA", "Ostrava")
]

customers_batch_1 = [
    {
        "customer_id": "CUST-1001",
        "national_id": "850115/1234",
        "first_name": "Jan",
        "last_name": "Novak",
        "email": "jan.novak@email.cz",
        "phone_number": "+420602111222",
        "branch_region": "PRAGUE",
        "banking_segment": "RETAIL",
        "credit_score": 680,
        "address_city": "Praha",
        "customer_status": "ACTIVE",
        "updated_at": "2026-01-10T09:00:00Z"
    },
    {
        "customer_id": "CUST-1002",
        "national_id": "905620/5678",
        "first_name": "Eva",
        "last_name": "Svobodova",
        "email": "eva.svobodova@seznam.cz",
        "phone_number": "+420777333444",
        "branch_region": "BRNO",
        "banking_segment": "PREMIUM",
        "credit_score": 790,
        "address_city": "Brno",
        "customer_status": "ACTIVE",
        "updated_at": "2026-01-10T09:05:00Z"
    },
    {
        "customer_id": "CUST-1003",
        "national_id": "780912/9988",
        "first_name": "Petr",
        "last_name": "Dvorak",
        "email": "petr.dvorak@centrum.cz",
        "phone_number": "+420608555666",
        "branch_region": "OSTRAVA",
        "banking_segment": "RETAIL",
        "credit_score": 520,
        "address_city": "Ostrava",
        "customer_status": "ACTIVE",
        "updated_at": "2026-01-10T09:10:00Z"
    }
]

for i in range(1004, 1051):
    fn = random.choice(first_names)
    ln = random.choice(last_names)
    reg, city = random.choice(regions_cities)
    score = random.randint(480, 820)
    seg = "PREMIUM" if score > 740 else random.choice(["RETAIL", "RETAIL", "PREMIUM"])
    customers_batch_1.append({
        "customer_id": f"CUST-{i}",
        "national_id": f"{random.randint(70,99):02d}{random.randint(1,12):02d}{random.randint(10,28):02d}/{random.randint(1000,9999)}",
        "first_name": fn,
        "last_name": ln,
        "email": f"{fn.lower()}.{ln.lower()}{i}@email.cz",
        "phone_number": f"+420{random.randint(601000000, 779999999)}",
        "branch_region": reg,
        "banking_segment": seg,
        "credit_score": score,
        "address_city": city,
        "customer_status": "ACTIVE",
        "updated_at": "2026-01-10T10:00:00Z"
    })

write_jsonl(f"{LANDING_BASE_PATH}/customers/customers_batch_01.json", customers_batch_1)

# Batch 2: SCD Type 2 updates + Intentional Malformed/Dirty Records for Quarantine
customers_batch_2 = [
    # SCD2 Update 1: Jan Novak moves to Jesenice, promoted to PREMIUM, credit score 680 -> 765 (Tier B -> Tier A)
    {
        "customer_id": "CUST-1001",
        "national_id": "850115/1234",
        "first_name": "Jan",
        "last_name": "Novak",
        "email": "jan.novak@email.cz",
        "phone_number": "+420602111222",
        "branch_region": "PRAGUE",
        "banking_segment": "PREMIUM",
        "credit_score": 765,
        "address_city": "Jesenice",
        "customer_status": "ACTIVE",
        "updated_at": "2026-02-15T14:30:00Z"
    },
    # SCD2 Update 2: CUST-1004 credit score drop
    {
        **customers_batch_1[3],
        "credit_score": 540,
        "banking_segment": "RETAIL",
        "updated_at": "2026-02-16T11:00:00Z"
    },
    # Dirty Record 1: Null PK + non-integer credit_score (triggers Auto Loader _rescued_data & Quarantine)
    {
        "customer_id": None,
        "national_id": "990101/0000",
        "first_name": "Ghost",
        "last_name": "User",
        "email": "bad@record.cz",
        "phone_number": "+420000000000",
        "branch_region": "PRAGUE",
        "banking_segment": "RETAIL",
        "credit_score": "INVALID_SCORE",
        "address_city": "Praha",
        "customer_status": "ACTIVE",
        "updated_at": "2026-02-15T15:00:00Z"
    }
]
write_jsonl(f"{LANDING_BASE_PATH}/customers/customers_batch_02.json", customers_batch_2)

# ----------------------------------------------------------------------------
# 3. Generate Contracts (contracts_batch_01.csv)
# ----------------------------------------------------------------------------
contract_fields = [
    "contract_id", "customer_id", "product_code", "origination_date",
    "maturity_date", "principal_amount", "interest_rate", "installment_amount",
    "currency_code", "contract_status", "updated_at"
]

contracts_batch_1 = [
    {
        "contract_id": "CNTR-5001", "customer_id": "CUST-1001", "product_code": "MORT_FIX_5Y",
        "origination_date": "2026-01-12", "maturity_date": "2051-01-12",
        "principal_amount": 4500000.00, "interest_rate": 0.0489, "installment_amount": 26015.50,
        "currency_code": "CZK", "contract_status": "ACTIVE", "updated_at": "2026-01-12T10:00:00Z"
    },
    {
        "contract_id": "CNTR-5002", "customer_id": "CUST-1002", "product_code": "AUTO_LOAN_3Y",
        "origination_date": "2026-01-15", "maturity_date": "2029-01-15",
        "principal_amount": 600000.00, "interest_rate": 0.0590, "installment_amount": 18225.00,
        "currency_code": "CZK", "contract_status": "ACTIVE", "updated_at": "2026-01-15T11:20:00Z"
    },
    {
        "contract_id": "CNTR-5003", "customer_id": "CUST-1003", "product_code": "CONS_UNSEC",
        "origination_date": "2026-01-18", "maturity_date": "2031-01-18",
        "principal_amount": 250000.00, "interest_rate": 0.0890, "installment_amount": 5177.00,
        "currency_code": "CZK", "contract_status": "DELINQUENT", "updated_at": "2026-02-28T18:00:00Z"
    },
    # Dirty Contract: Negative principal & 125% interest rate -> Routed to Quarantine
    {
        "contract_id": "CNTR-5004", "customer_id": "CUST-1001", "product_code": "CONS_UNSEC",
        "origination_date": "2026-01-20", "maturity_date": "2028-01-20",
        "principal_amount": -50000.00, "interest_rate": 1.2500, "installment_amount": 2500.00,
        "currency_code": "CZK", "contract_status": "ACTIVE", "updated_at": "2026-01-20T12:00:00Z"
    }
]

lending_specs = [
    ("MORT_FIX_5Y", 2500000, 8000000, 0.0489, 25),
    ("AUTO_LOAN_3Y", 250000, 900000, 0.0590, 4),
    ("CONS_UNSEC", 50000, 400000, 0.0890, 5)
]

for idx in range(5005, 5060):
    cust_id = f"CUST-{random.randint(1004, 1050)}"
    p_code, min_p, max_p, rate, years = random.choice(lending_specs)
    principal = float(round(random.randint(min_p, max_p), -3))
    installment = round((principal * (1 + rate * years)) / (years * 12), 2)
    status = random.choices(["ACTIVE", "DELINQUENT", "DEFAULTED"], weights=[0.82, 0.13, 0.05])[0]
    contracts_batch_1.append({
        "contract_id": f"CNTR-{idx}",
        "customer_id": cust_id,
        "product_code": p_code,
        "origination_date": "2026-01-15",
        "maturity_date": f"{2026 + years}-01-15",
        "principal_amount": principal,
        "interest_rate": rate,
        "installment_amount": installment,
        "currency_code": "CZK",
        "contract_status": status,
        "updated_at": "2026-01-15T12:00:00Z"
    })

write_csv(f"{LANDING_BASE_PATH}/contracts/contracts_batch_01.csv", contract_fields, contracts_batch_1)

# ----------------------------------------------------------------------------
# 4. Generate Financial Transactions (Batch 1 & Batch 2 with Schema Evolution)
# ----------------------------------------------------------------------------
transactions_batch_1 = [
    # Initial Disbursement for CNTR-5001 (Stored in Silver 3NF, excluded from Gold Repayment Fact)
    {
        "transaction_id": "TXN-9000", "contract_id": "CNTR-5001", "transaction_type_code": "DISBURSEMENT",
        "transaction_date": "2026-01-12", "transaction_timestamp": "2026-01-12T12:00:00Z",
        "principal_amount": 4500000.00, "interest_amount": 0.00, "fee_amount": 0.00,
        "payment_channel": "WIRE", "days_past_due": 0
    },
    # Feb 1 Repayment for CNTR-5001 (Joins to Jan Novak's Tier B / RETAIL SCD2 record)
    {
        "transaction_id": "TXN-9001", "contract_id": "CNTR-5001", "transaction_type_code": "SCHEDULED_REPAYMENT",
        "transaction_date": "2026-02-01", "transaction_timestamp": "2026-02-01T08:00:00Z",
        "principal_amount": 7678.00, "interest_amount": 18337.50, "fee_amount": 0.00,
        "payment_channel": "DIRECT_DEBIT", "days_past_due": 0
    },
    # Exact Duplicate arriving 5 seconds later -> Deduplicated in Silver
    {
        "transaction_id": "TXN-9001", "contract_id": "CNTR-5001", "transaction_type_code": "SCHEDULED_REPAYMENT",
        "transaction_date": "2026-02-01", "transaction_timestamp": "2026-02-01T08:00:05Z",
        "principal_amount": 7678.00, "interest_amount": 18337.50, "fee_amount": 0.00,
        "payment_channel": "DIRECT_DEBIT", "days_past_due": 0
    },
    # Delinquent payment (42 DPD) for CNTR-5003
    {
        "transaction_id": "TXN-9003", "contract_id": "CNTR-5003", "transaction_type_code": "SCHEDULED_REPAYMENT",
        "transaction_date": "2026-02-28", "transaction_timestamp": "2026-02-28T16:45:00Z",
        "principal_amount": 3322.80, "interest_amount": 1854.20, "fee_amount": 500.00,
        "payment_channel": "MOBILE_APP", "days_past_due": 42
    },
    # Dirty Transaction: Negative payment -> Routed to Quarantine
    {
        "transaction_id": "TXN-9999", "contract_id": "CNTR-5002", "transaction_type_code": "SCHEDULED_REPAYMENT",
        "transaction_date": "2026-02-15", "transaction_timestamp": "2026-02-15T10:00:00Z",
        "principal_amount": -1000.00, "interest_amount": 0.00, "fee_amount": 0.00,
        "payment_channel": "WIRE", "days_past_due": 0
    }
]

txn_counter = 9010
for c in contracts_batch_1:
    if c["contract_id"] in ["CNTR-5001", "CNTR-5003", "CNTR-5004"]:
        continue
    # Feb & Mar repayments for all generated contracts
    for month, m_str in [(2, "2026-02-15"), (3, "2026-03-15")]:
        txn_counter += 1
        inst = float(c["installment_amount"])
        prin = round(inst * 0.65, 2)
        intr = round(inst - prin, 2)
        dpd = 0 if c["contract_status"] == "ACTIVE" else (random.randint(31, 75) if c["contract_status"] == "DELINQUENT" else random.randint(95, 150))
        fee = 500.00 if dpd > 30 else 0.00
        transactions_batch_1.append({
            "transaction_id": f"TXN-{txn_counter}",
            "contract_id": c["contract_id"],
            "transaction_type_code": "SCHEDULED_REPAYMENT",
            "transaction_date": m_str,
            "transaction_timestamp": f"{m_str}T09:00:00Z",
            "principal_amount": prin,
            "interest_amount": intr,
            "fee_amount": fee,
            "payment_channel": random.choice(["DIRECT_DEBIT", "MOBILE_APP", "WIRE"]),
            "days_past_due": dpd
        })

write_jsonl(f"{LANDING_BASE_PATH}/transactions/transactions_batch_01.json", transactions_batch_1)

# Batch 2: March Early Repayment for CNTR-5001 (Joins to Jan Novak's NEW Tier A / PREMIUM SCD2 record)
# Also introduces new field `early_repayment_penalty` to showcase Auto Loader Schema Evolution
transactions_batch_2 = [
    {
        "transaction_id": "TXN-9002", "contract_id": "CNTR-5001", "transaction_type_code": "EARLY_REPAYMENT",
        "transaction_date": "2026-03-01", "transaction_timestamp": "2026-03-01T08:00:00Z",
        "principal_amount": 57709.30, "interest_amount": 18306.20, "fee_amount": 0.00,
        "payment_channel": "DIRECT_DEBIT", "days_past_due": 0,
        "early_repayment_penalty": 0.00
    }
]
write_jsonl(f"{LANDING_BASE_PATH}/transactions/transactions_batch_02.json", transactions_batch_2)

print("\nAll synthetic banking files successfully generated in /Volumes/retail_banking_dw/bronze/landing!")