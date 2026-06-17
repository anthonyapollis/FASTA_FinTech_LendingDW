"""
FASTA / Confluent FinTech Lending DW - Bronze Layer Data Generator
Generates realistic dirty/raw data simulating:
  - FASTA loan applications
  - Confluent brand web analytics
  - JustMoney / DebtBusters referrals
  - Credit checks, affordability, repayments, arrears
  - Collections, support tickets, reviews
Dirty data patterns:
  - Nulls in non-nullable fields
  - Duplicate rows (simulating double-fire events)
  - Mixed date formats
  - Mixed case inconsistencies
  - SA ID numbers (masked) with wrong lengths
  - Negative amounts (data entry errors)
  - Future payment dates
  - Orphaned foreign keys
  - Inconsistent status codes
  - Bot/spider web sessions with no events
"""
import random
import csv
import json
import os
from datetime import datetime, timedelta, date
import hashlib
import string

random.seed(42)
OUT = os.path.dirname(__file__)

# ─── Helpers ─────────────────────────────────────────────────────────────────

def rand_date(start="2025-01-01", end="2026-06-16"):
    s = datetime.strptime(start, "%Y-%m-%d")
    e = datetime.strptime(end, "%Y-%m-%d")
    return s + timedelta(days=random.randint(0, (e - s).days))

def rand_dt(start="2025-01-01", end="2026-06-16"):
    d = rand_date(start, end)
    h = random.randint(7, 22)
    m = random.randint(0, 59)
    s = random.randint(0, 59)
    return d.strftime("%Y-%m-%d") + f" {h:02d}:{m:02d}:{s:02d}"

def dirty_date(dt_str, i):
    """Introduce mixed date format errors ~15% of the time."""
    if random.random() < 0.10 and dt_str:
        try:
            d = datetime.strptime(dt_str[:10], "%Y-%m-%d")
            fmts = ["%d/%m/%Y", "%m-%d-%Y", "%d %b %Y", "%Y%m%d"]
            return d.strftime(random.choice(fmts))
        except Exception:
            pass
    return dt_str

def maybe_null(val, prob=0.05):
    return None if random.random() < prob else val

def dirty_amount(amount, prob=0.03):
    if random.random() < prob:
        return -abs(amount)
    return amount

def mask_id(id_str):
    return hashlib.sha256(id_str.encode()).hexdigest()[:12].upper()

def rand_phone():
    return f"+27 {random.choice(['71','72','73','74','76','78','79','82','83','84','85'])} {random.randint(100,999)} {random.randint(1000,9999)}"

def rand_email(name):
    domains = ["gmail.com", "outlook.com", "yahoo.co.za", "webmail.co.za", "icloud.com"]
    return f"{name.lower().replace(' ', '.')}{random.randint(1,99)}@{random.choice(domains)}"

def duplicate_rows(rows, prob=0.04):
    dupes = [r for r in rows if random.random() < prob]
    return rows + dupes

# ─── Reference Data ──────────────────────────────────────────────────────────

PROVINCES = ["Western Cape", "Gauteng", "KwaZulu-Natal", "Eastern Cape",
             "Limpopo", "Mpumalanga", "Free State", "North West", "Northern Cape"]

FIRST_NAMES = ["Thandi", "Michael", "Ayesha", "Sipho", "Nadine", "Lerato",
               "Trevor", "Zanele", "Pieter", "Fatima", "Bongani", "Liesl",
               "Mandla", "Sarah", "Nkosi", "Amina", "Francois", "Nomsa",
               "Deon", "Palesa", "Gareth", "Nokwanda", "Riaan", "Priya"]

LAST_NAMES = ["Mokoena", "Daniels", "Petersen", "Dlamini", "Jacobs", "Sithole",
              "Van Wyk", "Ngcobo", "Botha", "Hendricks", "Zulu", "De Villiers",
              "Khumalo", "Arendse", "Mthembu", "Singh", "Du Plessis", "Langa",
              "Coetzee", "Mahlangu", "Vermeulen", "Mabunda", "Steyn", "Pillay"]

BANKS = ["Capitec", "FNB", "Absa", "Standard Bank", "Nedbank", "TymeBank", "Discovery Bank"]
DEVICES = ["Mobile", "Desktop", "Tablet", "mobile", "MOBILE", "desktop"]  # dirty case
CHANNELS = ["Google Sponsored Search", "Organic Search", "Facebook",
            "Direct", "WhatsApp", "Email", None, "unknown"]
RISK_BANDS = ["LOW", "MEDIUM", "HIGH", "VERY_HIGH", None, "low", "Low"]  # dirty case

LOAN_STATUSES = ["ACTIVE", "SETTLED", "ARREARS", "WRITTEN_OFF", "CANCELLED",
                 "active", "Active", "settled"]  # dirty case

APP_STATUSES = ["STARTED", "SUBMITTED", "APPROVED", "DECLINED", "WITHDRAWN", "DISBURSED",
                "submitted", "Approved"]  # dirty

# ─── 1. Customers (2 000 rows + ~4% dupes) ──────────────────────────────────

def gen_customers(n=2000):
    rows = []
    for i in range(1, n + 1):
        fn = random.choice(FIRST_NAMES)
        ln = random.choice(LAST_NAMES)
        dob_year = random.randint(1965, 2004)
        dob = date(dob_year, random.randint(1, 12), random.randint(1, 28))
        sa_id_raw = f"demo-sa-id-{i:05d}"
        # ~5% have wrong ID length (dirty)
        id_hash = mask_id(sa_id_raw) if random.random() > 0.05 else mask_id(sa_id_raw)[:8]
        rows.append({
            "customer_id": i,
            "customer_identifier_hash": id_hash,
            "first_name": fn if random.random() > 0.02 else fn.lower(),
            "last_name": ln if random.random() > 0.02 else ln.upper(),
            "date_of_birth": dirty_date(dob.strftime("%Y-%m-%d"), i) if random.random() > 0.08 else None,
            "gender": maybe_null(random.choice(["Male", "Female", "Non-binary", "Prefer not to say", "M", "F"]), 0.06),
            "province": maybe_null(random.choice(PROVINCES), 0.04),
            "email_masked": maybe_null(rand_email(fn)[:3] + "***@" + rand_email(fn).split("@")[1], 0.10),
            "mobile_masked": maybe_null(rand_phone()[:8] + "****", 0.08),
            "bank_name": maybe_null(random.choice(BANKS), 0.12),
            "account_type": maybe_null(random.choice(["Savings", "Cheque", "savings", "CHEQUE"]), 0.12),
            "is_verified": random.choice([1, 0, 1, 1, 1]),
            "created_at": rand_dt("2023-01-01", "2026-06-16"),
            "is_repeat_customer": random.choice([0, 1, 0, 0]),
            "employment_status": maybe_null(random.choice([
                "Employed", "Self-Employed", "Unemployed", "Pensioner",
                "employed", "EMPLOYED", None
            ]), 0.08),
            "net_income_zar": maybe_null(round(random.uniform(5000, 80000), 2), 0.06),
        })
    return duplicate_rows(rows, 0.04)


# ─── 2. Campaigns (60 rows) ──────────────────────────────────────────────────

def gen_campaigns(n=60):
    brand_names = ["FASTA", "JustMoney", "DebtBusters"]
    objectives = ["Loan Application Acquisition", "Brand Awareness",
                  "Financial Education", "Debt Counselling Lead Gen",
                  "Retargeting", "Calculator Engagement"]
    rows = []
    for i in range(1, n + 1):
        s = rand_date("2024-01-01", "2026-05-01")
        e = s + timedelta(days=random.randint(30, 180)) if random.random() > 0.3 else None
        rows.append({
            "campaign_id": i,
            "brand_name": random.choice(brand_names),
            "channel_name": maybe_null(random.choice(CHANNELS[:-2]), 0.05),
            "campaign_name": f"Campaign {i} - {random.choice(objectives)[:20]}",
            "campaign_objective": random.choice(objectives),
            "start_date": dirty_date(s.strftime("%Y-%m-%d"), i),
            "end_date": dirty_date(e.strftime("%Y-%m-%d"), i) if e else None,
            "budget_zar": maybe_null(round(random.uniform(5000, 250000), 2), 0.15),
            "spend_zar": maybe_null(round(random.uniform(2000, 200000), 2), 0.20),
            "impressions": maybe_null(random.randint(1000, 5000000), 0.10),
            "clicks": maybe_null(random.randint(50, 50000), 0.10),
            "created_at": rand_dt("2024-01-01", "2026-06-01"),
        })
    return rows


# ─── 3. Web Sessions (8 000 rows + ~5% dupes) ───────────────────────────────

def gen_web_sessions(n=8000, n_customers=2000, n_campaigns=60):
    referrers = ["google.com", "facebook.com", "bing.com", None, "instagram.com",
                 "justmoney.co.za", "debtbusters.co.za", "email", "direct", ""]
    landing_pages = [
        "https://www.fasta.co.za/",
        "https://www.fasta.co.za/apply",
        "https://justmoney.co.za/loans",
        "https://debtbusters.co.za/apply",
        "https://www.fasta.co.za/calculator",
    ]
    rows = []
    for i in range(1, n + 1):
        cust = maybe_null(random.randint(1, n_customers), 0.25)  # anonymous sessions
        camp = maybe_null(random.randint(1, n_campaigns), 0.30)
        brand = random.choice(["FASTA", "JustMoney", "DebtBusters", "FASTA", "FASTA"])
        rows.append({
            "session_id": i,
            "customer_id": cust,
            "brand_name": brand,
            "campaign_id": camp,
            "session_started_at": rand_dt("2025-01-01", "2026-06-16"),
            "device_type": maybe_null(random.choice(DEVICES), 0.03),
            "landing_page": random.choice(landing_pages),
            "referrer_domain": maybe_null(random.choice(referrers), 0.10),
            "gclid": maybe_null("gclid_" + "".join(random.choices(string.ascii_lowercase + string.digits, k=10)), 0.75),
            "fbclid": maybe_null("fbclid_" + "".join(random.choices(string.ascii_lowercase + string.digits, k=10)), 0.85),
            "session_duration_seconds": maybe_null(random.randint(5, 1800), 0.08),
            "page_views_count": maybe_null(random.randint(1, 25), 0.08),
            "is_bot": 1 if random.random() < 0.07 else 0,  # 7% bot traffic
        })
    return duplicate_rows(rows, 0.05)


# ─── 4. Web Events (25 000 rows + ~6% dupes) ────────────────────────────────

def gen_web_events(n=25000, n_sessions=8000):
    event_names = [
        "page_view", "page_view", "page_view",
        "loan_calculator_changed", "loan_calculator_changed",
        "apply_now_click", "chat_open", "chat_message_sent",
        "facebook_contact_click", "whatsapp_click",
        "document_upload", "application_started",
        "application_step_1_complete", "application_step_2_complete",
        "application_submitted", "offer_viewed", "offer_accepted",
        "offer_declined", "session_timeout", "error_page",
    ]
    pages = [
        "https://www.fasta.co.za/",
        "https://www.fasta.co.za/apply/step1",
        "https://www.fasta.co.za/apply/step2",
        "https://www.fasta.co.za/apply/step3",
        "https://www.fasta.co.za/offer",
        "https://www.fasta.co.za/calculator",
    ]
    rows = []
    for i in range(1, n + 1):
        session_id = random.randint(1, n_sessions)
        event_name = random.choice(event_names)
        value = None
        if "calculator" in event_name or "apply" in event_name or "offer" in event_name:
            value = round(random.choice([500, 1000, 1500, 2000, 2500, 3300, 5000, 8000, 10000, 15000]) * random.uniform(0.9, 1.1), 2)
            value = dirty_amount(value, 0.02)
        rows.append({
            "event_id": i,
            "session_id": session_id,
            "event_at": rand_dt("2025-01-01", "2026-06-16"),
            "event_name": event_name if random.random() > 0.03 else event_name.upper(),
            "page_url": random.choice(pages),
            "event_value_zar": maybe_null(value, 0.20),
            "user_agent_category": maybe_null(random.choice(["Chrome", "Safari", "Firefox", "Edge", "Bot", None]), 0.05),
        })
    return duplicate_rows(rows, 0.06)


# ─── 5. Loan Quotes (4 000 rows) ─────────────────────────────────────────────

def gen_loan_quotes(n=4000, n_customers=2000, n_sessions=8000):
    rows = []
    for i in range(1, n + 1):
        amount = round(random.choice([500, 1000, 1500, 2000, 2500, 3300, 5000, 8000, 10000, 12000, 15000]) * random.uniform(0.95, 1.05), 2)
        term = random.randint(1, 6)
        fee_pct = random.uniform(0.18, 0.28)
        monthly = round((amount * (1 + fee_pct)) / term, 2)
        total_fees = round(amount * fee_pct, 2)
        total_repay = round(amount + total_fees, 2)
        # dirty: ~3% have negative amount
        amount_d = dirty_amount(amount, 0.03)
        rows.append({
            "quote_id": i,
            "customer_id": random.randint(1, n_customers),
            "product_name": random.choice(["FASTA Cash Loan", "FASTA Repeat Customer Loan", None]),
            "session_id": maybe_null(random.randint(1, n_sessions), 0.15),
            "quote_created_at": rand_dt("2025-01-01", "2026-06-16"),
            "amount_requested_zar": amount_d,
            "term_months": term,
            "monthly_instalment_zar": maybe_null(monthly, 0.04),
            "total_fees_interest_zar": maybe_null(total_fees, 0.04),
            "total_repayable_zar": maybe_null(total_repay, 0.04),
            "calculator_source": maybe_null(random.choice(["website", "app", "partner", "agent", None]), 0.10),
        })
    return rows


# ─── 6. Loan Applications (3 200 rows + ~3% dupes) ──────────────────────────

def gen_loan_applications(n=3200, n_quotes=4000, n_customers=2000):
    rows = []
    for i in range(1, n + 1):
        status = random.choice(APP_STATUSES)
        decline_reason = None
        if "DECLIN" in status.upper():
            decline_reason = random.choice([
                "Affordability failed",
                "Credit score below threshold",
                "Adverse credit listing",
                "Income verification failed",
                "Duplicate application",
                "NCA compliance block",
                None,
            ])
        rows.append({
            "application_id": i,
            "quote_id": random.randint(1, n_quotes),
            "customer_id": random.randint(1, n_customers),
            "application_submitted_at": rand_dt("2025-01-01", "2026-06-16"),
            "application_status": status,
            "sa_id_captured_flag": random.choice([1, 0, 1, 1]),
            "mobile_captured_flag": random.choice([1, 0, 1, 1]),
            "online_banking_captured_flag": random.choice([1, 0, 1]),
            "decline_reason": decline_reason,
            "channel_name": maybe_null(random.choice(CHANNELS), 0.08),
            "agent_code": maybe_null("AGT" + str(random.randint(100, 999)), 0.70),
            "processing_minutes": maybe_null(random.randint(1, 180), 0.10),
        })
    return duplicate_rows(rows, 0.03)


# ─── 7. Credit Checks (2 800 rows) ──────────────────────────────────────────

def gen_credit_checks(n=2800, n_apps=3200):
    bureaus = ["Demo Credit Bureau", "Experian", "TransUnion", "Compuscan", None]
    results = ["PASS", "FAIL", "REFER", "ERROR", "pass", "Fail"]
    rows = []
    for i in range(1, n + 1):
        score = maybe_null(random.randint(300, 850), 0.08)
        if score:
            if score >= 650:
                rb = random.choice(["LOW", "low", "Low"])
            elif score >= 580:
                rb = "MEDIUM"
            else:
                rb = random.choice(["HIGH", "VERY_HIGH"])
        else:
            rb = maybe_null(random.choice(RISK_BANDS), 0.10)
        rows.append({
            "credit_check_id": i,
            "application_id": random.randint(1, n_apps),
            "bureau_name": maybe_null(random.choice(bureaus), 0.05),
            "credit_score": score,
            "risk_band": rb,
            "enquiry_result": random.choice(results),
            "checked_at": rand_dt("2025-01-01", "2026-06-16"),
            "adverse_listings_count": maybe_null(random.randint(0, 5), 0.12),
            "total_open_accounts": maybe_null(random.randint(0, 15), 0.10),
            "total_judgements": maybe_null(random.randint(0, 3), 0.15),
        })
    return rows


# ─── 8. Affordability Assessments (2 600 rows) ──────────────────────────────

def gen_affordability(n=2600, n_apps=3200):
    results = ["PASS", "FAIL", "REFER", "pass", "Fail"]
    rows = []
    for i in range(1, n + 1):
        income = round(random.uniform(4500, 90000), 2)
        expenses = round(income * random.uniform(0.4, 0.95), 2)
        debt = round(random.uniform(0, income * 0.5), 2)
        afford = round(income - expenses - debt, 2)
        result = "PASS" if afford > 1000 else ("FAIL" if afford < 0 else "REFER")
        # inject dirty
        income_d = dirty_amount(income, 0.02)
        afford_d = dirty_amount(afford, 0.04)
        rows.append({
            "assessment_id": i,
            "application_id": random.randint(1, n_apps),
            "net_income_zar": maybe_null(income_d, 0.05),
            "monthly_expenses_zar": maybe_null(round(expenses, 2), 0.05),
            "existing_debt_zar": maybe_null(round(debt, 2), 0.08),
            "affordability_amount_zar": maybe_null(afford_d, 0.05),
            "assessment_result": maybe_null(random.choice(results), 0.03),
            "assessed_at": rand_dt("2025-01-01", "2026-06-16"),
            "dsr_ratio": maybe_null(round(debt / income if income > 0 else 0, 4), 0.10),
        })
    return rows


# ─── 9. Loan Contracts (1 800 rows) ─────────────────────────────────────────

def gen_loan_contracts(n=1800, n_apps=3200):
    rows = []
    for i in range(1, n + 1):
        principal = round(random.choice([500, 1000, 1500, 2000, 3300, 5000, 8000, 10000, 12000, 15000]) * random.uniform(0.95, 1.0), 2)
        term = random.randint(1, 6)
        fee_pct = random.uniform(0.18, 0.28)
        total_repay = round(principal * (1 + fee_pct), 2)
        disb_date = rand_date("2025-01-01", "2026-06-16")
        status = random.choice(LOAN_STATUSES)
        rows.append({
            "loan_id": i,
            "application_id": random.randint(1, n_apps),
            "customer_id": random.randint(1, 2000),
            "contract_number": f"FAST-{disb_date.strftime('%Y%m%d')}-{i:05d}",
            "principal_amount_zar": maybe_null(dirty_amount(principal, 0.02), 0.01),
            "term_months": term,
            "disbursed_amount_zar": maybe_null(round(principal * random.uniform(0.95, 1.0), 2), 0.02),
            "total_repayable_zar": maybe_null(total_repay, 0.02),
            "disbursement_date": dirty_date(disb_date.strftime("%Y-%m-%d"), i),
            "loan_status": status,
            "nca_compliant_flag": random.choice([1, 1, 1, 0]),  # some non-compliant
            "interest_rate_pct": maybe_null(round(random.uniform(3.0, 5.0), 4), 0.05),
        })
    return duplicate_rows(rows, 0.02)


# ─── 10. Repayment Schedule (9 000 rows) ─────────────────────────────────────

def gen_repayment_schedule(n_loans=1800):
    sched_statuses = ["PENDING", "PAID", "PART_PAID", "OVERDUE", "paid", "Overdue"]
    rows = []
    sid = 1
    for loan_id in range(1, n_loans + 1):
        term = random.randint(1, 6)
        amount = round(random.uniform(800, 4000), 2)
        base_date = rand_date("2025-02-01", "2026-07-01")
        for inst in range(1, term + 1):
            due = base_date + timedelta(days=30 * inst)
            status = random.choice(sched_statuses)
            rows.append({
                "schedule_id": sid,
                "loan_id": loan_id,
                "instalment_number": inst,
                "due_date": dirty_date(due.strftime("%Y-%m-%d"), sid),
                "instalment_amount_zar": maybe_null(dirty_amount(amount, 0.02), 0.02),
                "principal_component_zar": maybe_null(round(amount * 0.70, 2), 0.04),
                "fee_interest_component_zar": maybe_null(round(amount * 0.30, 2), 0.04),
                "schedule_status": maybe_null(status, 0.02),
            })
            sid += 1
    return rows


# ─── 11. Payments (5 500 rows) ───────────────────────────────────────────────

def gen_payments(n=5500, n_loans=1800, n_schedules=9000):
    methods = ["Debit Order", "Manual EFT", "debit order", "Card", "Cash Deposit"]
    pay_statuses = ["SUCCESS", "FAILED", "REVERSED", "PENDING", "success", "failed"]
    rows = []
    for i in range(1, n + 1):
        pay_date = rand_date("2025-02-01", "2026-08-01")
        # ~5% future payment dates (dirty)
        if random.random() < 0.05:
            pay_date = rand_date("2026-07-01", "2027-01-01")
        amount = round(random.uniform(200, 5000), 2)
        rows.append({
            "payment_id": i,
            "loan_id": random.randint(1, n_loans),
            "schedule_id": maybe_null(random.randint(1, n_schedules), 0.08),
            "payment_date": dirty_date(pay_date.strftime("%Y-%m-%d"), i),
            "payment_amount_zar": dirty_amount(amount, 0.04),
            "payment_method": random.choice(methods),
            "payment_status": random.choice(pay_statuses),
            "bank_reference": maybe_null("BANKREF-" + "".join(random.choices(string.digits, k=8)), 0.10),
            "naedo_run_id": maybe_null("NAEDO-" + str(random.randint(100000, 999999)), 0.60),
        })
    return duplicate_rows(rows, 0.04)


# ─── 12. Fee Charges (3 500 rows) ────────────────────────────────────────────

def gen_fee_charges(n=3500, n_loans=1800):
    fee_types = ["Initiation Fee", "Service Fee", "Late Payment Fee",
                 "Collection Fee", "Dishonour Fee", "initiation fee", "SERVICE FEE"]
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "fee_charge_id": i,
            "loan_id": random.randint(1, n_loans),
            "fee_type": random.choice(fee_types),
            "fee_amount_zar": maybe_null(dirty_amount(round(random.uniform(25, 500), 2), 0.03), 0.02),
            "fee_date": dirty_date(rand_date("2025-01-01", "2026-06-16").strftime("%Y-%m-%d"), i),
            "waived_flag": random.choice([0, 0, 0, 1]),
        })
    return rows


# ─── 13. Collection Actions (1 200 rows) ─────────────────────────────────────

def gen_collection_actions(n=1200, n_loans=1800):
    action_types = ["SMS Reminder", "Email Reminder", "Inbound Call", "Outbound Call",
                    "WhatsApp", "Legal Letter", "Handover to Collections Agency"]
    outcomes = ["Promise to Pay", "Partial Payment Received", "Full Payment Received",
                "No Answer", "Wrong Number", "Disputed", "Handed Over"]
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "collection_action_id": i,
            "loan_id": random.randint(1, n_loans),
            "action_date": dirty_date(rand_date("2025-03-01", "2026-06-16").strftime("%Y-%m-%d"), i),
            "action_type": random.choice(action_types),
            "action_outcome": maybe_null(random.choice(outcomes), 0.08),
            "agent_name": maybe_null(random.choice([
                "System", "Naledi Agent", "Sipho Collections", "Auto-Dialler", None
            ]), 0.05),
            "promise_to_pay_date": maybe_null(rand_date("2026-06-17", "2026-12-31").strftime("%Y-%m-%d"), 0.60),
            "promise_amount_zar": maybe_null(round(random.uniform(200, 4000), 2), 0.60),
        })
    return rows


# ─── 14. Support Tickets (2 000 rows) ────────────────────────────────────────

def gen_support_tickets(n=2000, n_customers=2000):
    topics = ["Application Help", "Payment Query", "Decline Explanation",
              "Fee Dispute", "Statement Request", "Fraud Report",
              "Financial Health Query", "Technical Issue", "Complaint"]
    priorities = ["Low", "Medium", "High", "Critical", "low", "HIGH"]
    statuses = ["OPEN", "PENDING_CUSTOMER", "RESOLVED", "CLOSED", "open", "Closed"]
    channels = ["Chat", "Email", "Phone", "WhatsApp", "Facebook"]
    rows = []
    for i in range(1, n + 1):
        opened = rand_dt("2025-01-01", "2026-06-16")
        closed = None
        if random.random() > 0.25:
            o_dt = datetime.strptime(opened, "%Y-%m-%d %H:%M:%S")
            closed = (o_dt + timedelta(minutes=random.randint(5, 4320))).strftime("%Y-%m-%d %H:%M:%S")
        rows.append({
            "ticket_id": i,
            "customer_id": maybe_null(random.randint(1, n_customers), 0.05),
            "brand_name": random.choice(["FASTA", "JustMoney", "DebtBusters"]),
            "channel_name": random.choice(channels),
            "opened_at": opened,
            "closed_at": closed,
            "topic": random.choice(topics),
            "priority": random.choice(priorities),
            "ticket_status": random.choice(statuses),
            "first_response_minutes": maybe_null(random.randint(1, 240), 0.12),
            "csat_score": maybe_null(round(random.uniform(1, 5), 1), 0.35),
            "nps_score": maybe_null(random.randint(0, 10), 0.40),
        })
    return rows


# ─── 15. Reviews (800 rows) ──────────────────────────────────────────────────

def gen_reviews(n=800):
    platforms = ["Google Reviews", "Facebook Reviews", "Hellopeter", "Trustpilot", "App Store"]
    sentiments = ["Positive", "Neutral", "Negative", None]
    sample_texts = [
        "Fast and easy application process",
        "Fees are too high but service is quick",
        "Applied and got money same day!",
        "Difficult to understand the repayment terms",
        "Great customer support",
        "Would not recommend - high interest rates",
        "Simple and straightforward",
        None,
    ]
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "review_id": i,
            "brand_name": random.choice(["FASTA", "FASTA", "JustMoney", "DebtBusters"]),
            "source_platform": random.choice(platforms),
            "review_date": dirty_date(rand_date("2024-01-01", "2026-06-16").strftime("%Y-%m-%d"), i),
            "rating": maybe_null(round(random.uniform(1.0, 5.0), 1), 0.05),
            "review_text": maybe_null(random.choice(sample_texts), 0.15),
            "reviewer_name_masked": maybe_null(f"{random.choice(FIRST_NAMES)[0]}*** {random.choice(LAST_NAMES)[0]}***", 0.20),
            "sentiment_label": maybe_null(random.choice(sentiments), 0.15),
        })
    return rows


# ─── 16. Partner Referrals (600 rows) ────────────────────────────────────────

def gen_referrals(n=600, n_customers=2000):
    reasons = [
        "Loan affordability failed; debt help required",
        "Customer requested financial education",
        "Existing client seeking debt consolidation",
        "Application declined; referred for counselling",
        "Income too low for loan; referred to savings product",
    ]
    ref_statuses = ["SENT", "ACCEPTED", "DECLINED", "CONTACTED", "CONVERTED", "sent", "Accepted"]
    rows = []
    for i in range(1, n + 1):
        src = random.choice(["FASTA", "JustMoney"])
        dst = "DebtBusters" if src == "FASTA" else random.choice(["FASTA", "DebtBusters"])
        rows.append({
            "referral_id": i,
            "customer_id": random.randint(1, n_customers),
            "source_brand": src,
            "destination_brand": dst,
            "referral_reason": random.choice(reasons),
            "referred_at": rand_dt("2025-01-01", "2026-06-16"),
            "referral_status": random.choice(ref_statuses),
            "commission_zar": maybe_null(round(random.uniform(50, 500), 2), 0.30),
        })
    return rows


# ─── Writer ──────────────────────────────────────────────────────────────────

def write_csv(filename, rows):
    if not rows:
        return
    path = os.path.join(OUT, filename)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Wrote {len(rows):>6,} rows -> {filename}")


def write_manifest(tables_meta):
    manifest = {
        "generated_at": datetime.now().isoformat(),
        "layer": "bronze",
        "description": "Raw dirty data simulating FASTA/Confluent FinTech lending platform",
        "tables": tables_meta,
        "dirty_data_patterns": [
            "Mixed date formats (DD/MM/YYYY, MM-DD-YYYY, YYYYMMDD)",
            "Mixed case inconsistencies (status codes, categories)",
            "Null values in non-nullable business fields",
            "Negative monetary amounts (data entry errors)",
            "Duplicate rows (~2-6% per table)",
            "Future payment dates (~5%)",
            "Bot/spider web sessions (7%)",
            "Orphaned foreign keys",
            "Masked SA ID hashes with wrong length (~5%)",
            "Inconsistent status codes",
        ],
    }
    path = os.path.join(OUT, "_bronze_manifest.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, default=str)
    print(f"  Wrote manifest -> _bronze_manifest.json")


# ─── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("Generating FASTA FinTech Bronze Layer data...\n")
    tables = {}

    customers = gen_customers(2000)
    write_csv("customers.csv", customers)
    tables["customers"] = len(customers)

    campaigns = gen_campaigns(60)
    write_csv("campaigns.csv", campaigns)
    tables["campaigns"] = len(campaigns)

    sessions = gen_web_sessions(8000)
    write_csv("web_sessions.csv", sessions)
    tables["web_sessions"] = len(sessions)

    events = gen_web_events(25000)
    write_csv("web_events.csv", events)
    tables["web_events"] = len(events)

    quotes = gen_loan_quotes(4000)
    write_csv("loan_quotes.csv", quotes)
    tables["loan_quotes"] = len(quotes)

    apps = gen_loan_applications(3200)
    write_csv("loan_applications.csv", apps)
    tables["loan_applications"] = len(apps)

    checks = gen_credit_checks(2800)
    write_csv("credit_checks.csv", checks)
    tables["credit_checks"] = len(checks)

    afford = gen_affordability(2600)
    write_csv("affordability_assessments.csv", afford)
    tables["affordability_assessments"] = len(afford)

    contracts = gen_loan_contracts(1800)
    write_csv("loan_contracts.csv", contracts)
    tables["loan_contracts"] = len(contracts)

    schedules = gen_repayment_schedule(1800)
    write_csv("repayment_schedule.csv", schedules)
    tables["repayment_schedule"] = len(schedules)

    payments = gen_payments(5500)
    write_csv("payments.csv", payments)
    tables["payments"] = len(payments)

    fees = gen_fee_charges(3500)
    write_csv("fee_charges.csv", fees)
    tables["fee_charges"] = len(fees)

    collections = gen_collection_actions(1200)
    write_csv("collection_actions.csv", collections)
    tables["collection_actions"] = len(collections)

    tickets = gen_support_tickets(2000)
    write_csv("support_tickets.csv", tickets)
    tables["support_tickets"] = len(tickets)

    reviews = gen_reviews(800)
    write_csv("reviews.csv", reviews)
    tables["reviews"] = len(reviews)

    referrals = gen_referrals(600)
    write_csv("partner_referrals.csv", referrals)
    tables["partner_referrals"] = len(referrals)

    write_manifest(tables)

    total = sum(tables.values())
    print(f"\nBronze layer complete: {len(tables)} tables, {total:,} total rows")
