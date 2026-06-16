"""
One-off seeder: add realistic T24 records to t24_adaptor.CUSTOMER (997 rows)
and t24_adaptor.ACCOUNT (994 rows). Touches ONLY those two tables.

Field semantics are taken from t24_adaptor.STANDARD_SELECTION:
  CUSTOMER: c1=MNEMONIC c2=SHORT.NAME c3=NAME.1 c5=STREET c7=TOWN c9=COUNTRY
            c11=SECTOR c15=NATIONALITY c16=RESIDENCE c22=EMAIL.1 c27=MARITAL
            c28=GENDER c29=DATE.OF.BIRTH c30=LEGAL.ID
            c64.1=CUST.SEGMENT c64.2=CUST.TIER c64.3=TAX.ID(sub) c64.4=KYC.REVIEW.DATE
  ACCOUNT:  c1=CUSTOMER c2=ACCOUNT.NO c3=CATEGORY c4=ACCOUNT.TITLE.1 c5=CURRENCY
            c6=OPENING.DATE c7=WORKING.BALANCE c8=ONLINE.ACT.BAL c12=ACCOUNT.OFFICER(multi)
            c64.1=RELATIONSHIP.CODE c64.2=RISK.RATING c64.3=INTEREST.RATE(sub)
"""
import os, random
from xml.sax.saxutils import escape

random.seed(20260615)  # deterministic; no Math.random surprises

XML_HEAD = '<?xml version="1.0" encoding="UTF-8"?>'

# ---- realistic value pools -------------------------------------------------
# (country, currency, town pool, nationality==residence)
COUNTRIES = [
    ("US", "USD", ["New York", "Chicago", "Boston", "Seattle", "Austin", "Denver"]),
    ("EG", "EGP", ["Cairo", "Giza", "Alexandria", "Mansoura", "Tanta", "Aswan"]),
    ("GB", "GBP", ["London", "Manchester", "Leeds", "Bristol", "Glasgow"]),
    ("AE", "AED", ["Dubai", "Abu Dhabi", "Sharjah", "Ajman"]),
    ("SA", "SAR", ["Riyadh", "Jeddah", "Dammam", "Mecca"]),
    ("DE", "EUR", ["Berlin", "Munich", "Hamburg", "Cologne"]),
    ("FR", "EUR", ["Paris", "Lyon", "Marseille", "Nice"]),
]

FIRST_M = ["John", "Ahmed", "Mohamed", "Omar", "Khaled", "James", "David", "Karim",
           "Tarek", "Hassan", "Youssef", "Adam", "Daniel", "Michael", "Samir", "Amir"]
FIRST_F = ["Sara", "Mona", "Laila", "Nour", "Emma", "Olivia", "Hana", "Aya", "Salma",
           "Dina", "Rana", "Sophia", "Maya", "Yara", "Heba", "Reem"]
LAST = ["Doe", "Salah", "Hassan", "Mahmoud", "Ali", "Ibrahim", "Smith", "Brown",
        "Khalil", "Said", "Mansour", "Farouk", "Nasser", "Wagner", "Dupont",
        "Johnson", "Williams", "Taylor", "Aziz", "Fahmy", "Shaker", "Gamal"]

STREETS = ["Wall Street", "Tahrir St", "Nile Ave", "King Fahd Rd", "Sheikh Zayed Rd",
           "Baker Street", "Main Street", "Corniche Rd", "Abbas El-Akkad", "Park Lane",
           "Maple Avenue", "Rue de Rivoli", "Unter den Linden", "Olaya St"]

SEGMENTS = ["SEGMENT-A", "SEGMENT-B", "SEGMENT-C", "SEGMENT-D"]
TIERS = ["VIP-GOLD", "STANDARD", "PREMIER", "PLATINUM", "SILVER"]
MARITAL = ["S", "M", "D", "W"]
SECTORS = ["1000", "1001", "2000", "3000", "4100", "5000"]

# account-specific pools
CATEGORIES = ["1001", "1002", "1003", "6001", "6002", "6003"]  # 1xxx savings, 6xxx current
CAT_LABEL = {"1001": "Savings A/C", "1002": "Savings A/C", "1003": "Savings A/C",
             "6001": "Current A/C", "6002": "Current A/C", "6003": "Current A/C"}
REL_CODES = ["PREMIER", "STANDARD", "GOLD", "PLATINUM"]
RISK = ["LOW", "MEDIUM", "HIGH"]


def esc(v):
    return escape(str(v))


def rand_date(y0, y1):
    y = random.randint(y0, y1)
    m = random.randint(1, 12)
    d = random.randint(1, 28)
    return f"{y:04d}{m:02d}{d:02d}"


def mnemonic(first, last, used):
    base = (first[0] + last[0]).upper()
    for _ in range(50):
        cand = f"{base}{random.randint(1000, 9999)}"
        if cand not in used:
            used.add(cand)
            return cand
    # fallback guaranteed-unique
    n = 10000
    while f"{base}{n}" in used:
        n += 1
    used.add(f"{base}{n}")
    return f"{base}{n}"


def build_customer(used_mn):
    cc, ccy, towns = random.choice(COUNTRIES)
    gender = random.choice(["MALE", "FEMALE"])
    first = random.choice(FIRST_M if gender == "MALE" else FIRST_F)
    last = random.choice(LAST)
    full = f"{first} {last}"
    mn = mnemonic(first, last, used_mn)
    town = random.choice(towns)
    dob = rand_date(1955, 2003)
    parts = [
        f"<c1>{esc(mn)}</c1>",
        f'<c2 m="1">{esc(full)}</c2>',
        f'<c3 m="1">{esc(first)} {esc(last)}</c3>',
        f'<c5 m="1">{esc(str(random.randint(1, 250)))} {esc(random.choice(STREETS))}</c5>',
        f'<c7 m="1">{esc(town)}</c7>',
        f'<c9 m="1">{esc(cc)}</c9>',
        f"<c11>{esc(random.choice(SECTORS))}</c11>",
        f"<c15>{esc(cc)}</c15>",
        f"<c16>{esc(cc)}</c16>",
        f'<c22 m="1">{esc(first.lower())}.{esc(last.lower())}@example.com</c22>',
        f"<c27>{esc(random.choice(MARITAL))}</c27>",
        f"<c28>{esc(gender)}</c28>",
        f"<c29>{esc(dob)}</c29>",
    ]
    # legal id only for ~70% (mirrors sparse real data)
    if random.random() < 0.7:
        parts.append(f'<c30 m="1">ID-{random.randint(10**8, 10**9 - 1)}</c30>')
    parts.append(f'<c64 m="1">{esc(random.choice(SEGMENTS))}</c64>')
    parts.append(f'<c64 m="2">{esc(random.choice(TIERS))}</c64>')
    if random.random() < 0.6:
        parts.append(f'<c64 m="3" s="1">TX-{random.randint(100000, 999999)}</c64>')
        if random.random() < 0.4:
            parts.append(f'<c64 m="3" s="2">TX-{random.randint(100000, 999999)}</c64>')
    if random.random() < 0.5:
        parts.append(f'<c64 m="4">{rand_date(2026, 2028)}</c64>')
    xml = f'{XML_HEAD}<DATA><row>{"".join(parts)}</row></DATA>'
    return mn, cc, ccy, full, xml


def build_account(seq, customers):
    """customers: list of (mnemonic, country, currency, fullname)"""
    mn, cc, ccy, full = random.choice(customers)
    cat = random.choice(CATEGORIES)
    acct_no = f"ACC{seq:05d}"
    bal = round(random.uniform(50, 250000), 2)
    online = round(bal - random.uniform(0, min(bal, 2000)), 2)
    title_first = full.split()[0]
    parts = [
        f"<c1>{esc(mn)}</c1>",
        f"<c2>{esc(acct_no)}</c2>",
        f"<c3>{esc(cat)}</c3>",
        f'<c4 m="1">{esc(full)} - {esc(CAT_LABEL[cat])}</c4>',
        f"<c5>{esc(ccy)}</c5>",
        f"<c6>{rand_date(2015, 2025)}</c6>",
        f"<c7>{bal:.2f}</c7>",
        f"<c8>{online:.2f}</c8>",
    ]
    # account officers: 1..3 multi-valued
    for i in range(1, random.randint(1, 3) + 1):
        parts.append(f'<c12 m="{i}">OFF-{random.randint(10, 40)}</c12>')
    parts.append(f'<c64 m="1">{esc(random.choice(REL_CODES))}</c64>')
    parts.append(f'<c64 m="2">{esc(random.choice(RISK))}</c64>')
    # interest rate, 1..3 sub-values
    base_rate = round(random.uniform(0.5, 6.0), 2)
    for s in range(1, random.randint(1, 3) + 1):
        parts.append(f'<c64 m="3" s="{s}">{base_rate + (s - 1) * 0.25:.2f}</c64>')
    xml = f'{XML_HEAD}<DATA><row>{"".join(parts)}</row></DATA>'
    return acct_no, xml


def generate():
    used_mn = {"JD1009", "SS2010", "AH4020"}  # don't collide with existing
    customers = []           # for account FK realism
    cust_rows = []           # (recordId, xml)
    # existing customers also valid FK targets
    existing = [("JD1009", "US", "USD", "John Doe"),
                ("SS2010", "EG", "EGP", "Sara Salah"),
                ("AH4020", "EG", "EGP", "Ahmed Hassan")]

    for i in range(997):
        rec_id = str(4 + i)  # existing CUSTOMER max recordId = 3
        mn, cc, ccy, full, xml = build_customer(used_mn)
        customers.append((mn, cc, ccy, full))
        cust_rows.append((rec_id, xml))

    fk_pool = customers + existing

    acc_rows = []
    for i in range(994):
        rec_id = str(7 + i)        # existing ACCOUNT max recordId = 6
        acct_no, xml = build_account(6 + i, fk_pool)  # ACC00006..
        acc_rows.append((rec_id, xml))

    return cust_rows, acc_rows


if __name__ == "__main__":
    cust_rows, acc_rows = generate()
    print(f"Generated {len(cust_rows)} CUSTOMER rows, {len(acc_rows)} ACCOUNT rows\n")
    print("--- sample CUSTOMER ---")
    for r in cust_rows[:3]:
        print(r[0], r[1])
    print("\n--- sample ACCOUNT ---")
    for r in acc_rows[:3]:
        print(r[0], r[1])
