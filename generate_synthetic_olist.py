"""
Generate synthetic Olist-shaped CSVs into raw_data/.

STAND-IN for the Kaggle download (Phase 1 of IMPLEMENTATION.md). Produces the
exact 9 filenames and column names that init_oltp_db.py and the dbt models
expect, with referential integrity intact so all 10 dbt tests pass.

To switch to the real dataset later: download the Kaggle zip, extract over
raw_data/, and re-run init_oltp_db.py. No pipeline code changes needed.
"""
import hashlib
import os

import numpy as np
import pandas as pd

OUT = "./raw_data"
SEED = 42

# Real Olist row counts, except geolocation (see note below)
N_CUSTOMERS = 99441
N_ORDERS = 99441
N_ITEMS = 112650
N_PRODUCTS = 32951
N_SELLERS = 3095
N_PAYMENTS = 103886
N_REVIEWS = 99224
# Real Olist geolocation is ~1,000,163 rows. Reduced here because no mart
# consumes it (IMPLEMENTATION.md Phase 10 item 6) and 1M rows makes every
# pandas to_sql round-trip in the DAG slow.
N_GEO = 200000

rng = np.random.default_rng(SEED)

STATES = np.array(["SP","RJ","MG","RS","PR","SC","BA","DF","GO","ES","PE","CE","PA","MT","MA","MS","PB","RN","PI","AL","SE","RO","TO","AC","AM","AP","RR"])
STATE_W = np.array([.42,.13,.12,.055,.05,.036,.034,.021,.02,.02,.017,.013,.01,.009,.007,.007,.006,.005,.005,.004,.004,.003,.003,.002,.002,.001,.001])
STATE_W = STATE_W / STATE_W.sum()
CITIES = np.array(["sao paulo","rio de janeiro","belo horizonte","brasilia","curitiba","campinas","porto alegre","salvador","guarulhos","sao bernardo do campo","niteroi","santo andre","osasco","fortaleza","recife","sorocaba","ribeirao preto","florianopolis","goiania","santos"])

CATEGORIES = ["cama_mesa_banho","beleza_saude","esporte_lazer","moveis_decoracao","informatica_acessorios","utilidades_domesticas","relogios_presentes","telefonia","ferramentas_jardim","automotivo","brinquedos","cool_stuff","perfumaria","bebes","eletronicos","papelaria","fashion_bolsas_e_acessorios","pet_shop","moveis_escritorio","consoles_games"]
CATEGORY_EN = ["bed_bath_table","health_beauty","sports_leisure","furniture_decor","computers_accessories","housewares","watches_gifts","telephony","garden_tools","auto","toys","cool_stuff","perfumery","baby","electronics","stationery","fashion_bags_accessories","pet_shop","office_furniture","consoles_games"]

STATUSES = np.array(["delivered","shipped","canceled","invoiced","processing","unavailable","approved","created"])
STATUS_W = np.array([.9700,.0110,.0063,.0031,.0031,.0006,.0002,.0057])
STATUS_W = STATUS_W / STATUS_W.sum()

PAYMENT_TYPES = np.array(["credit_card","boleto","voucher","debit_card"])
PAYMENT_W = np.array([.7395,.1904,.0552,.0149]); PAYMENT_W = PAYMENT_W / PAYMENT_W.sum()


def ids(prefix, n):
    """Unique 32-char hex ids, like the real dataset."""
    return np.array([hashlib.md5(f"{prefix}{i}".encode()).hexdigest() for i in range(n)])


def fmt(ts):
    """Datetime64 array -> Olist's 'YYYY-MM-DD HH:MM:SS' strings, NaT -> ''."""
    s = pd.Series(ts).dt.strftime("%Y-%m-%d %H:%M:%S")
    return s.fillna("")


os.makedirs(OUT, exist_ok=True)
print(f"Generating synthetic Olist dataset into {OUT}/ (seed={SEED})\n")

# ---------------------------------------------------------------- sellers
seller_ids = ids("seller", N_SELLERS)
seller_zip = rng.integers(1000, 99999, N_SELLERS)
pd.DataFrame({
    "seller_id": seller_ids,
    "seller_zip_code_prefix": seller_zip,
    "seller_city": rng.choice(CITIES, N_SELLERS),
    "seller_state": rng.choice(STATES, N_SELLERS, p=STATE_W),
}).to_csv(f"{OUT}/olist_sellers_dataset.csv", index=False)
print(f"  olist_sellers_dataset.csv            {N_SELLERS:>9,}")

# ---------------------------------------------------------------- customers
customer_ids = ids("cust", N_CUSTOMERS)
pd.DataFrame({
    "customer_id": customer_ids,
    "customer_unique_id": ids("uniq", N_CUSTOMERS),
    "customer_zip_code_prefix": rng.integers(1000, 99999, N_CUSTOMERS),
    "customer_city": rng.choice(CITIES, N_CUSTOMERS),
    "customer_state": rng.choice(STATES, N_CUSTOMERS, p=STATE_W),
}).to_csv(f"{OUT}/olist_customers_dataset.csv", index=False)
print(f"  olist_customers_dataset.csv          {N_CUSTOMERS:>9,}")

# ---------------------------------------------------------------- products
product_ids = ids("prod", N_PRODUCTS)
cat_idx = rng.integers(0, len(CATEGORIES), N_PRODUCTS)
pd.DataFrame({
    "product_id": product_ids,
    "product_category_name": np.array(CATEGORIES)[cat_idx],
    "product_name_lenght": rng.integers(20, 70, N_PRODUCTS),
    "product_description_lenght": rng.integers(100, 3000, N_PRODUCTS),
    "product_photos_qty": rng.integers(1, 8, N_PRODUCTS),
    "product_weight_g": rng.integers(50, 30000, N_PRODUCTS),
    "product_length_cm": rng.integers(7, 105, N_PRODUCTS),
    "product_height_cm": rng.integers(2, 105, N_PRODUCTS),
    "product_width_cm": rng.integers(6, 105, N_PRODUCTS),
}).to_csv(f"{OUT}/olist_products_dataset.csv", index=False)
print(f"  olist_products_dataset.csv           {N_PRODUCTS:>9,}")

# ------------------------------------------------- category translation
pd.DataFrame({
    "product_category_name": CATEGORIES,
    "product_category_name_english": CATEGORY_EN,
}).to_csv(f"{OUT}/product_category_name_translation.csv", index=False)
print(f"  product_category_name_translation.csv{len(CATEGORIES):>9,}")

# ---------------------------------------------------------------- geolocation
pd.DataFrame({
    "geolocation_zip_code_prefix": rng.integers(1000, 99999, N_GEO),
    "geolocation_lat": np.round(rng.uniform(-33.7, 5.2, N_GEO), 6),
    "geolocation_lng": np.round(rng.uniform(-73.9, -34.8, N_GEO), 6),
    "geolocation_city": rng.choice(CITIES, N_GEO),
    "geolocation_state": rng.choice(STATES, N_GEO, p=STATE_W),
}).to_csv(f"{OUT}/olist_geolocation_dataset.csv", index=False)
print(f"  olist_geolocation_dataset.csv        {N_GEO:>9,}   (real: ~1,000,163)")

# ---------------------------------------------------------------- orders
order_ids = ids("order", N_ORDERS)
status = rng.choice(STATUSES, N_ORDERS, p=STATUS_W)

# Real Olist spans 2016-09-04 .. 2018-10-17
start = np.datetime64("2016-09-04")
span_days = (np.datetime64("2018-10-17") - start).astype(int)
purchase = start + rng.integers(0, span_days, N_ORDERS).astype("timedelta64[D]") \
                 + rng.integers(0, 86400, N_ORDERS).astype("timedelta64[s]")

approved = purchase + rng.integers(60, 172800, N_ORDERS).astype("timedelta64[s]")
carrier  = approved + rng.integers(3600, 604800, N_ORDERS).astype("timedelta64[s]")
delivered = carrier + rng.integers(86400, 2592000, N_ORDERS).astype("timedelta64[s]")
estimated = purchase + rng.integers(7, 45, N_ORDERS).astype("timedelta64[D]")

# Lifecycle NULLs by status - this is what makes the COALESCE in stg_orders matter
approved  = np.where(np.isin(status, ["created","unavailable"]), np.datetime64("NaT"), approved)
carrier   = np.where(np.isin(status, ["delivered","shipped"]),   carrier,   np.datetime64("NaT"))
delivered = np.where(status == "delivered",                       delivered, np.datetime64("NaT"))

pd.DataFrame({
    "order_id": order_ids,
    "customer_id": customer_ids,          # 1:1, as in the real dataset
    "order_status": status,
    "order_purchase_timestamp": fmt(purchase),
    "order_approved_at": fmt(approved),
    "order_delivered_carrier_date": fmt(carrier),
    "order_delivered_customer_date": fmt(delivered),
    "order_estimated_delivery_date": fmt(estimated),
}).to_csv(f"{OUT}/olist_orders_dataset.csv", index=False)
print(f"  olist_orders_dataset.csv             {N_ORDERS:>9,}")

# ---------------------------------------------------------------- order_items
# 112,650 items across 99,441 orders -> most orders have 1, some have more.
extra = N_ITEMS - N_ORDERS
multi = rng.choice(N_ORDERS, size=extra, replace=True)
item_order_idx = np.concatenate([np.arange(N_ORDERS), multi])
item_order_idx.sort()
# order_item_id is 1..n within each order
seq = np.ones(len(item_order_idx), dtype=int)
same = item_order_idx[1:] == item_order_idx[:-1]
run = 1
for i in range(1, len(seq)):
    run = run + 1 if same[i - 1] else 1
    seq[i] = run

item_purchase = purchase[item_order_idx]
pd.DataFrame({
    "order_id": order_ids[item_order_idx],
    "order_item_id": seq,
    "product_id": product_ids[rng.integers(0, N_PRODUCTS, len(seq))],
    "seller_id": seller_ids[rng.integers(0, N_SELLERS, len(seq))],
    "shipping_limit_date": fmt(item_purchase + rng.integers(1, 10, len(seq)).astype("timedelta64[D]")),
    "price": np.round(rng.gamma(2.2, 55.0, len(seq)) + 3.0, 2),
    "freight_value": np.round(rng.gamma(2.0, 8.0, len(seq)) + 1.0, 2),
}).to_csv(f"{OUT}/olist_order_items_dataset.csv", index=False)
print(f"  olist_order_items_dataset.csv        {N_ITEMS:>9,}")

# ---------------------------------------------------------------- payments
pay_order_idx = np.concatenate([
    np.arange(N_ORDERS),
    rng.choice(N_ORDERS, size=N_PAYMENTS - N_ORDERS, replace=True),
])
pay_order_idx.sort()
pseq = np.ones(len(pay_order_idx), dtype=int)
psame = pay_order_idx[1:] == pay_order_idx[:-1]
run = 1
for i in range(1, len(pseq)):
    run = run + 1 if psame[i - 1] else 1
    pseq[i] = run

pd.DataFrame({
    "order_id": order_ids[pay_order_idx],
    "payment_sequential": pseq,
    "payment_type": rng.choice(PAYMENT_TYPES, len(pseq), p=PAYMENT_W),
    "payment_installments": rng.integers(1, 11, len(pseq)),
    "payment_value": np.round(rng.gamma(2.3, 70.0, len(pseq)) + 5.0, 2),
}).to_csv(f"{OUT}/olist_order_payments_dataset.csv", index=False)
print(f"  olist_order_payments_dataset.csv     {N_PAYMENTS:>9,}")

# ---------------------------------------------------------------- reviews
rev_order_idx = rng.choice(N_ORDERS, size=N_REVIEWS, replace=False)
rev_purchase = purchase[rev_order_idx]
rev_created = rev_purchase + rng.integers(1, 30, N_REVIEWS).astype("timedelta64[D]")
pd.DataFrame({
    "review_id": ids("review", N_REVIEWS),
    "order_id": order_ids[rev_order_idx],
    "review_score": rng.choice([1, 2, 3, 4, 5], N_REVIEWS, p=[.115, .032, .082, .193, .578]),
    "review_comment_title": "",
    "review_comment_message": "",
    "review_creation_date": fmt(rev_created),
    "review_answer_timestamp": fmt(rev_created + rng.integers(3600, 604800, N_REVIEWS).astype("timedelta64[s]")),
}).to_csv(f"{OUT}/olist_order_reviews_dataset.csv", index=False)
print(f"  olist_order_reviews_dataset.csv      {N_REVIEWS:>9,}")

print("\nDone.")
