import os
import time
from pathlib import Path

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from rapidfuzz import fuzz, process
from sqlalchemy import create_engine, text

# ─── Page config & theme ──────────────────────────────────────────────────────
st.set_page_config(
    page_title="Tiger X Market Intelligence",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Branding ───────────────────────────────────────────────────────────────────
logo = Path(__file__).parent / "logo.png"
col1, col2 = st.columns([1, 5])
with col1:
    st.image(str(logo), width=120)  # larger logo
with col2:
    st.markdown(
        """
        <div style="display:flex; flex-direction:column; justify-content:center; height:100%;">
          <h1 style="margin:0;font-family:'Segoe UI',sans-serif;color:#222;">Tiger X Market Intelligence</h1>
          <span style="font-size:0.9rem;color:#555;">By Marketing Team</span>
        </div>
        """,
        unsafe_allow_html=True,
    )
st.markdown("---")

# ─── Load credentials & engine ─────────────────────────────────────────────────
load_dotenv("credentials.env")
USER = os.getenv("DB_USER")
PWD  = os.getenv("DB_PASS")
HOST = os.getenv("DB_HOST")
DB   = os.getenv("DB_NAME", "VOLZA")
URI  = f"mysql+pymysql://{USER}:{PWD}@{HOST}:3306/{DB}"
engine = create_engine(URI, pool_pre_ping=True)

# ─── Helpers ────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def get_distinct(col: str):
    sql = text(f"SELECT DISTINCT `{col}` FROM volza_main WHERE `{col}` IS NOT NULL;")
    with engine.connect() as conn:
        return [r[0] for r in conn.execute(sql).fetchall()]

def fuzzy_filter(choices, query, limit=200, cutoff=70):
    return {
        match
        for match, score, _ in process.extract(query, choices, scorer=fuzz.WRatio, limit=limit)
        if score >= cutoff
    }

# ─── Sidebar ────────────────────────────────────────────────────────────────────
st.sidebar.header("📋 Filter Criteria")

mode = st.sidebar.radio("Mode", ["India Export", "India Import"])

if mode == "India Export":
    dests = get_distinct("country_of_destination")
    sel_dest = st.sidebar.multiselect("Country of Destination", dests, default=dests[:3])
else:
    origs = get_distinct("country_of_origin")
    sel_orig = st.sidebar.multiselect("Country of Origin", origs, default=origs[:3])

hs_q     = st.sidebar.text_input("HS Code prefix")
ship_q   = st.sidebar.text_input("Shipper")
cons_q   = st.sidebar.text_input("Consignee")
notify_q = st.sidebar.text_input("Notify Party")

if st.sidebar.button("🔍 Apply Filters"):
    clauses, params = [], {}

    if mode == "India Export" and sel_dest:
        clauses.append("country_of_destination IN :dest")
        params["dest"] = tuple(sel_dest)
    if mode == "India Import" and sel_orig:
        clauses.append("country_of_origin IN :orig")
        params["orig"] = tuple(sel_orig)

    if hs_q:
        clauses.append("hs_code LIKE :hs")
        params["hs"] = f"{hs_q}%"

    if ship_q:
        ships = get_distinct("shipper_name")
        good = fuzzy_filter(ships, ship_q)
        clauses.append("shipper_name IN :ship")
        params["ship"] = tuple(good)

    if cons_q:
        cons = get_distinct("consignee_name")
        good = fuzzy_filter(cons, cons_q)
        clauses.append("consignee_name IN :cons")
        params["cons"] = tuple(good)

    if notify_q:
        nots = get_distinct("notify_party")
        good = fuzzy_filter(nots, notify_q)
        clauses.append("notify_party IN :notf")
        params["notf"] = tuple(good)

    where = " AND ".join(clauses) if clauses else "1=1"
    base_sql = f"FROM volza_main WHERE {where}"

    # ── KPIs ──────────────────────────────────────────────────────────────────────
    kpi_sql = text(f"""
        SELECT
          COUNT(*)                AS total_shipments,
          COUNT(DISTINCT shipper_name)   AS shippers,
          COUNT(DISTINCT consignee_name) AS consignees,
          COUNT(DISTINCT notify_party)   AS notify_parties
        {base_sql};
    """)
    with engine.connect() as conn:
        k = conn.execute(kpi_sql, params).mappings().one()

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total Shipments", f"{k['total_shipments']:,}")
    k2.metric("Shippers",       f"{k['shippers']:,}")
    k3.metric("Consignees",     f"{k['consignees']:,}")
    k4.metric("Notify Parties", f"{k['notify_parties']:,}")

    # ── Detailed Results (moved above charts) ──────────────────────────────────────
    df = pd.read_sql_query(text(f"SELECT * {base_sql};"), engine, params=params)
    st.markdown("### 📋 Detailed Results")
    st.dataframe(df, use_container_width=True)
    st.download_button("⬇️ Download CSV", df.to_csv(index=False), "results.csv", "text/csv")

    # ── Top 5 Charts (smaller, side by side) ───────────────────────────────────────
    qc = {"params": params}
    top_comm = pd.read_sql_query(
        text(f"SELECT product_description, COUNT(*) AS cnt {base_sql} GROUP BY product_description ORDER BY cnt DESC LIMIT 5;"),
        engine, **qc
    )
    top_ship = pd.read_sql_query(
        text(f"SELECT shipper_name, COUNT(*) AS cnt {base_sql} GROUP BY shipper_name ORDER BY cnt DESC LIMIT 5;"),
        engine, **qc
    )

    colA, colB = st.columns(2)
    with colA:
        st.markdown("#### Top 5 Commodities")
        st.bar_chart(top_comm.set_index("product_description")["cnt"], height=250)
    with colB:
        st.markdown("#### Top 5 Shippers")
        st.bar_chart(top_ship.set_index("shipper_name")["cnt"], height=250)

else:
    st.info("Select your filters and click **Apply Filters** on the left.")
