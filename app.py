import os
import time
from pathlib import Path
import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from rapidfuzz import fuzz, process
from sqlalchemy import create_engine, text
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

import streamlit_authenticator as stauth

# Hashed credentials dictionary you generated before
credentials = {
    'usernames': {
        'admin': {
            'name': 'Saurav',
            'password': '$2b$12$twr2mJ1UltKDz0nZ5Kg0ru5lPk/uA1rFloe8xVie8Mo5zzyzZQO.i'  
        }
    }
}

authenticator = stauth.Authenticate(
    credentials,
    "tigerx_dashboard",  # cookie name
    "tiger",             # signature key
    cookie_expiry_days=1
)

name, auth_status, username = authenticator.login(
    form_name='Login',
    location='sidebar'
)


if auth_status is False:
    st.error("Username/password is incorrect")
    st.stop()
elif auth_status is None:
    st.warning("Please enter your username and password")
    st.stop()

# After login
authenticator.logout("Logout", location='sidebar')
st.sidebar.success(f"Welcome {name}!")




# ─── Page config & theme ──────────────────────────────────────────────────────
st.set_page_config(
    page_title="Tiger X Market Intelligence",
    layout="wide",
    initial_sidebar_state="expanded",
    page_icon="🐯"
)

# ─── Custom CSS for enhanced UI ─────────────────────────────────────────────────
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(90deg, #FFD700 0%, #FFA500 100%);
        padding: 1rem;
        border-radius: 10px;
        margin-bottom: 1rem;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    .metric-card {
        background: white;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #FFD700;
        margin: 0.5rem 0;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
    }
    .loading-container {
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        padding: 2rem;
    }
    .loading-ship {
        font-size: 3rem;
        animation: float 2s ease-in-out infinite;
    }
    @keyframes float {
        0%, 100% { transform: translateY(0px); }
        50% { transform: translateY(-10px); }
    }
    .stButton > button {
        background: linear-gradient(90deg, #FFD700 0%, #FFA500 100%);
        border: none;
        border-radius: 20px;
        padding: 0.5rem 2rem;
        font-weight: bold;
        color: black;
    }
    .sidebar .stSelectbox > div > div {
        background-color: #f8f9fa;
    }
</style>
""", unsafe_allow_html=True)

# ─── Branding with enhanced design ─────────────────────────────────────────────
def render_header():
    logo = Path(__file__).parent / "logo.png"
    col1, col2, col3 = st.columns([1, 6, 1])
    
    with col1:
        if logo.exists():
            st.image(str(logo), width=120)
        else:
            st.markdown("### 🐯")
    
    with col2:
        st.markdown(
            """
            <div class="main-header">
                <h1 style="margin:0;font-family:'Segoe UI',sans-serif;color:#222;">
                    🐯 Tiger X Market Intelligence
                </h1>
                <span style="font-size:0.9rem;color:#555;">
                    Advanced Trade Analytics Dashboard | By Marketing Team
                </span>
            </div>
            """,
            unsafe_allow_html=True,
        )
    
    with col3:
        st.markdown(f"**🕒 {datetime.now().strftime('%H:%M')}**")

render_header()

# ─── Enhanced Database Connection with Connection Pooling ──────────────────────
@st.cache_resource
def get_database_engine():
    """Initialize database engine with optimized settings"""
    load_dotenv("credentials.env")
    USER = os.getenv("DB_USER")
    PWD = os.getenv("DB_PASS")
    HOST = os.getenv("DB_HOST")
    DB = os.getenv("DB_NAME", "VOLZA")
    
    URI = f"mysql+pymysql://{USER}:{PWD}@{HOST}:3306/{DB}"
    
    # Enhanced engine with connection pooling and optimization
    engine = create_engine(
        URI,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=20,
        pool_recycle=3600,
        echo=False,  # Set to True for SQL debugging
        connect_args={
    "charset": "utf8mb4",
    "connect_timeout": 60,        # Increased from 30
    "read_timeout": 60,           # Increased from 30
    "write_timeout": 60,          # Increased from 30
    "autocommit": True,           # Add this
    "init_command": "SET SESSION wait_timeout=300"  # Add this
}
    )
    return engine

engine = get_database_engine()

# ─── Loading Animation Component ───────────────────────────────────────────────
def show_loading(message="Searching database..."):
    """Display animated loading indicator"""
    return st.markdown(f"""
    <div class="loading-container">
        <div class="loading-ship">🚢</div>
        <h3 style="color: #666; margin-top: 1rem;">{message}</h3>
        <p style="color: #999;">Optimizing your search results...</p>
    </div>
    """, unsafe_allow_html=True)

# ─── Optimized Data Fetching Functions ─────────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def get_distinct_optimized(col: str, limit: int = 1000):
    """Fetch distinct values with LIMIT for better performance"""
    sql = text(f"""
        SELECT DISTINCT `{col}` 
        FROM volza_main 
        WHERE `{col}` IS NOT NULL AND `{col}` != ''
        ORDER BY `{col}`
        LIMIT {limit}
    """)
    
    try:
        with engine.connect() as conn:
            result = conn.execute(sql).fetchall()
            return [r[0] for r in result]
    except Exception as e:
        st.error(f"Database error: {e}")
        return []

@st.cache_data(ttl=1800, show_spinner=False)
def get_count_estimate(where_clause: str, params: dict):
    """Get approximate count for large datasets"""
    sql = text(f"SELECT COUNT(*) FROM volza_main WHERE {where_clause}")
    
    try:
        with engine.connect() as conn:
            result = conn.execute(sql, params).scalar()
            return result
    except Exception as e:
        st.error(f"Count error: {e}")
        return 0
@st.cache_data(ttl=1800, show_spinner=False)
def get_filtered_countries(hs_code_prefix: str = None, mode: str = "Export"):
    """Get countries filtered by HS code if provided"""
    country_column = "country_of_destination" if "Export" in mode else "country_of_origin"
    
    if hs_code_prefix:
        sql = text(f"""
            SELECT DISTINCT `{country_column}` 
            FROM volza_main 
            WHERE `{country_column}` IS NOT NULL 
            AND `{country_column}` != ''
            AND hs_code LIKE :hs
            ORDER BY `{country_column}`
            LIMIT 200
        """)
        params = {"hs": f"{hs_code_prefix}%"}
    else:
        # Use a faster query for all countries
        sql = text(f"""
            SELECT `{country_column}`, COUNT(*) as cnt
            FROM volza_main 
            WHERE `{country_column}` IS NOT NULL 
            AND `{country_column}` != ''
            GROUP BY `{country_column}`
            ORDER BY cnt DESC
            LIMIT 100
        """)
        params = {}
    
    try:
        with engine.connect() as conn:
            conn.execute(text("SET SESSION wait_timeout=300"))  # Add this
            result = conn.execute(sql, params).fetchall()
            return [r[0] for r in result]
    except Exception as e:
        st.error(f"Database error: {e}")
        return ["USA", "CHINA", "GERMANY", "UK", "JAPAN"]  # Fallback list

@st.cache_data(ttl=3600, show_spinner=False)
def get_hs_codes_for_mode(mode: str):
    """Get unique 2-digit HS codes filtered by export/import mode"""
    if "Export" in mode:
        # For exports, get HS codes that have destination countries
        sql = text("""
            SELECT DISTINCT LEFT(hs_code, 2) as hs_2digit
            FROM volza_main 
            WHERE hs_code IS NOT NULL 
            AND hs_code != ''
            AND country_of_destination IS NOT NULL
            AND country_of_destination != ''
            AND LENGTH(hs_code) >= 2
            ORDER BY hs_2digit
        """)
    else:
        # For imports, get HS codes that have origin countries
        sql = text("""
            SELECT DISTINCT LEFT(hs_code, 2) as hs_2digit
            FROM volza_main 
            WHERE hs_code IS NOT NULL 
            AND hs_code != ''
            AND country_of_origin IS NOT NULL
            AND country_of_origin != ''
            AND LENGTH(hs_code) >= 2
            ORDER BY hs_2digit
        """)
    
    try:
        with engine.connect() as conn:
            conn.execute(text("SET SESSION wait_timeout=300"))
            result = conn.execute(sql).fetchall()
            return [r[0] for r in result]
    except Exception as e:
        st.error(f"Database error: {e}")
        return ["85", "84", "87", "90", "73"]  # Fallback
    

def fuzzy_filter_optimized(choices, query, limit=50, cutoff=80):
    """Optimized fuzzy matching with early termination"""
    if not query or len(query) < 2:
        return set()
    

    # Early exact match check
    exact_matches = {choice for choice in choices if query.lower() in choice.lower()}
    if exact_matches:
        return exact_matches
    
    # Fuzzy search with optimized parameters
    matches = process.extract(
        query, choices, scorer=fuzz.WRatio, limit=limit
    )
    return {match for match, score, _ in matches if score >= cutoff}

# ─── Enhanced Sidebar with Better UX ───────────────────────────────────────────
def render_sidebar():
    st.sidebar.markdown("### 🔍 **Search & Filter**")
    
    # Mode selection with icons
    mode = st.sidebar.radio(
        "**📊 Analysis Mode**",
        ["🇮🇳➡️ India Export", "🇮🇳⬅️ India Import"],
        help="Choose between export or import analysis"
    )
    
    st.sidebar.markdown("---")
    
    # Enhanced filters with help text
   
    
    ship_q = st.sidebar.text_input(
        "**🏢 Shipper Name**",
        placeholder="Enter company name...",
        help="Fuzzy search - partial matches work!"
    )
    
    cons_q = st.sidebar.text_input(
        "**🏭 Consignee Name**",
        placeholder="Enter company name...",
        help="Fuzzy search - partial matches work!"
    )
    
    prod_q = st.sidebar.text_input(
        "**📦 Product Description**",
        placeholder="Enter product keywords...",
        help="Search in product descriptions"
    )
    
    notify_q = st.sidebar.text_input(
        "**📞 Notify Party**",
        placeholder="Enter notify party...",
        help="Search in notify party field"
    )
    st.sidebar.markdown("---")
    # Country selection with loading
   # HS Code selection with dynamic filtering
    with st.sidebar:
        st.markdown("**📋 HS Code Selection**")
        available_hs = get_hs_codes_for_mode(mode)

          # HS Code dropdown with mode-specific suggestions
        selected_2digits = st.multiselect(
    "**2‑Digit HS Code**",
        available_hs,
        help="Select one or more 2‑digit HS codes"
    )
        
        # Manual HS input (optional for more specific codes)
        manual_hs = st.text_input(
            "**Specific HS Code (Optional)**",
            placeholder="e.g., 8517, 851712...",
            help="Enter more specific HS code or leave empty to use 2-digit selection"
        )
        
        # Determine final HS code to use
        final_hs = manual_hs if manual_hs else selected_2digits
        
    # Dynamic country selection based on HS code
    with st.sidebar:
        if "🇮🇳➡️ India Export" in mode:
            with st.spinner("Loading destinations..."):
                if final_hs:
                    dests = get_filtered_countries(final_hs, mode)
                    help_text = f"Countries available for HS code {final_hs}"
                else:
                    dests = get_filtered_countries(None, mode)
                    help_text = "Select HS code first for filtered results"
                    
            sel_dest = st.multiselect(
                f"**🌍 Country of Destination** ({len(dests)} available)",
                dests,
                help=help_text
            )
            sel_orig = None
        else:
            with st.spinner("Loading origins..."):
                if final_hs:
                    origs = get_filtered_countries(final_hs, mode)
                    help_text = f"Countries available for HS code {final_hs}"
                else:
                    origs = get_filtered_countries(None, mode)
                    help_text = "Select HS code first for filtered results"
                    
            sel_orig = st.multiselect(
                f"**🌍 Country of Origin** ({len(origs)} available)",
                origs,
                help=help_text
            )
            sel_dest = None
    st.sidebar.markdown("---")
    # Enhanced search button
    search_clicked = st.sidebar.button(
        "🚀 **SEARCH DATABASE**",
        use_container_width=True,
        help="Click to execute your search query"
    )
    
    # Performance tips
    with st.sidebar.expander("💡 **Performance Tips**"):
        st.markdown("""
        - Use **HS Code prefix** for faster searches
        - Limit country selection to 3-5 countries
        - Use specific keywords in text searches
        """)
    
    return {
        'mode': mode,
        'hs_q': selected_2digits,  # now a list of prefixes
        'ship_q': ship_q,
        'cons_q': cons_q,
        'prod_q': prod_q,
        'notify_q': notify_q,
        'sel_dest': sel_dest,
        'sel_orig': sel_orig,
        'search_clicked': search_clicked
    }
# ─── Enhanced Results Display Functions ────────────────────────────────────────
def render_kpis(df):
    """Render KPI metrics with enhanced styling"""
    if df.empty:
        return
    
    df['date'] = pd.to_datetime(df['date'])
    start_date = df['date'].min().strftime("%d-%b-%Y")
    end_date = df['date'].max().strftime("%d-%b-%Y")
    
    # Create metric columns
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric(
            label="📦 Total Shipments",
            value=f"{len(df):,}",
            delta=f"{len(df)/1000:.1f}K records" if len(df) > 1000 else None
        )
    
    with col2:
        unique_shippers = df['shipper_name'].nunique()
        st.metric(
            label="🏢 Shippers",
            value=f"{unique_shippers:,}",
            delta=f"{unique_shippers/len(df)*100:.1f}% diversity"
        )
    
    with col3:
        unique_consignees = df['consignee_name'].nunique()
        st.metric(
            label="🏭 Consignees",
            value=f"{unique_consignees:,}",
            delta=f"{unique_consignees/len(df)*100:.1f}% diversity"
        )
    
    with col4:
        unique_notify = df['notify_party'].nunique()
        st.metric(
            label="📞 Notify Parties",
            value=f"{unique_notify:,}",
            delta="Active contacts"
        )
    
    with col5:
        st.metric(
            label="📅 Time Period",
            value=f"{start_date}",
            delta=f"to {end_date}"
        )

def render_charts(df, analysis_mode):
    """Render enhanced charts with Plotly"""
    if df.empty:
        return
    
    # Chart row 1
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 🚢 **Shipment Mode Distribution**")
        mode_counts = df['shipment_mode'].value_counts()
        
        fig = px.pie(
            values=mode_counts.values,
            names=mode_counts.index,
            color_discrete_sequence=['#FFD700', '#FFA500', '#FF8C00', '#FF7F50']
        )
        fig.update_traces(
            textposition='inside', 
            textinfo='percent+label',
            textfont=dict(size=12, color='black'),
            textfont_size=14
        )
        fig.update_layout(
            showlegend=True, 
            height=400,
            font=dict(size=12, color='black')
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown("#### 🌍 **Top 10 Destination Ports**")
        top_ports = df['port_of_destination'].value_counts().head(10)
        
        fig = px.bar(
            x=top_ports.values,
            y=top_ports.index,
            orientation='h',
            color=top_ports.values,
            color_continuous_scale='Viridis'
        )
        fig.update_layout(
            showlegend=False,
            height=400,
            yaxis={'categoryorder': 'total ascending'},
            xaxis_title="<b>Number of Shipments</b>",
            yaxis_title="<b>Ports</b>",
            font=dict(size=11)
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Chart row 2
    col3, col4 = st.columns(2)
    
    with col3:
        st.markdown("#### 📈 **Monthly Shipment Trends**")
        df['year_month'] = df['date'].dt.to_period('M').astype(str)
        monthly_data = df['year_month'].value_counts().sort_index()
        
        fig = px.line(
            x=monthly_data.index,
            y=monthly_data.values,
            markers=True,
            color_discrete_sequence=['#FFD700']
        )
        fig.update_layout(
            showlegend=False,
            height=400,
            xaxis_title="<b>Month</b>",
            yaxis_title="<b>Shipments</b>",
            font=dict(size=11)
        )
        fig.update_traces(line=dict(width=3), marker=dict(size=8))
        st.plotly_chart(fig, use_container_width=True)
    
    with col4:
        st.markdown("#### 📦 **Top 10 Products**")
        top_products = df['product_description'].value_counts().head(10)
        
        fig = px.bar(
            x=top_products.values,
            y=[desc[:30] + "..." if len(desc) > 30 else desc for desc in top_products.index],
            orientation='h',
            color=top_products.values,
            color_continuous_scale='Plasma'
        )
        fig.update_layout(
            showlegend=False,
            height=400,
            yaxis={'categoryorder': 'total ascending'},
            xaxis_title="<b>Number of Shipments</b>",
            yaxis_title="<b>Products</b>",
            font=dict(size=11)
        )
        st.plotly_chart(fig, use_container_width=True)

    # Chart row 3 - NEW ROW
    col5, col6 = st.columns(2)

    with col5:
        if "Export" in analysis_mode:
            st.markdown("#### 🏢 **Top 10 Shippers**")
            top_shippers = df['shipper_name'].value_counts().head(10)
            
            fig = px.bar(
                x=top_shippers.values,
                y=[name[:30] + "..." if len(name) > 30 else name for name in top_shippers.index],
                orientation='h',
                color=top_shippers.values,
                color_continuous_scale='Blues'
            )
        else:  # Import mode
            st.markdown("#### 🏭 **Top 10 Consignees**")
            top_consignees = df['consignee_name'].value_counts().head(10)
            
            fig = px.bar(
                x=top_consignees.values,
                y=[name[:30] + "..." if len(name) > 30 else name for name in top_consignees.index],
                orientation='h',
                color=top_consignees.values,
                color_continuous_scale='Blues'
            )
        
        fig.update_layout(
            showlegend=False,
            height=400,
            yaxis={'categoryorder': 'total ascending'},
            xaxis_title="<b>Number of Shipments</b>",
            yaxis_title="<b>Companies</b>",
            font=dict(size=11)
        )
        st.plotly_chart(fig, use_container_width=True)

    with col6:
        st.markdown("#### 📣 **Top 10 Notify Parties**")
        top_notifies = df['notify_party'].value_counts().head(10)

        fig = px.bar(
            x=top_notifies.values,
            y=[name[:30] + "..." if len(name) > 30 else name for name in top_notifies.index],
            orientation='h',
            color=top_notifies.values,
            color_continuous_scale='Purples'
        )
        fig.update_layout(
            showlegend=False,
            height=400,
            yaxis={'categoryorder': 'total ascending'},
            xaxis_title="<b>Number of Shipments</b>",
            yaxis_title="<b>Notify Parties</b>",
            font=dict(size=11)
        )
        st.plotly_chart(fig, use_container_width=True)

        pass
# ─── Main Application Logic ────────────────────────────────────────────────────
def main():
    # Render sidebar and get parameters
    params = render_sidebar()
    
    if params['search_clicked']:
        # Show loading animation
        loading_placeholder = st.empty()
        with loading_placeholder:
            show_loading("🔍 Processing your search query...")
        
        start_time = time.time()
        
        # Build query with optimal filter order (most selective first)
        clauses, sql_params = [], {}
        
        # 1. HS Code filtering first (most selective)
        if params['hs_q']:
            clauses.append("LEFT(hs_code,2) IN :hs2")
            sql_params["hs2"] = tuple(params['hs_q'])
        
        # 2. Country filtering second
        if "Export" in params['mode'] and params['sel_dest']:
            placeholders = ','.join([f':dest{i}' for i in range(len(params['sel_dest']))])
            clauses.append(f"country_of_destination IN ({placeholders})")
            for i, dest in enumerate(params['sel_dest']):
                sql_params[f"dest{i}"] = dest
        elif "Import" in params['mode'] and params['sel_orig']:
            placeholders = ','.join([f':orig{i}' for i in range(len(params['sel_orig']))])
            clauses.append(f"country_of_origin IN ({placeholders})")
            for i, orig in enumerate(params['sel_orig']):
                sql_params[f"orig{i}"] = orig
        
        # 3. Fuzzy searches (only if previous filters don't reduce dataset enough)
        
        perform_fuzzy = True
            
        if perform_fuzzy:
            with loading_placeholder:
                show_loading("🤖 Processing fuzzy searches...")
            
            # Batch fuzzy searches for better performance
            fuzzy_searches = [
                (params['ship_q'], "shipper_name", "ship"),
                (params['cons_q'], "consignee_name", "cons"), 
                (params['prod_q'], "product_description", "prod"),
                (params['notify_q'], "notify_party", "notf")
            ]
            
            for query, column, param_key in fuzzy_searches:
                if query:
                    choices = get_distinct_optimized(column, 1500)  # Reduced from 2000
                    matches = fuzzy_filter_optimized(choices, query, limit=30, cutoff=75)
                    if matches:
                        placeholders = ','.join([f':{param_key}{i}' for i in range(len(matches))])
                        clauses.append(f"{column} IN ({placeholders})")
                        for i, match in enumerate(matches):
                            sql_params[f"{param_key}{i}"] = match
        
        # Build final query
       # Build final optimized query
        where_clause = " AND ".join(clauses) if clauses else "1=1"
        
        
       
            
        # Get count first
        with loading_placeholder:
            show_loading("📊 Counting results...")
        
        total_count = get_count_estimate(where_clause, sql_params)
        
        if total_count == 0:
            loading_placeholder.empty()
            st.warning("🔍 **No results found!** Try adjusting your search criteria.")
            st.info("💡 **Tips:** Use broader search terms or remove some filters.")
            return
        
        
        
        with loading_placeholder:
            show_loading("📥 Fetching your data...")
        
        try:
            sql_query = text(f"""
                SELECT * FROM volza_main 
                WHERE {where_clause}
                ORDER BY date DESC
                
            """)
            
            df = pd.read_sql_query(sql_query, engine, params=sql_params)
            
        except Exception as e:
            loading_placeholder.empty()
            st.error(f"❌ **Database Error:** {e}")
            st.info("🔧 Please try again or contact support if the issue persists.")
            return
        
        # Clear loading animation
        loading_placeholder.empty()
        
        # Show performance metrics
        elapsed_time = time.time() - start_time
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.success(f"✅ **Search completed in {elapsed_time:.2f}s**")
        with col2:
            st.info(f"📊 **Found {len(df):,} records**")
        with col3:
            if len(df) < total_count:
                st.warning(f"⚠️ **Showing top {len(df):,} of {total_count:,}**")
        
        st.markdown("---")
        
        # Render results
        if not df.empty:
            # KPIs
            render_kpis(df)
            st.markdown("---")
            
            # Charts
            st.markdown("## 📈 **Analytics Dashboard**")
            render_charts(df, params['mode'])

            st.markdown("---")
            
            # Data table with enhanced features
            st.markdown("## 📋 **Detailed Results**")
            
            #Add filters for the data table
            col1, col2, col3 = st.columns(3)
            with col1:
                show_sample = st.checkbox("📝 Show sample (1000 rows)", value=True)
            with col2:
                sort_by = st.selectbox("🔄 Sort by", ["date", "shipper_name", "consignee_name"])
            with col3:
                sort_order = st.selectbox("📊 Order", ["Descending", "Ascending"])
            
            # ─── Apply sorting and sampling ─────────────────────────────────────────────────
            display_df = df.copy()
            ascending = sort_order == "Ascending"
            display_df = display_df.sort_values(by=sort_by, ascending=ascending)

            # Drop technical columns
            exclude = [
                "gross_weight","gross_weight_unit","raw_shipper_name","raw_consignee_name",
                "raw_shipper_address1","raw_shipper_address2","raw_shipper_city","raw_shipper_state",
                "raw_consignee_add1","raw_consignee_add2","raw_consignee_city","raw_consignee_state",
                "raw_consignee_pincode","raw_consignee_phone","raw_consignee_e_mail",
                "raw_consignee_country","is_unique","isunique","record_id","iec",
                "source_file","source_folder","processed_timestamp"
            ]
            display_df = display_df.drop(columns=exclude, errors="ignore")

            # If sampling, trim to top 1,000
            if show_sample and len(display_df) > 1000:
                display_df = display_df.head(1000)
                st.info(f"📋 Showing sample of 1000 rows from {len(df):,} total records")

            # Render table
            st.dataframe(display_df, use_container_width=True, height=400)

            # ─── Enhanced download options ─────────────────────────────────────────────────
            # Prepare clean full‐download (without tech columns)
            download_df = df.drop(columns=exclude, errors="ignore")

            col1, col2, col3 = st.columns(3)
            with col1:
                csv_data = download_df.to_csv(index=False)
                st.download_button(
                    "📥 **Download Full CSV**",
                    csv_data,
                    f"tiger_x_export_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                    "text/csv",
                    use_container_width=True
                )
            with col2:
                sample_csv = display_df.to_csv(index=False)
                st.download_button(
                    "📄 **Download Sample CSV**",
                    sample_csv,
                    f"tiger_x_sample_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                    "text/csv",
                    use_container_width=True
                )
            with col3:
                st.markdown(f"**📊 Total Records:** {len(df):,}")

            st.markdown("---")

            
            # Enhanced Summary Table
            st.markdown("## 📊 **Executive Summary**")
            
            if "Export" in params['mode']:
                summary = (df.groupby(['shipper_name', 'product_description'], as_index=False)
                          .agg({
                              'date': 'count',
                              'shipper_contact_person': 'first',
                              'notify_party': lambda x: x.value_counts().index[0] if len(x.value_counts()) > 0 else '',
                              'shipper_email': 'first',
                              'shipper_phone': 'first',
                              'shipper_city': 'first'
                          })
                          .rename(columns={
                              'date': 'shipments',
                              'shipper_contact_person': 'contact_person',
                              'shipper_email': 'email',
                              'shipper_phone': 'phone',
                              'shipper_city': 'location'
                          })
                          .sort_values('shipments', ascending=False))
                
                st.markdown("### 🏢 **Top Exporters by Shipment Volume**")
                
            else:
                summary = (df.groupby(['consignee_name', 'product_description'], as_index=False)
                          .agg({
                              'date': 'count',
                              'contact_person': 'first',
                              'consignee_e_mail': 'first',
                              'consignee_city': 'first'
                          })
                          .rename(columns={
                              'date': 'shipments',
                              'consignee_e_mail': 'email',
                              'consignee_city': 'location'
                          })
                          .sort_values('shipments', ascending=False))
                
                st.markdown("### 🏭 **Top Importers by Shipment Volume**")
            
            # Display summary with enhanced styling
            st.dataframe(
                summary.head(100),
                use_container_width=True,
                height=400
            )
            
            # Summary download
            summary_csv = summary.to_csv(index=False)
            st.download_button(
                "📥 **Download Summary Report**",
                summary_csv,
                f"tiger_x_summary_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                "text/csv"
            )
    
    else:
        # Welcome screen
        st.markdown("""
        ## 🚀 **Welcome to Tiger Logistics X Market Intelligence**
        
        **Your Advanced Trade Analytics Platform**
        
        ### 🔍 **Getting Started:**
        1. **Select your analysis mode** (Export/Import) in the sidebar
        2. **Apply filters** to narrow your search:
           - Country filters for targeted analysis
           - HS Code prefix for specific product categories
           - Company names with fuzzy search capability
           - Product descriptions and notify parties
        3. **Click "SEARCH DATABASE"** to get instant insights
        
        ### 💡 **Pro Tips:**
        - Use **HS Code prefixes** (2, 4, 6, or 8 digits) for faster, more targeted searches
        - **Fuzzy search** works with partial company names - no need for exact matches
        - Start with **3-5 countries** max for optimal performance
        - Use **specific keywords** in product searches
        
       
        ---
        **🔧 Powered by cloud infrastructure **
        """)
        
        # Show sample data or recent stats
        with st.expander("📈 **Platform Statistics**"):
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("🗄️ **Database Size**", "1M+ Records")
            with col2:
                st.metric("🌍 **Countries**", "50+ Covered")
            with col3:
                st.metric("🏢 **Companies**", "1000+")
            with col4:
                st.metric("⚡ **Avg Query Time**", "< 10 seconds")

if __name__ == "__main__":
    main()
