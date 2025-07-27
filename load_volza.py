import glob
import os
import sys
import time
import math
import difflib
import re
import logging
from pathlib import Path
from typing import List, Dict, Tuple, Optional, Set
from collections import defaultdict

import pandas as pd
from sqlalchemy import create_engine, text, MetaData, Table, Column, String, Text
from sqlalchemy.dialects.mysql import LONGTEXT
from dotenv import load_dotenv

# ——— CONFIGURATION ———
DEFAULT_FOLDER      = Path("Final_Download")
TABLE_NAME          = "volza_main"
ENV_FILE            = "credentials.env"
BATCH_SIZE          = 25_000  # Reduced for better memory management
FUZZY_MATCH_CUTOFF  = 0.80   # Slightly reduced for better matching
EXCEL_HEADER_ROW    = 1
MAX_RETRIES         = 3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f"volza_processing_{int(time.time())}.log")
    ]
)
logger = logging.getLogger(__name__)

UNIFIED_SCHEMA = [
    "Date","HS Code","Product Description","HS Description","HS2","HS4","Month",
    "Shipper Name","Consignee Name","Notify Party",
    "Shipper Address1","Shipper Address2","Shipper City","Shipper State","Shipper Pincode",
    "Shipper Phone","Shipper Email","Shipper Contact Person",
    "Consignee Address 1","Consignee Address 2","Consignee City","Consignee State",
    "Consignee Pincode","Consignee Phone","Consignee E-mail","Contact Person",
    "Port of Origin","Port of Destination","Country of Origin","Country of Destination","Shipment Mode",
    "Standard Qty","Standard Unit","QTY","Unit",
    "Standard Unit Rate $","Estimated F.O.B Value $","Estimated CIF Value $",
    "Estimated Unit Rate $","Unit Rate $","Value in FC","Rate In FC","Rate Currency",
    "Landed Value $","Tax $","Tax %","Freight Value $","Insurance Value $",
    "BL TYP","Terms","Gross Weight","Gross Weight Unit",
    "Raw Shipper Name","Raw Consignee Name","Raw Shipper Address1","Raw Shipper Address2",
    "Raw Shipper City","Raw Shipper State","Raw Consignee Add1","Raw Consignee Add2",
    "Raw Consignee City","Raw Consignee State","Raw Consignee Pincode",
    "Raw Consignee Phone","Raw Consignee E-mail","Raw Consignee Country",
    "Is Unique","IsUnique","Record Id","IEC"
]

# Enhanced column mappings for better accuracy
COLUMN_MAPPINGS = {
    "shipper name": "Shipper Name",
    "shipper_name": "Shipper Name",
    "consignee name": "Consignee Name",
    "consignee_name": "Consignee Name",
    "hs code": "HS Code",
    "product description": "Product Description",
    "country of origin": "Country of Origin",
    "country of destination": "Country of Destination",
    "port of origin": "Port of Origin",
    "port of destination": "Port of Destination",
    "shipment mode": "Shipment Mode",
    "date": "Date",
    "month": "Month",
    "qty": "QTY",
    "unit": "Unit",
    "value": "Estimated F.O.B Value $",
    "fob value": "Estimated F.O.B Value $",
    "cif value": "Estimated CIF Value $",
}

def sanitize_column_name(raw: str) -> str:
    """Improved column name sanitization"""
    if not raw or pd.isna(raw):
        return "unnamed"
    s = str(raw).strip()
    
    # Handle special characters more carefully
    for ch in ["$", "%", ".", "?", "(", ")", "[", "]", "{", "}"]:
        s = s.replace(ch, "_")
    
    # Replace non-alphanumeric characters with underscores
    s = re.sub(r"[^\w\s]", "_", s)
    s = re.sub(r"\s+", "_", s)  # Replace spaces with underscores
    s = re.sub(r"_+", "_", s).strip("_").lower()
    
    # Ensure column doesn't start with number
    return f"col_{s}" if re.match(r"^\d", s) else s or "unnamed"

def normalize_for_matching(col: str) -> str:
    """Enhanced normalization for better fuzzy matching"""
    s = str(col).strip().lower()
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def dedupe_columns(cols: List[str]) -> List[str]:
    """Remove duplicate column names by adding suffixes"""
    seen = {}
    out = []
    for c in cols:
        if c in seen:
            seen[c] += 1
            out.append(f"{c}_{seen[c]}")
        else:
            seen[c] = 0
            out.append(c)
    return out

def discover_excel_files(base: Path) -> List[Tuple[Path, str, str]]:
    """Recursively discover Excel files in nested folder structure"""
    files = []
    
    def scan_directory(directory: Path, parent_folder: str = ""):
        for item in directory.iterdir():
            if item.is_dir():
                # Recursively scan subdirectories
                folder_name = f"{parent_folder}/{item.name}" if parent_folder else item.name
                scan_directory(item, folder_name)
            elif item.suffix.lower() in ['.xlsx', '.xls']:
                try:
                    with pd.ExcelFile(item) as xls:
                        # Look for "Data Sheet" first, then fall back to first sheet
                        sheet = None
                        for s in xls.sheet_names:
                            if "data sheet" in s.lower():
                                sheet = s
                                break
                        if not sheet:
                            sheet = xls.sheet_names[0]
                        
                        folder_name = parent_folder if parent_folder else base.name
                        files.append((item, sheet, folder_name))
                        
                except Exception as e:
                    logger.warning(f"[!] Could not read {item.name}: {e}")
    
    scan_directory(base)
    logger.info(f"Found {len(files)} Excel files across all folders")
    return files

def analyze_variations(excel_files):
    """Analyze header variations across all files"""
    variations = defaultdict(int)
    all_headers = set()
    
    for path, sheet, folder in excel_files:
        try:
            # Read just the headers
            hdrs = pd.read_excel(path, sheet_name=sheet, header=EXCEL_HEADER_ROW, nrows=0).columns
            key = tuple(str(h).strip() for h in hdrs)
            variations[key] += 1
            all_headers.update(key)
            
        except Exception as e:
            logger.warning(f"[!] Failed reading headers from {path.name}: {e}")
    
    logger.info(f"Found {len(variations)} unique header patterns across {len(all_headers)} unique columns")
    
    # Log the most common patterns for debugging
    sorted_variations = sorted(variations.items(), key=lambda x: x[1], reverse=True)
    for i, (pattern, count) in enumerate(sorted_variations[:3]):
        logger.info(f"Pattern {i+1} ({count} files): {len(pattern)} columns")
    
    return all_headers

def build_mapping(all_hdrs: Set[str]) -> Dict[str, str]:
    """Build comprehensive mapping from raw headers to unified schema"""
    schema_norm = {normalize_for_matching(u): u for u in UNIFIED_SCHEMA}
    mapping = {}
    
    for raw in all_hdrs:
        raw_str = str(raw).strip()
        normalized = normalize_for_matching(raw_str)
        
        # Check exact mapping first
        if normalized in COLUMN_MAPPINGS:
            mapping[raw_str] = COLUMN_MAPPINGS[normalized]
        # Check if normalized version exists in schema
        elif normalized in schema_norm:
            mapping[raw_str] = schema_norm[normalized]
        else:
            # Use fuzzy matching for close matches
            best_matches = difflib.get_close_matches(
                normalized, 
                schema_norm.keys(), 
                n=1, 
                cutoff=FUZZY_MATCH_CUTOFF
            )
            if best_matches:
                mapping[raw_str] = schema_norm[best_matches[0]]
                logger.debug(f"Fuzzy matched '{raw_str}' -> '{mapping[raw_str]}'")
            else:
                mapping[raw_str] = raw_str
                logger.debug(f"No match found for '{raw_str}', keeping original")
    
    logger.info(f"Built mapping for {len(mapping)} columns")
    return mapping

def process_file(path: Path, sheet: str, folder: str, mapping: Dict[str, str], final_cols: List[str]) -> Optional[pd.DataFrame]:
    """Process individual Excel file with enhanced error handling"""
    try:
        # Read the Excel file
        df = pd.read_excel(path, sheet_name=sheet, header=EXCEL_HEADER_ROW, dtype=str)
        
        if df.empty:
            logger.warning(f"{path.name} is empty, skipping")
            return None
        
        # Map and sanitize column names
        new_cols = []
        for col in df.columns:
            col_str = str(col).strip()
            mapped_col = mapping.get(col_str, col_str)
            sanitized_col = sanitize_column_name(mapped_col)
            new_cols.append(sanitized_col)
        
        # Handle duplicate columns
        new_cols = dedupe_columns(new_cols)
        df.columns = new_cols
        
        # Add missing columns from final schema
        for final_col in final_cols:
            if final_col not in df.columns:
                df[final_col] = pd.NA
        
        # Select only the columns that exist in our final schema
        available_cols = [c for c in final_cols if c in df.columns]
        df = df[available_cols]
        
        # Remove header rows that might have slipped through
        if len(df) > 0 and final_cols:
            first_col = final_cols[0]
            if first_col in df.columns:
                # Remove rows where first column contains the header name
                header_mask = df[first_col].astype(str).str.strip().str.lower() != normalize_for_matching(first_col)
                df = df[header_mask]
        
        # Add metadata columns
        df["source_file"] = path.name
        df["source_folder"] = folder
        df["processed_timestamp"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
        
        logger.info(f"[+] Processed: {path.name} | Rows: {len(df)} | Columns: {len(df.columns)}")
        return df
        
    except Exception as e:
        logger.error(f"[!] Failed to process {path.name}: {e}")
        return None

def create_table(engine, table_name: str, sample_df: pd.DataFrame):
    """Create MySQL table with proper column types based on sample data"""
    column_types = {}
    
    for col in sample_df.columns:
        try:
            # Get non-null values for type detection
            series = sample_df[col].dropna()
            
            if len(series) == 0:
                column_types[col] = "TEXT"
                continue
            
            # Convert to string for analysis
            str_series = series.astype(str)
            
            # Check if numeric
            numeric_count = 0
            for val in str_series.head(100):  # Sample first 100 values
                try:
                    float(val.replace(',', ''))  # Handle comma-separated numbers
                    numeric_count += 1
                except:
                    pass
            
            # If more than 80% of values are numeric, treat as DOUBLE
            if numeric_count / min(len(str_series), 100) > 0.8:
                column_types[col] = "DOUBLE"
            else:
                # Determine string length for VARCHAR vs TEXT
                max_length = str_series.str.len().max()
                if pd.isna(max_length) or max_length == 0:
                    column_types[col] = "TEXT"
                elif max_length < 255:
                    column_types[col] = f"VARCHAR({min(255, max(50, int(max_length * 1.2)))})"
                else:
                    column_types[col] = "TEXT"
                    
        except Exception as e:
            logger.warning(f"[!] Failed to detect type for column '{col}': {e}")
            column_types[col] = "TEXT"
    
    # Create the table
    column_definitions = []
    for col, col_type in column_types.items():
        # Escape column names with backticks
        column_definitions.append(f"`{col}` {col_type}")
    
    create_sql = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({', '.join(column_definitions)})"
    
    with engine.connect() as conn:
        # Drop existing table
        conn.execute(text(f"DROP TABLE IF EXISTS `{table_name}`"))
        conn.commit()
        
        # Create new table
        conn.execute(text(create_sql))
        conn.commit()
    
    logger.info(f"[+] Created table `{table_name}` with {len(column_types)} columns")

def upload_to_mysql(df: pd.DataFrame, engine, table_name: str):
    """Upload DataFrame to MySQL with optimized batching"""
    
    # Remove duplicate columns if any
    df = df.loc[:, ~df.columns.duplicated()]
    
    # Create table based on sample
    sample_size = min(1000, len(df))
    create_table(engine, table_name, df.head(sample_size))
    
    # Upload in batches
    total_rows = len(df)
    batches = math.ceil(total_rows / BATCH_SIZE)
    
    logger.info(f"Uploading {total_rows:,} rows in {batches} batches of {BATCH_SIZE:,} each")
    
    successful_uploads = 0
    
    for batch_num in range(batches):
        start_idx = batch_num * BATCH_SIZE
        end_idx = min(start_idx + BATCH_SIZE, total_rows)
        batch_df = df.iloc[start_idx:end_idx].copy()
        
        retry_count = 0
        while retry_count < MAX_RETRIES:
            try:
                batch_df.to_sql(
                    table_name, 
                    engine, 
                    if_exists="append", 
                    index=False, 
                    method="multi",
                    chunksize=5000
                )
                successful_uploads += len(batch_df)
                logger.info(f"[+] Batch {batch_num + 1}/{batches}: Uploaded rows {start_idx:,}-{end_idx:,}")
                break
                
            except Exception as e:
                retry_count += 1
                logger.warning(f"[!] Batch {batch_num + 1} failed (attempt {retry_count}): {e}")
                if retry_count >= MAX_RETRIES:
                    logger.error(f"[!] Failed to upload batch {batch_num + 1} after {MAX_RETRIES} attempts")
                else:
                    time.sleep(2 ** retry_count)  # Exponential backoff
    
    logger.info(f"[+] Upload complete: {successful_uploads:,}/{total_rows:,} rows successfully uploaded")

def validate_data_quality(df: pd.DataFrame):
    """Perform basic data quality checks"""
    logger.info("=== DATA QUALITY REPORT ===")
    logger.info(f"Total rows: {len(df):,}")
    logger.info(f"Total columns: {len(df.columns)}")
    
    # Check for completely empty rows
    empty_rows = df.isnull().all(axis=1).sum()
    logger.info(f"Completely empty rows: {empty_rows:,}")
    
    # Check column completeness
    for col in df.columns[:10]:  # Check first 10 columns
        non_null_count = df[col].count()
        completeness = (non_null_count / len(df)) * 100
        logger.info(f"Column '{col}': {completeness:.1f}% complete ({non_null_count:,} values)")
    
    logger.info("=== END QUALITY REPORT ===")

def main():
    """Main execution function"""
    start_time = time.time()
    
    # Load environment variables
    load_dotenv(ENV_FILE)
    
    # Database configuration
    user = os.getenv("DB_USER")
    pwd = os.getenv("DB_PASS")
    host = os.getenv("DB_HOST")
    db = os.getenv("DB_NAME", "VOLZA")
    
    if not all([user, pwd, host]):
        logger.error("Missing database credentials in environment file")
        sys.exit(1)
    
    # Create database connection
    uri = f"mysql+pymysql://{user}:{pwd}@{host}:3306/{db}"
    engine = create_engine(uri, pool_pre_ping=True, pool_recycle=3600)
    
    # Get input folder
    folder = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_FOLDER
    
    if not folder.exists():
        logger.error(f"Folder '{folder}' does not exist")
        sys.exit(1)
    
    logger.info(f"Processing folder: {folder}")
    
    # Step 1: Discover all Excel files
    excel_files = discover_excel_files(folder)
    if not excel_files:
        logger.error("No Excel files found")
        sys.exit(1)
    
    # Step 2: Analyze header variations
    all_headers = analyze_variations(excel_files)
    
    # Step 3: Build column mapping
    mapping = build_mapping(all_headers)
    
    # Step 4: Define final column schema
    final_cols = [sanitize_column_name(col) for col in UNIFIED_SCHEMA]
    final_cols.extend(["source_file", "source_folder", "processed_timestamp"])
    
    # Step 5: Process all files
    logger.info("Starting file processing...")
    dataframes = []
    
    for i, (path, sheet, folder_name) in enumerate(excel_files, 1):
        logger.info(f"Processing file {i}/{len(excel_files)}: {path.name}")
        df = process_file(path, sheet, folder_name, mapping, final_cols)
        if df is not None and not df.empty:
            dataframes.append(df)
    
    if not dataframes:
        logger.error("No valid data found in any files")
        sys.exit(1)
    
    # Step 6: Combine all dataframes
    logger.info("Combining all dataframes...")
    combined_df = pd.concat(dataframes, ignore_index=True, sort=False)
    
    # Remove any duplicate columns that might have been created
    combined_df = combined_df.loc[:, ~combined_df.columns.duplicated()]
    
    logger.info(f"Combined dataset shape: {combined_df.shape}")
    
    # Step 7: Data quality validation
    validate_data_quality(combined_df)
    
    # Step 8: Upload to MySQL
    logger.info("Starting upload to MySQL...")
    upload_to_mysql(combined_df, engine, TABLE_NAME)
    
    # Final summary
    execution_time = time.time() - start_time
    logger.info(f"=== PROCESSING COMPLETE ===")
    logger.info(f"Files processed: {len(excel_files)}")
    logger.info(f"Total rows: {len(combined_df):,}")
    logger.info(f"Total columns: {len(combined_df.columns)}")
    logger.info(f"Execution time: {execution_time:.2f} seconds")
    logger.info(f"Table created: {TABLE_NAME}")
    logger.info("Ready for dashboard development!")

if __name__ == "__main__":
    main()