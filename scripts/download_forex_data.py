"""
Download real forex 1-minute data from HistData.com, resample to 1-hour bars.

Source: HistData.com (free for personal/research use)
Method: histdata Python package (pip install histdata)
Original timezone: Eastern Time (ET / EST / EDT) — HistData uses New York time
Output timezone: UTC

This script downloads 1-minute data and resamples to 1-hour OHLCV bars.
Volume from HistData is TICK COUNT (number of ticks per bar), not actual traded volume.
This is documented and noted in the output.
"""

import os
import sys
import time
import zipfile
import glob
import pandas as pd
from histdata import download_hist_data as dl
from histdata.api import Platform as P, TimeFrame as TF

# Configuration
PAIRS = ["eurusd", "gbpusd", "usdjpy", "audusd", "usdcad", "usdchf"]
YEARS = list(range(2017, 2025))  # 2017-2024
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "external")
DOWNLOAD_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "downloads_m1")

# HistData Generic ASCII column names
# Format: DateTime;Open;High;Low;Close;Volume
# DateTime format: YYYYMMDD HHMMSS
COLUMNS = ["DateTime", "Open", "High", "Low", "Close", "Volume"]


def download_pair_year(pair: str, year: int) -> str:
    """Download 1-minute data for a pair/year. Returns path to zip file."""
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    old_cwd = os.getcwd()
    os.chdir(DOWNLOAD_DIR)
    
    zip_name = f"DAT_ASCII_{pair.upper()}_M1_{year}.zip"
    zip_path = os.path.join(DOWNLOAD_DIR, zip_name)
    
    if os.path.exists(zip_path):
        print(f"  Already downloaded: {zip_name}")
        os.chdir(old_cwd)
        return zip_path
    
    try:
        print(f"  Downloading {pair.upper()} {year} M1...")
        result = dl(pair=pair, year=str(year), month=None, 
                    platform=P.GENERIC_ASCII, time_frame=TF.ONE_MINUTE)
        os.chdir(old_cwd)
        
        if result and os.path.exists(os.path.join(DOWNLOAD_DIR, os.path.basename(str(result)))):
            actual_path = os.path.join(DOWNLOAD_DIR, os.path.basename(str(result)))
            print(f"  OK: {os.path.basename(actual_path)} ({os.path.getsize(actual_path)} bytes)")
            return actual_path
        else:
            print(f"  WARN: Download returned {result} but file not found")
            return None
    except Exception as e:
        os.chdir(old_cwd)
        print(f"  ERROR downloading {pair.upper()} {year}: {e}")
        return None


def extract_and_read_m1(zip_path: str) -> pd.DataFrame:
    """Extract zip and read 1-minute CSV data."""
    extract_dir = os.path.dirname(zip_path)
    
    with zipfile.ZipFile(zip_path, 'r') as zf:
        names = zf.namelist()
        csv_name = [n for n in names if n.endswith('.csv') or n.endswith('.txt')]
        if not csv_name:
            # Sometimes the file inside has no extension
            csv_name = names
        zf.extractall(extract_dir)
    
    # Find the extracted file
    extracted = os.path.join(extract_dir, csv_name[0])
    if not os.path.exists(extracted):
        # Try finding it
        base = os.path.splitext(os.path.basename(zip_path))[0]
        candidates = glob.glob(os.path.join(extract_dir, base + ".*"))
        candidates += glob.glob(os.path.join(extract_dir, "DAT_*"))
        for c in candidates:
            if not c.endswith('.zip'):
                extracted = c
                break
    
    # HistData ASCII format: semicolon-separated, no header
    # DateTime;Open;High;Low;Close;Volume
    try:
        df = pd.read_csv(extracted, sep=';', header=None, names=COLUMNS)
    except Exception:
        # Try comma separated
        df = pd.read_csv(extracted, sep=',', header=None, names=COLUMNS)
    
    # Parse datetime - HistData format: YYYYMMDD HHMMSS or YYYY.MM.DD HH:MM:SS
    # Handle multiple possible formats
    sample = str(df['DateTime'].iloc[0]).strip()
    if '.' in sample and ':' in sample:
        # Format: YYYY.MM.DD HH:MM:SS
        df['DateTime'] = pd.to_datetime(df['DateTime'].str.strip(), format='%Y.%m.%d %H:%M:%S')
    elif len(sample.replace(' ', '')) == 14:
        # Format: YYYYMMDD HHMMSS
        df['DateTime'] = pd.to_datetime(df['DateTime'].str.strip(), format='%Y%m%d %H%M%S')
    else:
        df['DateTime'] = pd.to_datetime(df['DateTime'].str.strip())
    
    return df


def resample_to_hourly(df_m1: pd.DataFrame) -> pd.DataFrame:
    """Resample 1-minute bars to 1-hour bars."""
    df_m1 = df_m1.set_index('DateTime').sort_index()
    
    hourly = df_m1.resample('1h').agg({
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last',
        'Volume': 'sum'
    }).dropna()
    
    hourly = hourly.reset_index()
    hourly = hourly.rename(columns={'DateTime': 'Date'})
    return hourly


def convert_et_to_utc(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert HistData Eastern Time timestamps to UTC.
    HistData uses New York time (ET), which is UTC-5 (EST) or UTC-4 (EDT).
    """
    # Localize to US/Eastern first (handles DST automatically)
    df['Date'] = pd.to_datetime(df['Date'])
    df['Date'] = df['Date'].dt.tz_localize('US/Eastern', ambiguous='NaT', nonexistent='shift_forward')
    df['Date'] = df['Date'].dt.tz_convert('UTC')
    df['Date'] = df['Date'].dt.tz_localize(None)  # Remove tz info for clean CSV
    
    # Drop any NaT rows from ambiguous DST transitions
    df = df.dropna(subset=['Date'])
    return df


def validate_pair_data(df: pd.DataFrame, pair: str) -> dict:
    """Validate downloaded data for plausibility."""
    result = {
        'pair': pair.upper(),
        'rows': len(df),
        'date_min': str(df['Date'].min()),
        'date_max': str(df['Date'].max()),
        'close_min': df['Close'].min(),
        'close_max': df['Close'].max(),
        'close_mean': df['Close'].mean(),
        'duplicates': df.duplicated(subset=['Date']).sum(),
        'nulls': df.isnull().sum().sum(),
        'plausible': True,
        'issues': []
    }
    
    # Price plausibility ranges (approximate for 2017-2024)
    PLAUSIBLE_RANGES = {
        'EURUSD': (0.95, 1.30),
        'GBPUSD': (1.05, 1.45),
        'USDJPY': (100, 165),
        'AUDUSD': (0.55, 0.85),
        'USDCAD': (1.15, 1.50),
        'USDCHF': (0.80, 1.05),
    }
    
    pair_upper = pair.upper()
    if pair_upper in PLAUSIBLE_RANGES:
        lo, hi = PLAUSIBLE_RANGES[pair_upper]
        if result['close_min'] < lo * 0.9 or result['close_max'] > hi * 1.1:
            result['plausible'] = False
            result['issues'].append(
                f"Close range [{result['close_min']:.5f}, {result['close_max']:.5f}] "
                f"outside expected [{lo}, {hi}]"
            )
    
    if result['duplicates'] > 0:
        result['issues'].append(f"{result['duplicates']} duplicate timestamps")
    
    if result['nulls'] > 0:
        result['issues'].append(f"{result['nulls']} null values")
    
    # Check rows per year
    df_copy = df.copy()
    df_copy['Year'] = pd.to_datetime(df_copy['Date']).dt.year
    yearly = df_copy.groupby('Year').size()
    result['rows_per_year'] = yearly.to_dict()
    
    for yr, cnt in yearly.items():
        if cnt < 4000:
            result['issues'].append(f"Year {yr} has only {cnt} rows (expected ~6000+)")
    
    return result


def process_pair(pair: str):
    """Download, extract, resample, convert, validate, and save one pair."""
    print(f"\n{'='*60}")
    print(f"Processing {pair.upper()}")
    print(f"{'='*60}")
    
    all_hourly = []
    
    for year in YEARS:
        zip_path = download_pair_year(pair, year)
        if zip_path is None:
            print(f"  Skipping {pair.upper()} {year} - download failed")
            continue
        
        try:
            df_m1 = extract_and_read_m1(zip_path)
            print(f"  {pair.upper()} {year}: {len(df_m1)} M1 bars")
            
            df_h1 = resample_to_hourly(df_m1)
            print(f"  Resampled to {len(df_h1)} H1 bars")
            
            all_hourly.append(df_h1)
        except Exception as e:
            print(f"  ERROR processing {pair.upper()} {year}: {e}")
            import traceback
            traceback.print_exc()
            continue
        
        # Be polite to the server
        time.sleep(1)
    
    if not all_hourly:
        print(f"  No data obtained for {pair.upper()}")
        return None
    
    # Combine all years
    combined = pd.concat(all_hourly, ignore_index=True)
    combined = combined.sort_values('Date').reset_index(drop=True)
    
    # Convert ET to UTC
    print(f"  Converting {len(combined)} rows from ET to UTC...")
    combined = convert_et_to_utc(combined)
    
    # Remove duplicates (DST transitions can cause overlaps)
    before_dedup = len(combined)
    combined = combined.drop_duplicates(subset=['Date'], keep='first')
    if len(combined) < before_dedup:
        print(f"  Removed {before_dedup - len(combined)} duplicate timestamps from DST transitions")
    
    # Validate
    validation = validate_pair_data(combined, pair)
    print(f"\n  Validation for {pair.upper()}:")
    print(f"    Rows: {validation['rows']}")
    print(f"    Date range: {validation['date_min']} to {validation['date_max']}")
    print(f"    Close range: [{validation['close_min']:.5f}, {validation['close_max']:.5f}]")
    print(f"    Plausible: {validation['plausible']}")
    if validation['issues']:
        for issue in validation['issues']:
            print(f"    [WARN] {issue}")
    print(f"    Rows per year: {validation['rows_per_year']}")
    
    # Save
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, f"{pair.upper()}_1h_2017-2024_histdata.csv")
    combined.to_csv(out_path, index=False)
    print(f"  Saved to {out_path} ({os.path.getsize(out_path)} bytes)")
    
    return validation


def main():
    print("=" * 60)
    print("Forex Historical Data Downloader")
    print("Source: HistData.com (free for personal/research use)")
    print("Method: Download M1, resample to H1, convert ET->UTC")
    print("Volume note: HistData volume = tick count, not traded volume")
    print("=" * 60)
    
    results = {}
    for pair in PAIRS:
        validation = process_pair(pair)
        if validation:
            results[pair.upper()] = validation
    
    # Summary
    print("\n" + "=" * 60)
    print("DOWNLOAD SUMMARY")
    print("=" * 60)
    for pair, v in results.items():
        status = "[PASS] PASS" if v['plausible'] else "[FAIL] FAIL"
        print(f"  {pair}: {v['rows']} rows, "
              f"[{v['close_min']:.4f}-{v['close_max']:.4f}], "
              f"{status}")
        if v['issues']:
            for issue in v['issues']:
                print(f"    [WARN] {issue}")
    
    return results


if __name__ == "__main__":
    main()

