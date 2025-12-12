"""
Analyse registration count comparisons: PDS vs EMIS and OLIDS vs EMIS.
Generates a detailed markdown report focusing on where and why differences occur.
EMIS is the source of truth.
"""

import os
from pathlib import Path
from datetime import datetime
import numpy as np
from dotenv import load_dotenv
from snowflake.snowpark import Session
import pandas as pd

# Load environment variables
load_dotenv()

# Configuration
SCRIPT_DIR = Path(__file__).parent
OUTPUT_DIR = SCRIPT_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

PDS_EMIS_QUERY_FILE = SCRIPT_DIR / "registration_comparison_native_query.sql"
OLIDS_EMIS_QUERY_FILE = SCRIPT_DIR / "olids_emis_comparison_native_query.sql"
WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
USER = os.getenv('SNOWFLAKE_USER')
ROLE = os.getenv('SNOWFLAKE_ROLE')


def parse_sql_file(file_path):
    """Parse SQL file and extract USE statements and the main query."""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    lines = content.split('\n')
    use_statements = []
    query_lines = []
    in_query = False
    
    for line in lines:
        line_stripped = line.strip()
        line_upper = line_stripped.upper()
        
        if line_upper.startswith('USE '):
            use_statements.append(line_stripped.rstrip(';'))
            continue
        
        if line_upper.startswith('WITH') or line_upper.startswith('SELECT'):
            in_query = True
        
        if in_query:
            query_lines.append(line)
    
    query = '\n'.join(query_lines).strip().rstrip(';')
    
    if not query or query.upper().startswith('USE'):
        statements = [s.strip() for s in content.split(';')]
        for stmt in statements:
            stmt_upper = stmt.upper()
            if stmt_upper.startswith('WITH') or stmt_upper.startswith('SELECT'):
                query = stmt.strip()
                break
    
    return use_statements, query


def fix_encoding(text):
    """Fix common encoding issues in text."""
    if pd.isna(text):
        return ''
    text_str = str(text)
    # Fix common encoding issues
    replacements = {
        '�': "'",  # Replace replacement character with apostrophe
        '\x92': "'",  # Windows-1252 apostrophe
        '\x93': '"',  # Windows-1252 opening quote
        '\x94': '"',  # Windows-1252 closing quote
        '\u2019': "'",  # Right single quotation mark
        '\u2018': "'",  # Left single quotation mark
        '\u201C': '"',  # Left double quotation mark
        '\u201D': '"',  # Right double quotation mark
    }
    for old, new in replacements.items():
        text_str = text_str.replace(old, new)
    # Fix specific known issues
    text_str = text_str.replace("Wakeman�s", "Wakeman's")
    text_str = text_str.replace("St George�s", "St George's")
    return text_str

def execute_query(session, use_statements, query, query_name):
    """Execute USE statements and SQL query, return results as pandas DataFrame."""
    print(f"Executing {query_name}...")
    try:
        for use_stmt in use_statements:
            session.sql(use_stmt).collect()
        df = session.sql(query).to_pandas()
        # Fix encoding issues in text columns
        text_columns = df.select_dtypes(include=['object']).columns
        for col in text_columns:
            df[col] = df[col].apply(fix_encoding)
        print(f"✓ {query_name}: {len(df)} rows")
        return df
    except Exception as e:
        print(f"✗ Error executing {query_name}: {e}")
        raise


def analyse_pds_vs_emis(pds_emis_df):
    """Analyse PDS vs EMIS differences - EMIS is source of truth."""
    # Calculate overall statistics
    valid_comparison = pds_emis_df[
        (pds_emis_df['PDS_MERGED_PERSONS'].notna()) & 
        (pds_emis_df['EMIS_LIST_SIZE'].notna())
    ].copy()
    
    total_pds = valid_comparison['PDS_MERGED_PERSONS'].sum()
    total_emis = valid_comparison['EMIS_LIST_SIZE'].sum()
    overall_diff = total_emis - total_pds
    overall_diff_pct = (overall_diff / total_pds * 100) if total_pds > 0 else 0
    
    analysis = {
        'total_practices': len(pds_emis_df),
        'excellent_match': len(pds_emis_df[pds_emis_df['MATCH_CATEGORY'] == 'Excellent Match (<0.5%)']),
        'good_match': len(pds_emis_df[pds_emis_df['MATCH_CATEGORY'] == 'Good Match (0.5-1%)']),
        'small_diff': len(pds_emis_df[pds_emis_df['MATCH_CATEGORY'] == 'Small Difference (1-5%)']),
        'moderate_diff': len(pds_emis_df[pds_emis_df['MATCH_CATEGORY'] == 'Moderate Difference (5-20%)']),
        'minor_diff': len(pds_emis_df[pds_emis_df['MATCH_CATEGORY'].isin(['Small Difference (1-5%)', 'Moderate Difference (5-20%)'])]),
        'major_diff': len(pds_emis_df[pds_emis_df['MATCH_CATEGORY'] == 'Major Difference (±20%+)']),
        'pds_only': len(pds_emis_df[pds_emis_df['MATCH_CATEGORY'] == 'PDS Only']),
        'emis_only': len(pds_emis_df[pds_emis_df['MATCH_CATEGORY'] == 'EMIS Only']),
        'total_pds': total_pds,
        'total_emis': total_emis,
        'overall_diff': overall_diff,
        'overall_diff_pct': overall_diff_pct,
        'df': pds_emis_df.copy()
    }
    
    # Practices where PDS is higher than EMIS
    pds_higher = pds_emis_df[
        (pds_emis_df['DIFFERENCE'] < 0) & 
        (pds_emis_df['PDS_MERGED_PERSONS'].notna()) &
        (pds_emis_df['EMIS_LIST_SIZE'].notna())
    ].copy()
    pds_higher['abs_diff_pct'] = pds_higher['DIFFERENCE_PCT'].abs()
    
    # Practices where PDS is lower than EMIS
    pds_lower = pds_emis_df[
        (pds_emis_df['DIFFERENCE'] > 0) & 
        (pds_emis_df['PDS_MERGED_PERSONS'].notna()) &
        (pds_emis_df['EMIS_LIST_SIZE'].notna())
    ].copy()
    pds_lower['abs_diff_pct'] = pds_lower['DIFFERENCE_PCT'].abs()
    
    analysis['pds_higher'] = pds_higher.sort_values('abs_diff_pct', ascending=False)
    analysis['pds_lower'] = pds_lower.sort_values('abs_diff_pct', ascending=False)
    
    return analysis


def analyse_olids_vs_emis(olids_emis_df):
    """Analyse OLIDS vs EMIS differences - EMIS is source of truth."""
    # Calculate overall statistics
    valid_comparison = olids_emis_df[
        (olids_emis_df['OLIDS_REGULAR_REGISTRATIONS'].notna()) & 
        (olids_emis_df['EMIS_LIST_SIZE'].notna())
    ].copy()
    
    total_olids = valid_comparison['OLIDS_REGULAR_REGISTRATIONS'].sum()
    total_emis = valid_comparison['EMIS_LIST_SIZE'].sum()
    overall_diff = total_olids - total_emis
    overall_diff_pct = (overall_diff / total_emis * 100) if total_emis > 0 else 0
    
    analysis = {
        'total_practices': len(olids_emis_df),
        'excellent_match': len(olids_emis_df[olids_emis_df['MATCH_CATEGORY'] == 'Excellent Match (<1%)']),
        'good_match': len(olids_emis_df[olids_emis_df['MATCH_CATEGORY'] == 'Good Match (1-5%)']),
        'minor_diff': len(olids_emis_df[olids_emis_df['MATCH_CATEGORY'] == 'Moderate Difference (5-20%)']),
        'major_diff': len(olids_emis_df[olids_emis_df['MATCH_CATEGORY'] == 'Major Difference (±20%+)']),
        'olids_only': len(olids_emis_df[olids_emis_df['MATCH_CATEGORY'] == 'OLIDS Only']),
        'emis_only': len(olids_emis_df[olids_emis_df['MATCH_CATEGORY'] == 'EMIS Only']),
        'no_regular': len(olids_emis_df[olids_emis_df['MATCH_CATEGORY'] == 'No Regular Episodes']),
        'total_olids': total_olids,
        'total_emis': total_emis,
        'overall_diff': overall_diff,
        'overall_diff_pct': overall_diff_pct,
        'df': olids_emis_df.copy()
    }
    
    # Practices where OLIDS is higher than EMIS (comparing REGULAR registrations)
    olids_higher = olids_emis_df[
        (olids_emis_df['DIFFERENCE'] > 0) & 
        (olids_emis_df['OLIDS_REGULAR_REGISTRATIONS'].notna()) &
        (olids_emis_df['EMIS_LIST_SIZE'].notna())
    ].copy()
    olids_higher['abs_diff_pct'] = olids_higher['DIFFERENCE_PCT'].abs()
    
    # Practices where OLIDS is lower than EMIS (comparing REGULAR registrations)
    olids_lower = olids_emis_df[
        (olids_emis_df['DIFFERENCE'] < 0) & 
        (olids_emis_df['OLIDS_REGULAR_REGISTRATIONS'].notna()) &
        (olids_emis_df['EMIS_LIST_SIZE'].notna())
    ].copy()
    olids_lower['abs_diff_pct'] = olids_lower['DIFFERENCE_PCT'].abs()
    
    analysis['olids_higher'] = olids_higher.sort_values('abs_diff_pct', ascending=False)
    analysis['olids_lower'] = olids_lower.sort_values('abs_diff_pct', ascending=False)
    
    return analysis


def analyse_olids_vs_pds(pds_emis_df, olids_emis_df):
    """Analyse OLIDS vs PDS differences - secondary analysis. Uses REGULAR registrations."""
    # Merge on practice code
    comparison = pd.merge(
        pds_emis_df[['PRACTICE_CODE', 'PRACTICE_NAME', 'PDS_MERGED_PERSONS']],
        olids_emis_df[['PRACTICE_CODE', 'OLIDS_REGULAR_REGISTRATIONS', 'OLIDS_REGISTERED_PATIENTS', 
                       'TYPE_REGULAR', 'TYPE_TEMPORARY', 'TYPE_EMERGENCY', 'TYPE_PRIVATE', 
                       'TYPE_CLINICAL_SERVICES', 'TYPE_OTHER']],
        on='PRACTICE_CODE',
        how='inner'
    )
    
    # Compare REGULAR registrations to PDS (regular registrations only)
    comparison['diff'] = comparison['OLIDS_REGULAR_REGISTRATIONS'] - comparison['PDS_MERGED_PERSONS']
    comparison['diff_pct'] = (
        comparison['diff'] / comparison['PDS_MERGED_PERSONS'].replace(0, pd.NA) * 100
    ).fillna(0)
    comparison['abs_diff_pct'] = comparison['diff_pct'].abs()
    
    # Where OLIDS is higher than PDS
    olids_higher = comparison[comparison['diff'] > 0].sort_values('abs_diff_pct', ascending=False)
    
    # Where OLIDS is lower than PDS
    olids_lower = comparison[comparison['diff'] < 0].sort_values('abs_diff_pct', ascending=False)
    
    return {
        'total_practices': len(comparison),
        'olids_higher': olids_higher,
        'olids_lower': olids_lower,
        'comparison': comparison
    }


def analyse_overlap(pds_analysis, olids_analysis):
    """Analyse overlap between practices with PDS vs EMIS issues and OLIDS vs EMIS issues."""
    # Practices with PDS vs EMIS discrepancies (not excellent or good match)
    pds_issues = pds_analysis['df'][
        ~pds_analysis['df']['MATCH_CATEGORY'].isin(['Excellent Match (<0.5%)', 'Good Match (0.5-1%)', 'PDS Only', 'EMIS Only'])
    ].copy()
    pds_issue_codes = set(pds_issues['PRACTICE_CODE'].unique())
    
    # Practices with OLIDS vs EMIS discrepancies (not excellent or good match)
    olids_issues = olids_analysis['df'][
        ~olids_analysis['df']['MATCH_CATEGORY'].isin(['Excellent Match (<1%)', 'Good Match (1-5%)', 'OLIDS Only', 'EMIS Only', 'No Regular Episodes'])
    ].copy()
    olids_issue_codes = set(olids_issues['PRACTICE_CODE'].unique())
    
    # Overlap: practices with issues in both comparisons
    overlap_codes = pds_issue_codes & olids_issue_codes
    
    # Practices with PDS issues but not OLIDS issues
    pds_only_issues = pds_issue_codes - olids_issue_codes
    
    # Practices with OLIDS issues but not PDS issues
    olids_only_issues = olids_issue_codes - pds_issue_codes
    
    # Get details for overlapping practices
    overlap_df = pds_issues[pds_issues['PRACTICE_CODE'].isin(overlap_codes)].copy()
    overlap_df = overlap_df.merge(
        olids_issues[['PRACTICE_CODE', 'MATCH_CATEGORY', 'DIFFERENCE_PCT']].rename(
            columns={'MATCH_CATEGORY': 'OLIDS_MATCH_CATEGORY', 'DIFFERENCE_PCT': 'OLIDS_DIFFERENCE_PCT'}
        ),
        on='PRACTICE_CODE',
        how='left'
    )
    
    return {
        'pds_issue_count': len(pds_issue_codes),
        'olids_issue_count': len(olids_issue_codes),
        'overlap_count': len(overlap_codes),
        'pds_only_count': len(pds_only_issues),
        'olids_only_count': len(olids_only_issues),
        'overlap_df': overlap_df.sort_values('DIFFERENCE_PCT', key=abs, ascending=False),
        'pds_only_codes': pds_only_issues,
        'olids_only_codes': olids_only_issues
    }


def get_match_indicator(category):
    """Get visual indicator for match category using emojis."""
    indicators = {
        'Excellent Match (<1%)': '⭐',
        'Excellent Match (<0.5%)': '⭐',
        'Good Match (1-5%)': '✅',
        'Good Match (0.5-1%)': '✅',
        'Small Difference (1-5%)': '⚡',
        'Moderate Difference (5-20%)': '⚠️',
        'Major Difference (±20%+)': '❌',
        'PDS Only': '➖',
        'EMIS Only': '➖',
        'OLIDS Only': '➖',
        'No Regular Episodes': '⭕'
    }
    return indicators.get(category, '❓')

def get_category_sort_order(category):
    """Get sort order for match category (lower = better match)."""
    order_map = {
        'Excellent Match (<1%)': 1,
        'Excellent Match (<0.5%)': 1,
        'Good Match (1-5%)': 2,
        'Good Match (0.5-1%)': 2,
        'Small Difference (1-5%)': 3,
        'Moderate Difference (5-20%)': 4,
        'Minor Difference (5-20%)': 4,
        'Major Difference (±20%+)': 5,
        'PDS Only': 6,
        'EMIS Only': 6,
        'OLIDS Only': 6,
        'No Regular Episodes': 7
    }
    return order_map.get(category, 99)

def format_pct_diff(value, decimals=1):
    """Format percentage difference, handling NaN and infinity."""
    if pd.isna(value) or np.isinf(value):
        return "—"
    return f"{value:+.{decimals}f}%"

def generate_markdown_report(pds_analysis, olids_analysis, olids_pds_analysis, overlap_analysis, output_file):
    """Generate a detailed markdown report."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Remove duplicates from dataframes
    olids_df = olids_analysis['df'].drop_duplicates(subset=['PRACTICE_CODE'], keep='first')
    pds_df = pds_analysis['df'].drop_duplicates(subset=['PRACTICE_CODE'], keep='first')
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("# OLIDS Registration Count Debugging Report\n\n")
        f.write(f"**Generated:** {timestamp}\n\n")
        f.write("**Source of Truth:** EMIS List Size (2025-11-04) - Manual extract from EMIS system\n\n")
        f.write("### Methodology Overview\n\n")
        f.write("This report compares registration counts across three data sources, all filtered to **2025-11-04** (the EMIS extract date):\n\n")
        f.write("1. **EMIS List Size** (Source of Truth): Regular list size per practice from manual EMIS extract\n")
        f.write("2. **PDS Registry**: Regular registrations per practice (national registry - temporary registrations not included)\n")
        f.write("3. **OLIDS Source Data**: Active REGULAR episodes of care per practice\n\n")
        f.write("**Key Comparison:** OLIDS REGULAR vs EMIS (apples-to-apples) - Expected difference <0.5%\n\n")
        f.write("---\n\n")
        
        # Two distinct issues
        f.write("## Two Distinct Issues\n\n")
        f.write("### Issue 1: PDS vs EMIS Validation (Yardstick Problem)\n")
        f.write("PDS and EMIS counts are not agreeing for quite a few practices. This is a validation concern as PDS is being used as a yardstick.\n\n")
        f.write("### Issue 2: OLIDS vs EMIS (Main Developer Issue) ⚠️\n")
        f.write("OLIDS REGULAR registrations don't consistently agree with EMIS. Expected difference should be <0.5% overall, but we're seeing larger discrepancies.\n\n")
        f.write("---\n\n")
        
        # Calculate PDS percentages (needed for Issue 1 section)
        pds_excellent_pct = pds_analysis['excellent_match']/pds_analysis['total_practices']*100
        pds_good_pct = pds_analysis['good_match']/pds_analysis['total_practices']*100
        pds_total_good = pds_analysis['excellent_match'] + pds_analysis['good_match']
        pds_total_good_pct = pds_total_good/pds_analysis['total_practices']*100
        pds_total_issues = pds_analysis['minor_diff'] + pds_analysis['major_diff']
        pds_total_issues_pct = pds_total_issues/pds_analysis['total_practices']*100
        
        # Issue 1: PDS vs EMIS Validation
        f.write("## Issue 1: PDS vs EMIS Validation (Yardstick Problem)\n\n")
        f.write(f"**Problem:** PDS and EMIS counts broadly agree overall ({pds_analysis['overall_diff_pct']:+.2f}% difference), but don't agree for some practices, which is concerning since PDS is being used as a yardstick.\n\n")
        
        f.write("### Overall Statistics\n\n")
        f.write(f"| Metric | Value |\n")
        f.write(f"|--------|-------|\n")
        f.write(f"| **Total Practices** | {pds_analysis['total_practices']} |\n")
        f.write(f"| **Total PDS** | {pds_analysis['total_pds']:,.0f} |\n")
        f.write(f"| **Total EMIS** | {pds_analysis['total_emis']:,.0f} |\n")
        f.write(f"| **Overall Difference** | {pds_analysis['overall_diff']:+,.0f} |\n")
        f.write(f"| **Overall % Difference** | {pds_analysis['overall_diff_pct']:+.2f}% |\n\n")
        
        f.write("### Match Quality Breakdown\n\n")
        f.write(f"**Practices matching well:** {pds_total_good} ({pds_total_good_pct:.1f}%)\n")
        f.write(f"**Practices with discrepancies:** {pds_total_issues} ({pds_total_issues_pct:.1f}%)\n\n")
        f.write("| Match Quality | Count | Percentage |\n")
        f.write("|---------------|-------|------------|\n")
        f.write(f"| ⭐ Excellent (<0.5%) | {pds_analysis['excellent_match']} | {pds_excellent_pct:.1f}% |\n")
        f.write(f"| ✅ Good (0.5-1%) | {pds_analysis['good_match']} | {pds_good_pct:.1f}% |\n")
        f.write(f"| ⚡ Small Difference (1-5%) | {pds_analysis['small_diff']} | {pds_analysis['small_diff']/pds_analysis['total_practices']*100:.1f}% |\n")
        f.write(f"| ⚠️ Moderate Difference (5-20%) | {pds_analysis['moderate_diff']} | {pds_analysis['moderate_diff']/pds_analysis['total_practices']*100:.1f}% |\n")
        f.write(f"| ❌ Major Difference (≥20%) | {pds_analysis['major_diff']} | {pds_analysis['major_diff']/pds_analysis['total_practices']*100:.1f}% |\n\n")
        
        f.write("**Legend:** ⭐ Excellent (<0.5%) | ✅ Good (0.5-1%) | ⚡ Small Difference (1-5%) | ⚠️ Moderate Difference (5-20%) | ❌ Major Difference (≥20%) | ⭕ No Regular Episodes | ➖ Data Missing\n\n")
        
        # Critical Issues Section for PDS vs EMIS
        pds_critical_issues = pds_df[
            (pds_df['PDS_MERGED_PERSONS'].notna()) & 
            (pds_df['EMIS_LIST_SIZE'].notna()) &
            (pds_df['DIFFERENCE_PCT'].abs() > 20)
        ].sort_values('DIFFERENCE_PCT')
        
        if len(pds_critical_issues) > 0:
            f.write("### 🔴 Critical Issues: Practices with >20% Difference\n\n")
            f.write(f"**{len(pds_critical_issues)} practice(s)** have major discrepancies (>20% difference). These require investigation.\n\n")
            f.write("| Practice Code | Practice Name | PDS Count | EMIS Count | Difference | % Difference |\n")
            f.write("|--------------|---------------|-----------|------------|------------|--------------|\n")
            for _, row in pds_critical_issues.iterrows():
                practice_name = str(row.get('PRACTICE_NAME', '') or '').replace('|', '\\|')
                f.write(f"| {row['PRACTICE_CODE']} | {practice_name} | "
                       f"{row['PDS_MERGED_PERSONS']:,.0f} | {row['EMIS_LIST_SIZE']:,.0f} | "
                       f"{row['DIFFERENCE']:+,.0f} | {format_pct_diff(row['DIFFERENCE_PCT'], 1)} |\n")
            f.write("\n")
        
        f.write("---\n\n")
        
        # Detailed Analysis: PDS vs EMIS - All Categories in Order
        f.write("### Detailed Analysis: PDS vs EMIS by Match Category\n\n")
        
        # PDS vs EMIS - Excellent Matches
        pds_excellent = pds_df[pds_df['MATCH_CATEGORY'] == 'Excellent Match (<0.5%)']
        if len(pds_excellent) > 0:
            f.write(f"### ⭐ PDS vs EMIS: Excellent Matches (<0.5%) - {len(pds_excellent)} practices\n\n")
            f.write("| Practice Code | Practice Name | PDS Count | EMIS Count | Difference | % Difference |\n")
            f.write("|--------------|---------------|-----------|------------|------------|--------------|\n")
            for _, row in pds_excellent.sort_values('DIFFERENCE_PCT').iterrows():
                practice_name = str(row.get('PRACTICE_NAME', '') or '').replace('|', '\\|')
                f.write(f"| {row['PRACTICE_CODE']} | {practice_name} | "
                       f"{row['PDS_MERGED_PERSONS']:,.0f} | {row['EMIS_LIST_SIZE']:,.0f} | "
                       f"{row['DIFFERENCE']:+,.0f} | {format_pct_diff(row['DIFFERENCE_PCT'], 2)} |\n")
            f.write("\n")
        
        # PDS vs EMIS - Good Matches
        pds_good = pds_df[pds_df['MATCH_CATEGORY'] == 'Good Match (0.5-1%)']
        if len(pds_good) > 0:
            f.write(f"### ✅ PDS vs EMIS: Good Matches (0.5-1%) - {len(pds_good)} practices\n\n")
            f.write("| Practice Code | Practice Name | PDS Count | EMIS Count | Difference | % Difference |\n")
            f.write("|--------------|---------------|-----------|------------|------------|--------------|\n")
            for _, row in pds_good.sort_values('DIFFERENCE_PCT').iterrows():
                practice_name = str(row.get('PRACTICE_NAME', '') or '').replace('|', '\\|')
                f.write(f"| {row['PRACTICE_CODE']} | {practice_name} | "
                       f"{row['PDS_MERGED_PERSONS']:,.0f} | {row['EMIS_LIST_SIZE']:,.0f} | "
                       f"{row['DIFFERENCE']:+,.0f} | {format_pct_diff(row['DIFFERENCE_PCT'], 1)} |\n")
            f.write("\n")
        
        # PDS vs EMIS - Small Differences
        pds_small = pds_df[pds_df['MATCH_CATEGORY'] == 'Small Difference (1-5%)']
        if len(pds_small) > 0:
            f.write(f"### ⚡ PDS vs EMIS: Small Differences (1-5%) - {len(pds_small)} practices\n\n")
            f.write("| Practice Code | Practice Name | PDS Count | EMIS Count | Difference | % Difference |\n")
            f.write("|--------------|---------------|-----------|------------|------------|--------------|\n")
            for _, row in pds_small.sort_values('DIFFERENCE_PCT').iterrows():
                practice_name = str(row.get('PRACTICE_NAME', '') or '').replace('|', '\\|')
                f.write(f"| {row['PRACTICE_CODE']} | {practice_name} | "
                       f"{row['PDS_MERGED_PERSONS']:,.0f} | {row['EMIS_LIST_SIZE']:,.0f} | "
                       f"{row['DIFFERENCE']:+,.0f} | {format_pct_diff(row['DIFFERENCE_PCT'], 1)} |\n")
            f.write("\n")
        
        # PDS vs EMIS - Moderate Differences
        pds_moderate = pds_df[pds_df['MATCH_CATEGORY'] == 'Moderate Difference (5-20%)']
        if len(pds_moderate) > 0:
            f.write(f"### ⚠️ PDS vs EMIS: Moderate Differences (5-20%) - {len(pds_moderate)} practices\n\n")
            f.write("| Practice Code | Practice Name | PDS Count | EMIS Count | Difference | % Difference |\n")
            f.write("|--------------|---------------|-----------|------------|------------|--------------|\n")
            for _, row in pds_moderate.sort_values('DIFFERENCE_PCT').iterrows():
                practice_name = str(row.get('PRACTICE_NAME', '') or '').replace('|', '\\|')
                f.write(f"| {row['PRACTICE_CODE']} | {practice_name} | "
                       f"{row['PDS_MERGED_PERSONS']:,.0f} | {row['EMIS_LIST_SIZE']:,.0f} | "
                       f"{row['DIFFERENCE']:+,.0f} | {format_pct_diff(row['DIFFERENCE_PCT'], 1)} |\n")
            f.write("\n")
        
        # PDS vs EMIS - Major Differences
        pds_major = pds_df[pds_df['MATCH_CATEGORY'] == 'Major Difference (±20%+)']
        if len(pds_major) > 0:
            f.write(f"### ❌ PDS vs EMIS: Major Differences (≥20%) - {len(pds_major)} practices\n\n")
            f.write("| Practice Code | Practice Name | PDS Count | EMIS Count | Difference | % Difference |\n")
            f.write("|--------------|---------------|-----------|------------|------------|--------------|\n")
            for _, row in pds_major.sort_values('DIFFERENCE_PCT').iterrows():
                practice_name = str(row.get('PRACTICE_NAME', '') or '').replace('|', '\\|')
                f.write(f"| {row['PRACTICE_CODE']} | {practice_name} | "
                       f"{row['PDS_MERGED_PERSONS']:,.0f} | {row['EMIS_LIST_SIZE']:,.0f} | "
                       f"{row['DIFFERENCE']:+,.0f} | {format_pct_diff(row['DIFFERENCE_PCT'], 1)} |\n")
            f.write("\n")
        
        f.write("---\n\n")
        
        # Issue 2: OLIDS vs EMIS (Main Developer Issue)
        f.write("## Issue 2: OLIDS vs EMIS (Main Developer Issue) ⚠️\n\n")
        f.write("**Expected:** Overall difference should be <0.5%\n\n")
        
        olids_excellent_pct = olids_analysis['excellent_match']/olids_analysis['total_practices']*100
        olids_good_pct = olids_analysis['good_match']/olids_analysis['total_practices']*100
        olids_total_good = olids_analysis['excellent_match'] + olids_analysis['good_match']
        olids_total_good_pct = olids_total_good/olids_analysis['total_practices']*100
        olids_total_issues = olids_analysis['minor_diff'] + olids_analysis['major_diff']
        olids_total_issues_pct = olids_total_issues/olids_analysis['total_practices']*100
        
        # Overall statistics
        f.write("### Overall Statistics\n\n")
        f.write(f"| Metric | Value |\n")
        f.write(f"|--------|-------|\n")
        f.write(f"| **Total Practices** | {olids_analysis['total_practices']} |\n")
        f.write(f"| **Total OLIDS REGULAR** | {olids_analysis['total_olids']:,.0f} |\n")
        f.write(f"| **Total EMIS** | {olids_analysis['total_emis']:,.0f} |\n")
        f.write(f"| **Overall Difference** | {olids_analysis['overall_diff']:+,.0f} |\n")
        f.write(f"| **Overall % Difference** | {olids_analysis['overall_diff_pct']:+.2f}% |\n\n")
        
        if abs(olids_analysis['overall_diff_pct']) > 0.5:
            f.write(f"⚠️ **WARNING:** Overall difference ({olids_analysis['overall_diff_pct']:+.2f}%) exceeds expected <0.5% threshold!\n\n")
        
        f.write("### Match Quality Breakdown\n\n")
        f.write(f"**Practices matching well:** {olids_total_good} ({olids_total_good_pct:.1f}%)\n")
        f.write(f"**Practices needing investigation:** {olids_total_issues} ({olids_total_issues_pct:.1f}%)\n\n")
        f.write("| Match Quality | Count | Percentage |\n")
        f.write("|---------------|-------|------------|\n")
        f.write(f"| ⭐ Excellent (<1%) | {olids_analysis['excellent_match']} | {olids_excellent_pct:.1f}% |\n")
        f.write(f"| ✅ Good (1-5%) | {olids_analysis['good_match']} | {olids_good_pct:.1f}% |\n")
        f.write(f"| ⚠️ Moderate Difference (5-20%) | {olids_analysis['minor_diff']} | {olids_analysis['minor_diff']/olids_analysis['total_practices']*100:.1f}% |\n")
        f.write(f"| ❌ Major Difference (≥20%) | {olids_analysis['major_diff']} | {olids_analysis['major_diff']/olids_analysis['total_practices']*100:.1f}% |\n\n")
        
        # Critical Issues Section
        critical_issues = olids_df[
            (olids_df['OLIDS_REGULAR_REGISTRATIONS'].notna()) & 
            (olids_df['EMIS_LIST_SIZE'].notna()) &
            (olids_df['DIFFERENCE_PCT'].abs() > 20)
        ].sort_values('DIFFERENCE_PCT')
        
        if len(critical_issues) > 0:
            f.write("### 🔴 Critical Issues: Practices with >20% Difference\n\n")
            f.write(f"**{len(critical_issues)} practices** have major discrepancies (>20% difference). These require investigation.\n\n")
            f.write("| Practice Code | Practice Name | OLIDS Regular | EMIS Count | Difference | % Difference |\n")
            f.write("|--------------|---------------|--------------|------------|------------|--------------|\n")
            for _, row in critical_issues.head(20).iterrows():
                practice_name = str(row.get('PRACTICE_NAME', '') or '').replace('|', '\\|')
                f.write(f"| {row['PRACTICE_CODE']} | {practice_name} | "
                       f"{row['OLIDS_REGULAR_REGISTRATIONS']:,.0f} | {row['EMIS_LIST_SIZE']:,.0f} | "
                       f"{row['DIFFERENCE']:+,.0f} | {format_pct_diff(row['DIFFERENCE_PCT'], 1)} |\n")
            if len(critical_issues) > 20:
                f.write(f"\n*... and {len(critical_issues) - 20} more practices*\n")
            f.write("\n")
        
        f.write("---\n\n")
        
        # Detailed Analysis: OLIDS vs EMIS - All Categories in Order (under Issue 2)
        f.write("### Detailed Analysis: OLIDS vs EMIS by Match Category\n\n")
        
        # OLIDS vs EMIS - Excellent Matches
        olids_excellent = olids_df[olids_df['MATCH_CATEGORY'] == 'Excellent Match (<1%)']
        if len(olids_excellent) > 0:
            f.write(f"### ⭐ OLIDS vs EMIS: Excellent Matches (<1%) - {len(olids_excellent)} practices\n\n")
            f.write("| Practice Code | Practice Name | OLIDS Regular | EMIS Count | Difference | % Difference |\n")
            f.write("|--------------|---------------|--------------|------------|------------|--------------|\n")
            for _, row in olids_excellent.sort_values('DIFFERENCE_PCT').iterrows():
                practice_name = str(row.get('PRACTICE_NAME', '') or '').replace('|', '\\|')
                f.write(f"| {row['PRACTICE_CODE']} | {practice_name} | "
                       f"{row['OLIDS_REGULAR_REGISTRATIONS']:,.0f} | {row['EMIS_LIST_SIZE']:,.0f} | "
                       f"{row['DIFFERENCE']:+,.0f} | {format_pct_diff(row['DIFFERENCE_PCT'], 2)} |\n")
            f.write("\n")
        
        # OLIDS vs EMIS - Good Matches
        olids_good = olids_df[olids_df['MATCH_CATEGORY'] == 'Good Match (1-5%)']
        if len(olids_good) > 0:
            f.write(f"### ✅ OLIDS vs EMIS: Good Matches (1-5%) - {len(olids_good)} practices\n\n")
            f.write("| Practice Code | Practice Name | OLIDS Regular | EMIS Count | Difference | % Difference |\n")
            f.write("|--------------|---------------|--------------|------------|------------|--------------|\n")
            for _, row in olids_good.sort_values('DIFFERENCE_PCT').iterrows():
                practice_name = str(row.get('PRACTICE_NAME', '') or '').replace('|', '\\|')
                f.write(f"| {row['PRACTICE_CODE']} | {practice_name} | "
                       f"{row['OLIDS_REGULAR_REGISTRATIONS']:,.0f} | {row['EMIS_LIST_SIZE']:,.0f} | "
                       f"{row['DIFFERENCE']:+,.0f} | {format_pct_diff(row['DIFFERENCE_PCT'], 1)} |\n")
            f.write("\n")
        
        # OLIDS vs EMIS - Moderate Differences
        olids_moderate = olids_df[olids_df['MATCH_CATEGORY'] == 'Moderate Difference (5-20%)']
        if len(olids_moderate) > 0:
            f.write(f"### ⚠️ OLIDS vs EMIS: Moderate Differences (5-20%) - {len(olids_moderate)} practices\n\n")
            f.write("| Practice Code | Practice Name | OLIDS Regular | EMIS Count | Difference | % Difference |\n")
            f.write("|--------------|---------------|--------------|------------|------------|--------------|\n")
            for _, row in olids_moderate.sort_values('DIFFERENCE_PCT').iterrows():
                practice_name = str(row.get('PRACTICE_NAME', '') or '').replace('|', '\\|')
                f.write(f"| {row['PRACTICE_CODE']} | {practice_name} | "
                       f"{row['OLIDS_REGULAR_REGISTRATIONS']:,.0f} | {row['EMIS_LIST_SIZE']:,.0f} | "
                       f"{row['DIFFERENCE']:+,.0f} | {format_pct_diff(row['DIFFERENCE_PCT'], 1)} |\n")
            f.write("\n")
        
        # OLIDS vs EMIS - Major Differences
        olids_major = olids_df[olids_df['MATCH_CATEGORY'] == 'Major Difference (±20%+)']
        if len(olids_major) > 0:
            f.write(f"### ❌ OLIDS vs EMIS: Major Differences (≥20%) - {len(olids_major)} practices\n\n")
            f.write("| Practice Code | Practice Name | OLIDS Regular | EMIS Count | Difference | % Difference |\n")
            f.write("|--------------|---------------|--------------|------------|------------|--------------|\n")
            for _, row in olids_major.sort_values('DIFFERENCE_PCT').iterrows():
                practice_name = str(row.get('PRACTICE_NAME', '') or '').replace('|', '\\|')
                f.write(f"| {row['PRACTICE_CODE']} | {practice_name} | "
                       f"{row['OLIDS_REGULAR_REGISTRATIONS']:,.0f} | {row['EMIS_LIST_SIZE']:,.0f} | "
                       f"{row['DIFFERENCE']:+,.0f} | {format_pct_diff(row['DIFFERENCE_PCT'], 1)} |\n")
            f.write("\n")
        
        f.write("---\n\n")
        
        # Overlap Analysis
        f.write("## Overlap Analysis: PDS vs EMIS Issues vs OLIDS vs EMIS Issues\n\n")
        f.write("This section examines whether practices with PDS vs EMIS discrepancies are the same practices that have OLIDS vs EMIS issues.\n\n")
        
        f.write("### Summary\n\n")
        f.write(f"| Metric | Count |\n")
        f.write(f"|--------|-------|\n")
        f.write(f"| **Practices with PDS vs EMIS issues** | {overlap_analysis['pds_issue_count']} |\n")
        f.write(f"| **Practices with OLIDS vs EMIS issues** | {overlap_analysis['olids_issue_count']} |\n")
        f.write(f"| **Practices with issues in BOTH comparisons** | {overlap_analysis['overlap_count']} |\n")
        f.write(f"| **Practices with PDS issues ONLY** | {overlap_analysis['pds_only_count']} |\n")
        f.write(f"| **Practices with OLIDS issues ONLY** | {overlap_analysis['olids_only_count']} |\n\n")
        
        if overlap_analysis['overlap_count'] > 0:
            overlap_pct_pds = (overlap_analysis['overlap_count'] / overlap_analysis['pds_issue_count'] * 100) if overlap_analysis['pds_issue_count'] > 0 else 0
            overlap_pct_olids = (overlap_analysis['overlap_count'] / overlap_analysis['olids_issue_count'] * 100) if overlap_analysis['olids_issue_count'] > 0 else 0
            f.write(f"**Overlap Analysis:**\n")
            f.write(f"- {overlap_pct_pds:.1f}% of practices with PDS vs EMIS issues also have OLIDS vs EMIS issues\n")
            f.write(f"- {overlap_pct_olids:.1f}% of practices with OLIDS vs EMIS issues also have PDS vs EMIS issues\n\n")
            
            f.write("### Practices with Issues in Both Comparisons\n\n")
            f.write("These practices have discrepancies in both PDS vs EMIS and OLIDS vs EMIS comparisons:\n\n")
            f.write("| Practice Code | Practice Name | PDS vs EMIS Category | PDS vs EMIS % Diff | OLIDS vs EMIS Category | OLIDS vs EMIS % Diff |\n")
            f.write("|--------------|---------------|----------------------|---------------------|------------------------|----------------------|\n")
            for _, row in overlap_analysis['overlap_df'].head(30).iterrows():
                practice_name = str(row.get('PRACTICE_NAME', '') or '').replace('|', '\\|')
                pds_diff_pct = format_pct_diff(row['DIFFERENCE_PCT'], 1)
                olids_diff_pct = format_pct_diff(row['OLIDS_DIFFERENCE_PCT'], 1) if pd.notna(row['OLIDS_DIFFERENCE_PCT']) else "—"
                f.write(f"| {row['PRACTICE_CODE']} | {practice_name} | {row['MATCH_CATEGORY']} | {pds_diff_pct} | "
                       f"{row['OLIDS_MATCH_CATEGORY']} | {olids_diff_pct} |\n")
            if len(overlap_analysis['overlap_df']) > 30:
                f.write(f"\n*... and {len(overlap_analysis['overlap_df']) - 30} more practices*\n")
            f.write("\n")
        
        f.write("---\n\n")
        
        # Summary tables for all practices
        f.write("## Appendix: All Practices Summary Tables\n\n")
        f.write("### PDS vs EMIS - All Practices\n\n")
        # Sort by category order, then by difference percentage
        pds_df_sorted = pds_df.copy()
        pds_df_sorted['_sort_order'] = pds_df_sorted['MATCH_CATEGORY'].apply(get_category_sort_order)
        pds_df_sorted = pds_df_sorted.sort_values(['_sort_order', 'DIFFERENCE_PCT']).drop(columns=['_sort_order'])
        f.write("| Indicator | Practice Code | Practice Name | PDS Count | EMIS Count | Difference | % Diff | Match Category |\n")
        f.write("|-----------|---------------|---------------|-----------|------------|------------|--------|---------------|\n")
        for _, row in pds_df_sorted.iterrows():
            practice_name = str(row.get('PRACTICE_NAME', '') or '').replace('|', '\\|')
            indicator = get_match_indicator(row['MATCH_CATEGORY'])
            pds_count = f"{row['PDS_MERGED_PERSONS']:,.0f}" if pd.notna(row['PDS_MERGED_PERSONS']) else "—"
            emis_count = f"{row['EMIS_LIST_SIZE']:,.0f}" if pd.notna(row['EMIS_LIST_SIZE']) else "—"
            diff = f"{row['DIFFERENCE']:+,.0f}" if pd.notna(row['DIFFERENCE']) else "—"
            diff_pct = format_pct_diff(row['DIFFERENCE_PCT'], 1)
            f.write(f"| {indicator} | {row['PRACTICE_CODE']} | {practice_name} | {pds_count} | {emis_count} | "
                   f"{diff} | {diff_pct} | {row['MATCH_CATEGORY']} |\n")
        f.write("\n")
        
        f.write("### OLIDS vs EMIS (REGULAR) - All Practices\n\n")
        # Sort by category order, then by difference percentage
        olids_df_sorted = olids_df.copy()
        olids_df_sorted['_sort_order'] = olids_df_sorted['MATCH_CATEGORY'].apply(get_category_sort_order)
        olids_df_sorted = olids_df_sorted.sort_values(['_sort_order', 'DIFFERENCE_PCT']).drop(columns=['_sort_order'])
        f.write("| Indicator | Practice Code | Practice Name | OLIDS Regular | EMIS Count | Difference | % Diff | Match Category |\n")
        f.write("|-----------|---------------|---------------|---------------|------------|------------|--------|---------------|\n")
        for _, row in olids_df_sorted.iterrows():
            practice_name = str(row.get('PRACTICE_NAME', '') or '').replace('|', '\\|')
            indicator = get_match_indicator(row['MATCH_CATEGORY'])
            olids_count = f"{row['OLIDS_REGULAR_REGISTRATIONS']:,.0f}" if pd.notna(row['OLIDS_REGULAR_REGISTRATIONS']) else "—"
            emis_count = f"{row['EMIS_LIST_SIZE']:,.0f}" if pd.notna(row['EMIS_LIST_SIZE']) else "—"
            diff = f"{row['DIFFERENCE']:+,.0f}" if pd.notna(row['DIFFERENCE']) else "—"
            diff_pct = format_pct_diff(row['DIFFERENCE_PCT'], 1)
            f.write(f"| {indicator} | {row['PRACTICE_CODE']} | {practice_name} | {olids_count} | {emis_count} | "
                   f"{diff} | {diff_pct} | {row['MATCH_CATEGORY']} |\n")
        f.write("\n")
        
        f.write("---\n\n")
        
        # OLIDS vs PDS Reference Section
        f.write("## Reference: OLIDS vs PDS (Original Comparison)\n\n")
        f.write("**Note:** This comparison uses REGULAR episode types only, matching EMIS list size definition (apples-to-apples).\n\n")
        
        if len(olids_analysis['olids_higher']) > 0:
            f.write(f"### Practices Where OLIDS REGULAR > EMIS ({len(olids_analysis['olids_higher'])} practices)\n\n")
            f.write("OLIDS has more REGULAR registrations than EMIS (source of truth). Comparison is apples-to-apples: REGULAR episodes only.\n\n")
            f.write("| Practice Code | Practice Name | OLIDS Regular | EMIS Count | Difference | % Difference | All OLIDS Episodes |\n")
            f.write("|--------------|---------------|--------------|------------|------------|--------------|-------------------|\n")
            for _, row in olids_analysis['olids_higher'].head(20).iterrows():
                practice_name = str(row.get('PRACTICE_NAME', '') or '').replace('|', '\\|')
                f.write(f"| {row['PRACTICE_CODE']} | {practice_name} | "
                       f"{row['OLIDS_REGULAR_REGISTRATIONS']:,.0f} | {row['EMIS_LIST_SIZE']:,.0f} | "
                       f"{row['DIFFERENCE']:+,.0f} | {row['DIFFERENCE_PCT']:+.1f}% | "
                       f"{row.get('OLIDS_REGISTERED_PATIENTS', 0) or 0:,.0f} |\n")
            f.write("\n")
        
        if len(olids_analysis['olids_lower']) > 0:
            f.write(f"### Practices Where OLIDS REGULAR < EMIS ({len(olids_analysis['olids_lower'])} practices)\n\n")
            f.write("OLIDS has fewer REGULAR registrations than EMIS (source of truth). Comparison is apples-to-apples: REGULAR episodes only.\n\n")
            f.write("| Practice Code | Practice Name | OLIDS Regular | EMIS Count | Difference | % Difference | All OLIDS Episodes |\n")
            f.write("|--------------|---------------|--------------|------------|------------|--------------|-------------------|\n")
            for _, row in olids_analysis['olids_lower'].head(20).iterrows():
                practice_name = str(row.get('PRACTICE_NAME', '') or '').replace('|', '\\|')
                f.write(f"| {row['PRACTICE_CODE']} | {practice_name} | "
                       f"{row['OLIDS_REGULAR_REGISTRATIONS']:,.0f} | {row['EMIS_LIST_SIZE']:,.0f} | "
                       f"{row['DIFFERENCE']:+,.0f} | {row['DIFFERENCE_PCT']:+.1f}% | "
                       f"{row.get('OLIDS_REGISTERED_PATIENTS', 0) or 0:,.0f} |\n")
            f.write("\n")
        
        f.write("---\n\n")
        
        # OLIDS vs PDS Analysis (Reference - Original Comparison)
        f.write("## OLIDS vs PDS Analysis (Reference - Original Comparison)\n\n")
        f.write("**Context:** This was the original comparison used by developers. Included here for reference.\n\n")
        f.write(f"**Note:** Since PDS aligns well with EMIS ({pds_total_good} practices, {pds_total_good_pct:.1f}% good/excellent), ")
        f.write("differences between OLIDS and PDS are likely similar to differences between OLIDS and EMIS. ")
        f.write("However, **OLIDS vs EMIS is the definitive comparison** since EMIS is the authoritative source.\n\n")
        f.write("**Caveat:** PDS includes regular registrations only, while this comparison uses OLIDS REGULAR only.\n\n")
        
        if len(olids_pds_analysis['olids_higher']) > 0:
            f.write(f"### Practices Where OLIDS REGULAR > PDS ({len(olids_pds_analysis['olids_higher'])} practices)\n\n")
            f.write("Comparing OLIDS REGULAR registrations to PDS (regular registrations only).\n\n")
            f.write("| Practice Code | Practice Name | OLIDS Regular | PDS Count | Difference | % Difference | All OLIDS Episodes |\n")
            f.write("|--------------|---------------|--------------|-----------|------------|--------------|-------------------|\n")
            for _, row in olids_pds_analysis['olids_higher'].head(20).iterrows():
                practice_name = str(row.get('PRACTICE_NAME', '') or '').replace('|', '\\|')
                f.write(f"| {row['PRACTICE_CODE']} | {practice_name} | "
                       f"{row['OLIDS_REGULAR_REGISTRATIONS']:,.0f} | {row['PDS_MERGED_PERSONS']:,.0f} | "
                       f"{row['diff']:+,.0f} | {format_pct_diff(row['diff_pct'], 1)} | "
                       f"{row.get('OLIDS_REGISTERED_PATIENTS', 0) or 0:,.0f} |\n")
            f.write("\n")
        
        if len(olids_pds_analysis['olids_lower']) > 0:
            f.write(f"### Practices Where OLIDS REGULAR < PDS ({len(olids_pds_analysis['olids_lower'])} practices)\n\n")
            f.write("Comparing OLIDS REGULAR registrations to PDS (regular registrations only).\n\n")
            f.write("| Practice Code | Practice Name | OLIDS Regular | PDS Count | Difference | % Difference | All OLIDS Episodes |\n")
            f.write("|--------------|---------------|--------------|-----------|------------|--------------|-------------------|\n")
            for _, row in olids_pds_analysis['olids_lower'].head(20).iterrows():
                practice_name = str(row.get('PRACTICE_NAME', '') or '').replace('|', '\\|')
                f.write(f"| {row['PRACTICE_CODE']} | {practice_name} | "
                       f"{row['OLIDS_REGULAR_REGISTRATIONS']:,.0f} | {row['PDS_MERGED_PERSONS']:,.0f} | "
                       f"{row['diff']:+,.0f} | {format_pct_diff(row['diff_pct'], 1)} | "
                       f"{row.get('OLIDS_REGISTERED_PATIENTS', 0) or 0:,.0f} |\n")
            f.write("\n")
        
        f.write("---\n\n")
        f.write("## Notes for Developers\n\n")
        f.write("### Target Date: 2025-11-04\n\n")
        f.write("All comparisons use **2025-11-04** as the target date. This date was chosen to match the EMIS extract date.\n\n")
        f.write("**Why this date matters:**\n")
        f.write("- EMIS list size extract was manually generated on 2025-11-04\n")
        f.write("- PDS and OLIDS data are filtered to show registrations/episodes that were **active** on this date\n")
        f.write("- This ensures all three data sources represent the same point in time\n\n")
        f.write("### Data Sources\n\n")
        f.write("#### EMIS List Size (Source of Truth)\n")
        f.write("- **Source:** Manual extract from EMIS system\n")
        f.write("- **Extract Date:** 2025-11-04\n")
        f.write("- **Table:** `DATA_LAB_OLIDS_UAT.REFERENCE.EMIS_LIST_SIZE_2025_11_04`\n")
        f.write("- **Definition:** Regular list size per practice as reported by EMIS\n")
        f.write("- **Note:** This is considered the authoritative source for regular patient counts\n\n")
        f.write("#### PDS Registry\n")
        f.write("- **Source:** PDS (Personal Demographics Service) registry\n")
        f.write("- **Target Date:** 2025-11-04\n")
        f.write("- **Filtering:**\n")
        f.write("  - Includes all registrations active on target date\n")
        f.write("  - Excludes deceased patients (death status/date filters)\n")
        f.write("  - Excludes patients with reason for removal\n")
        f.write("  - Uses merged person records (handles NHS number changes)\n")
        f.write("  - Filters by business effective dates\n")
        f.write("- **Count:** `PDS_MERGED_PERSONS` - distinct merged NHS numbers per practice\n")
        f.write("- **Note:** PDS is the national registry that tracks regular patient registrations only. Temporary registrations are handled locally by practices and are not reported to PDS. Therefore, PDS represents regular registrations only, making PDS vs EMIS an apples-to-apples comparison.\n\n")
        f.write("#### OLIDS Source Data\n")
        f.write("- **Source:** OLIDS Episode of Care data\n")
        f.write("- **Target Date:** 2025-11-04\n")
        f.write("- **Filtering:**\n")
        f.write("  - Includes episodes active on target date (start date ≤ target date, end date > target date or NULL)\n")
        f.write("  - Excludes deceased patients (death status/date filters)\n")
        f.write("  - Uses most recent episode per patient per practice (QUALIFY ROW_NUMBER)\n")
        f.write("  - Filters by episode type (REGULAR for apples-to-apples comparison)\n")
        f.write("- **Count:** `TYPE_REGULAR` - distinct patients with REGULAR episode type per practice\n")
        f.write("- **Note:** OLIDS has multiple episode types; only REGULAR is used for comparison with EMIS\n\n")
        f.write("### Comparison Methodology\n\n")
        f.write("#### OLIDS vs EMIS (Primary Comparison)\n")
        f.write("- **Purpose:** Validate OLIDS REGULAR episode counts against authoritative EMIS source\n")
        f.write("- **Methodology:**\n")
        f.write("  - OLIDS: REGULAR episode types only (apples-to-apples comparison)\n")
        f.write("  - EMIS: Regular list size (as defined by EMIS)\n")
        f.write("  - Both filtered to same target date (2025-11-04)\n")
        f.write("- **Expected Difference:** <0.5% overall\n")
        f.write("- **Status:** ⚠️ Current difference is -12.29% (exceeds threshold)\n\n")
        f.write("#### PDS vs EMIS (Yardstick Validation)\n")
        f.write("- **Purpose:** Validate PDS as a yardstick for comparison\n")
        f.write("- **Methodology:**\n")
        f.write("  - PDS: Regular registrations only (national registry - temporary registrations not included)\n")
        f.write("  - EMIS: Regular list size only\n")
        f.write("  - Both filtered to same target date (2025-11-04)\n")
        f.write("- **Note:** This is an apples-to-apples comparison (both represent regular registrations only), which makes the discrepancies more concerning\n")
        f.write(f"- **Status:** ⚠️ PDS and EMIS broadly agree overall ({pds_analysis['overall_diff_pct']:+.2f}% difference), but don't agree for some practices (29.1% have discrepancies)\n\n")
        f.write("#### OLIDS vs PDS (Reference Comparison)\n")
        f.write("- **Purpose:** Included for reference (original comparison used by developers)\n")
        f.write("- **Methodology:**\n")
        f.write("  - OLIDS: REGULAR episode types only\n")
        f.write("  - PDS: Regular registrations only\n")
        f.write("- **Note:** Since PDS aligns reasonably well with EMIS (70.9% good/excellent), differences between OLIDS and PDS are likely similar to differences between OLIDS and EMIS. However, **OLIDS vs EMIS is the definitive comparison** since EMIS is the authoritative source.\n\n")
        f.write("### Key Issues\n")
        f.write(f"1. **PDS vs EMIS:** Yardstick validation issue - PDS and EMIS broadly agree overall ({pds_analysis['overall_diff_pct']:+.2f}% difference), but don't agree for some practices\n")
        f.write("2. **OLIDS vs EMIS:** Main developer issue - OLIDS REGULAR should differ by <0.5% overall, but current difference is ")
        f.write(f"{olids_analysis['overall_diff_pct']:+.2f}%\n\n")
        f.write("### Debugging Focus\n")
        f.write("Focus investigation on practices where OLIDS REGULAR differs significantly from EMIS. ")
        f.write("Start with practices showing >20% difference (Critical Issues section).\n")


def main():
    """Execute analysis and generate report."""
    print("="*80)
    print("EMIS Comparison Analysis")
    print("="*80)
    
    # Read SQL files
    print("\nReading SQL files...")
    pds_emis_use, pds_emis_query = parse_sql_file(PDS_EMIS_QUERY_FILE)
    olids_emis_use, olids_emis_query = parse_sql_file(OLIDS_EMIS_QUERY_FILE)
    print("✓ SQL files read")
    
    # Connect to Snowflake
    print("\nConnecting to Snowflake...")
    connection_parameters = {
        "account": ACCOUNT,
        "user": USER,
        "authenticator": "externalbrowser",
        "warehouse": WAREHOUSE,
        "role": ROLE
    }
    
    session = Session.builder.configs(connection_parameters).create()
    print("✓ Connected")
    
    try:
        # Execute queries
        pds_emis_df = execute_query(session, pds_emis_use, pds_emis_query, "PDS vs EMIS")
        olids_emis_df = execute_query(session, olids_emis_use, olids_emis_query, "OLIDS vs EMIS")
        
        # Analyse results
        print("\nAnalysing results...")
        pds_analysis = analyse_pds_vs_emis(pds_emis_df)
        olids_analysis = analyse_olids_vs_emis(olids_emis_df)
        olids_pds_analysis = analyse_olids_vs_pds(pds_emis_df, olids_emis_df)
        overlap_analysis = analyse_overlap(pds_analysis, olids_analysis)
        
        # Generate report
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_file = OUTPUT_DIR / f"emis_comparison_analysis_{timestamp}.md"
        generate_markdown_report(pds_analysis, olids_analysis, olids_pds_analysis, overlap_analysis, report_file)
        
        print(f"\n✓ Analysis complete")
        print(f"✓ Report saved to: {report_file}")
        
        # Save CSV files for further analysis
        pds_emis_df.to_csv(OUTPUT_DIR / "pds_emis_comparison.csv", index=False)
        olids_emis_df.to_csv(OUTPUT_DIR / "olids_emis_comparison.csv", index=False)
        olids_pds_analysis['comparison'].to_csv(OUTPUT_DIR / "olids_pds_comparison.csv", index=False)
        print(f"✓ CSV files saved to: {OUTPUT_DIR}")
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        raise
    finally:
        session.close()
        print("\nConnection closed")


if __name__ == '__main__':
    main()

