"""
Simplified OLIDS vs EMIS comparison analysis with acceptance criteria.
Generates a markdown report focusing on acceptance criteria validation.

Acceptance Criteria:
1. <1% variance in aggregate across all practices
2. <2% variance OR fewer than five persons difference per practice (whichever is greater)
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

OLIDS_EMIS_QUERY_FILE = SCRIPT_DIR / "registrations_comparison_of_methods/olids_emis_comparison_native_query.sql"
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
        '�': "'",
        '\x92': "'",
        '\x93': '"',
        '\x94': '"',
        '\u2019': "'",
        '\u2018': "'",
        '\u201C': '"',
        '\u201D': '"',
    }
    for old, new in replacements.items():
        text_str = text_str.replace(old, new)
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


def categorise_practices(df):
    """Categorise practices based on acceptance criteria."""
    # Acceptance criteria: <2% variance OR fewer than five persons difference
    df = df.copy()
    
    # Calculate absolute difference
    df['abs_difference'] = df['DIFFERENCE'].abs()
    df['abs_difference_pct'] = df['DIFFERENCE_PCT'].abs()
    
    # Determine if practice meets acceptance criteria
    # Meets if: abs_difference_pct < 2 OR abs_difference < 5
    df['meets_criteria'] = (df['abs_difference_pct'] < 2) | (df['abs_difference'] < 5)
    
    # Categorise practices
    def get_category(row):
        if pd.isna(row['OLIDS_REGULAR_REGISTRATIONS']) or pd.isna(row['EMIS_LIST_SIZE']):
            return 'Missing Data'
        
        abs_pct = row['abs_difference_pct']
        
        if row['meets_criteria']:
            return 'Meets Criteria (<2% or <5 persons)'
        elif abs_pct < 5:
            return '2-5% Variance'
        elif abs_pct < 20:
            return '5-20% Variance'
        else:
            return '20%+ Variance'
    
    df['acceptance_category'] = df.apply(get_category, axis=1)
    
    return df


def analyse_olids_vs_emis(df):
    """Analyse OLIDS vs EMIS with acceptance criteria."""
    # Filter to valid comparisons only
    valid_comparison = df[
        (df['OLIDS_REGULAR_REGISTRATIONS'].notna()) & 
        (df['EMIS_LIST_SIZE'].notna())
    ].copy()
    
    total_olids = valid_comparison['OLIDS_REGULAR_REGISTRATIONS'].sum()
    total_emis = valid_comparison['EMIS_LIST_SIZE'].sum()
    overall_diff = total_olids - total_emis
    overall_diff_pct = (overall_diff / total_emis * 100) if total_emis > 0 else 0
    
    # Categorise practices
    categorised_df = categorise_practices(valid_comparison)
    
    # Count by category
    category_counts = categorised_df['acceptance_category'].value_counts().to_dict()
    
    # Check aggregate acceptance criteria (<1% variance)
    aggregate_meets_criteria = abs(overall_diff_pct) < 1
    
    analysis = {
        'total_practices': len(df),
        'valid_comparisons': len(valid_comparison),
        'total_olids': total_olids,
        'total_emis': total_emis,
        'overall_diff': overall_diff,
        'overall_diff_pct': overall_diff_pct,
        'aggregate_meets_criteria': aggregate_meets_criteria,
        'category_counts': category_counts,
        'df': categorised_df
    }
    
    return analysis


def format_pct_diff(value, decimals=1):
    """Format percentage difference, handling NaN and infinity."""
    if pd.isna(value) or np.isinf(value):
        return "—"
    return f"{value:+.{decimals}f}%"


def generate_markdown_report(analysis, output_file):
    """Generate a simplified markdown report focused on acceptance criteria."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    df = analysis['df'].drop_duplicates(subset=['PRACTICE_CODE'], keep='first')
    
    # Calculate summary statistics
    total_valid = analysis['valid_comparisons']
    meets_count = analysis['category_counts'].get('Meets Criteria (<2% or <5 persons)', 0)
    variance_2_5 = analysis['category_counts'].get('2-5% Variance', 0)
    variance_5_20 = analysis['category_counts'].get('5-20% Variance', 0)
    variance_20_plus = analysis['category_counts'].get('20%+ Variance', 0)
    missing_data = analysis['category_counts'].get('Missing Data', 0)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        # Header
        f.write("# 📊 OLIDS vs EMIS Acceptance Criteria Report\n\n")
        f.write(f"**Generated:** {timestamp}  \n")
        f.write("**Source of Truth:** EMIS List Size (2025-11-04)  \n")
        f.write("**Target Date:** 2025-11-04\n\n")
        
        f.write("---\n\n")
        
        # Executive Summary Box
        f.write("## 📋 Executive Summary\n\n")
        
        # Aggregate status box
        if analysis['aggregate_meets_criteria']:
            f.write(f"### ✅ Aggregate Acceptance Criteria: **PASS**\n\n")
            f.write(f"> **Overall Variance:** {analysis['overall_diff_pct']:+.2f}%  \n")
            f.write(f"> **Status:** Meets requirement (<1% variance)\n\n")
        else:
            f.write(f"### ❌ Aggregate Acceptance Criteria: **FAIL**\n\n")
            f.write(f"> **Overall Variance:** {analysis['overall_diff_pct']:+.2f}%  \n")
            f.write(f"> **Status:** Exceeds 1% threshold\n\n")
        
        # Per-practice summary box
        meets_pct = (meets_count / total_valid * 100) if total_valid > 0 else 0
        f.write(f"### 📈 Per-Practice Summary\n\n")
        f.write(f"> **Practices Meeting Criteria:** {meets_count} of {total_valid} ({meets_pct:.1f}%)  \n")
        f.write(f"> **Practices Requiring Review:** {variance_2_5 + variance_5_20 + variance_20_plus} ({((variance_2_5 + variance_5_20 + variance_20_plus) / total_valid * 100):.1f}%)\n\n")
        
        f.write("---\n\n")
        
        # Acceptance Criteria Section
        f.write("## 📐 Acceptance Criteria\n\n")
        f.write("The following criteria are used to validate OLIDS REGULAR registration counts against EMIS:\n\n")
        f.write("1. **Aggregate Variance:** <1% variance across all practices combined\n")
        f.write("2. **Per-Practice Variance:** <2% variance **OR** fewer than five persons difference (whichever is greater)\n\n")
        
        f.write("---\n\n")
        
        # Overall Statistics
        f.write("## 📊 Overall Statistics\n\n")
        f.write(f"| Metric | Value |\n")
        f.write(f"|--------|-------|\n")
        f.write(f"| **Total Practices** | {analysis['total_practices']} |\n")
        f.write(f"| **Valid Comparisons** | {analysis['valid_comparisons']} |\n")
        f.write(f"| **Total OLIDS REGULAR** | {analysis['total_olids']:,.0f} |\n")
        f.write(f"| **Total EMIS** | {analysis['total_emis']:,.0f} |\n")
        f.write(f"| **Overall Difference** | {analysis['overall_diff']:+,.0f} |\n")
        f.write(f"| **Overall % Difference** | {analysis['overall_diff_pct']:+.2f}% |\n\n")
        
        f.write("---\n\n")
        
        # Per-Practice Acceptance Criteria Summary
        # Calculate total signed difference for each category (can be negative)
        meets_df = df[df['acceptance_category'] == 'Meets Criteria (<2% or <5 persons)']
        variance_2_5_df = df[df['acceptance_category'] == '2-5% Variance']
        variance_5_20_df = df[df['acceptance_category'] == '5-20% Variance']
        variance_20_plus_df = df[df['acceptance_category'] == '20%+ Variance']
        
        # Sum of actual signed differences (not absolute)
        meets_diff = meets_df['DIFFERENCE'].sum() if len(meets_df) > 0 else 0
        variance_2_5_diff = variance_2_5_df['DIFFERENCE'].sum() if len(variance_2_5_df) > 0 else 0
        variance_5_20_diff = variance_5_20_df['DIFFERENCE'].sum() if len(variance_5_20_df) > 0 else 0
        variance_20_plus_diff = variance_20_plus_df['DIFFERENCE'].sum() if len(variance_20_plus_df) > 0 else 0
        
        # For percentage calculation, use absolute values to show contribution to total variance magnitude
        total_abs_diff = abs(meets_diff) + abs(variance_2_5_diff) + abs(variance_5_20_diff) + abs(variance_20_plus_diff)
        
        meets_diff_pct = (abs(meets_diff) / total_abs_diff * 100) if total_abs_diff > 0 else 0
        variance_2_5_diff_pct = (abs(variance_2_5_diff) / total_abs_diff * 100) if total_abs_diff > 0 else 0
        variance_5_20_diff_pct = (abs(variance_5_20_diff) / total_abs_diff * 100) if total_abs_diff > 0 else 0
        variance_20_plus_diff_pct = (abs(variance_20_plus_diff) / total_abs_diff * 100) if total_abs_diff > 0 else 0
        
        f.write("## 📋 Per-Practice Acceptance Criteria Breakdown\n\n")
        f.write("| Category | Count | % of Practices | Total Difference | % of Total Difference |\n")
        f.write("|----------|-------|----------------|-------------------|----------------------|\n")
        
        f.write(f"| ✅ **Meets Criteria** (<2% or <5 persons) | **{meets_count}** | **{meets_count/total_valid*100:.1f}%** | {meets_diff:+,.0f} | {meets_diff_pct:.1f}% |\n")
        f.write(f"| ⚠️ 2-5% Variance | {variance_2_5} | {variance_2_5/total_valid*100:.1f}% | {variance_2_5_diff:+,.0f} | {variance_2_5_diff_pct:.1f}% |\n")
        f.write(f"| ⚠️ 5-20% Variance | {variance_5_20} | {variance_5_20/total_valid*100:.1f}% | {variance_5_20_diff:+,.0f} | {variance_5_20_diff_pct:.1f}% |\n")
        f.write(f"| ❌ 20%+ Variance | {variance_20_plus} | {variance_20_plus/total_valid*100:.1f}% | {variance_20_plus_diff:+,.0f} | {variance_20_plus_diff_pct:.1f}% |\n")
        if missing_data > 0:
            f.write(f"| ➖ Missing Data | {missing_data} | {missing_data/analysis['total_practices']*100:.1f}% | — | — |\n")
        
        f.write("\n")
        
        f.write("---\n\n")
        
        # Practices Meeting Criteria
        meets_criteria_df = df[df['acceptance_category'] == 'Meets Criteria (<2% or <5 persons)'].copy()
        if len(meets_criteria_df) > 0:
            f.write(f"## ✅ Practices Meeting Acceptance Criteria\n\n")
            f.write(f"**{len(meets_criteria_df)} practices** meet the acceptance criteria: <2% variance OR fewer than five persons difference.\n\n")
            f.write("| Practice Code | Practice Name | OLIDS Regular | EMIS Count | Difference | % Difference |\n")
            f.write("|--------------|---------------|--------------|------------|------------|--------------|\n")
            for _, row in meets_criteria_df.sort_values('PRACTICE_CODE').iterrows():
                practice_name = str(row.get('PRACTICE_NAME', '') or '').replace('|', '\\|')
                f.write(f"| `{row['PRACTICE_CODE']}` | {practice_name} | "
                       f"{row['OLIDS_REGULAR_REGISTRATIONS']:,.0f} | {row['EMIS_LIST_SIZE']:,.0f} | "
                       f"{row['DIFFERENCE']:+,.0f} | {format_pct_diff(row['DIFFERENCE_PCT'], 2)} |\n")
            f.write("\n")
        
        f.write("---\n\n")
        
        # Practices Not Meeting Criteria - 2-5% Variance
        variance_2_5_df = df[df['acceptance_category'] == '2-5% Variance'].copy()
        if len(variance_2_5_df) > 0:
            f.write(f"## ⚠️ Practices with 2-5% Variance\n\n")
            f.write(f"**{len(variance_2_5_df)} practices** have variance between 2-5%. These require review.\n\n")
            f.write("| Practice Code | Practice Name | OLIDS Regular | EMIS Count | Difference | % Difference |\n")
            f.write("|--------------|---------------|--------------|------------|------------|--------------|\n")
            for _, row in variance_2_5_df.sort_values('abs_difference_pct', ascending=False).iterrows():
                practice_name = str(row.get('PRACTICE_NAME', '') or '').replace('|', '\\|')
                f.write(f"| `{row['PRACTICE_CODE']}` | {practice_name} | "
                       f"{row['OLIDS_REGULAR_REGISTRATIONS']:,.0f} | {row['EMIS_LIST_SIZE']:,.0f} | "
                       f"{row['DIFFERENCE']:+,.0f} | {format_pct_diff(row['DIFFERENCE_PCT'], 1)} |\n")
            f.write("\n")
        
        # Practices Not Meeting Criteria - 5-20% Variance
        variance_5_20_df = df[df['acceptance_category'] == '5-20% Variance'].copy()
        if len(variance_5_20_df) > 0:
            f.write(f"## ⚠️ Practices with 5-20% Variance\n\n")
            f.write(f"**{len(variance_5_20_df)} practices** have variance between 5-20%. These require investigation.\n\n")
            f.write("| Practice Code | Practice Name | OLIDS Regular | EMIS Count | Difference | % Difference |\n")
            f.write("|--------------|---------------|--------------|------------|------------|--------------|\n")
            for _, row in variance_5_20_df.sort_values('abs_difference_pct', ascending=False).iterrows():
                practice_name = str(row.get('PRACTICE_NAME', '') or '').replace('|', '\\|')
                f.write(f"| `{row['PRACTICE_CODE']}` | {practice_name} | "
                       f"{row['OLIDS_REGULAR_REGISTRATIONS']:,.0f} | {row['EMIS_LIST_SIZE']:,.0f} | "
                       f"{row['DIFFERENCE']:+,.0f} | {format_pct_diff(row['DIFFERENCE_PCT'], 1)} |\n")
            f.write("\n")
        
        # Practices Not Meeting Criteria - 20%+ Variance
        variance_20_plus_df = df[df['acceptance_category'] == '20%+ Variance'].copy()
        if len(variance_20_plus_df) > 0:
            f.write(f"## ❌ Practices with 20%+ Variance\n\n")
            f.write(f"**{len(variance_20_plus_df)} practices** have variance ≥20%. These require **immediate investigation**.\n\n")
            f.write("| Practice Code | Practice Name | OLIDS Regular | EMIS Count | Difference | % Difference |\n")
            f.write("|--------------|---------------|--------------|------------|------------|--------------|\n")
            for _, row in variance_20_plus_df.sort_values('abs_difference_pct', ascending=False).iterrows():
                practice_name = str(row.get('PRACTICE_NAME', '') or '').replace('|', '\\|')
                f.write(f"| `{row['PRACTICE_CODE']}` | {practice_name} | "
                       f"{row['OLIDS_REGULAR_REGISTRATIONS']:,.0f} | {row['EMIS_LIST_SIZE']:,.0f} | "
                       f"{row['DIFFERENCE']:+,.0f} | {format_pct_diff(row['DIFFERENCE_PCT'], 1)} |\n")
            f.write("\n")
        
        # Missing Data
        missing_df = df[df['acceptance_category'] == 'Missing Data'].copy()
        if len(missing_df) > 0:
            f.write("## ➖ Practices with Missing Data\n\n")
            f.write(f"**{len(missing_df)} practices** have missing OLIDS or EMIS data.\n\n")
            f.write("| Practice Code | Practice Name | OLIDS Regular | EMIS Count |\n")
            f.write("|--------------|---------------|--------------|------------|\n")
            for _, row in missing_df.sort_values('PRACTICE_CODE').iterrows():
                practice_name = str(row.get('PRACTICE_NAME', '') or '').replace('|', '\\|')
                olids_val = f"{row['OLIDS_REGULAR_REGISTRATIONS']:,.0f}" if pd.notna(row['OLIDS_REGULAR_REGISTRATIONS']) else "—"
                emis_val = f"{row['EMIS_LIST_SIZE']:,.0f}" if pd.notna(row['EMIS_LIST_SIZE']) else "—"
                f.write(f"| `{row['PRACTICE_CODE']}` | {practice_name} | {olids_val} | {emis_val} |\n")
            f.write("\n")
        
        f.write("---\n\n")
        
        # Appendix: All Practices Summary
        f.write("## 📑 Appendix: Complete Practice Listing\n\n")
        f.write("Complete listing of all practices sorted by acceptance category (best to worst) and variance.\n\n")
        f.write("| Practice Code | Practice Name | OLIDS Regular | EMIS Count | Difference | % Difference | Category |\n")
        f.write("|--------------|---------------|--------------|------------|------------|--------------|----------|\n")
        
        # Define category sort order (best to worst)
        category_order = {
            'Meets Criteria (<2% or <5 persons)': 1,
            '2-5% Variance': 2,
            '5-20% Variance': 3,
            '20%+ Variance': 4,
            'Missing Data': 5
        }
        
        # Sort: by category order first, then by absolute difference percentage within category
        df_sorted = df.copy()
        df_sorted['_category_order'] = df_sorted['acceptance_category'].map(category_order).fillna(99)
        df_sorted = df_sorted.sort_values(['_category_order', 'abs_difference_pct'], ascending=[True, False])
        df_sorted = df_sorted.drop(columns=['_category_order'])
        
        for _, row in df_sorted.iterrows():
            practice_name = str(row.get('PRACTICE_NAME', '') or '').replace('|', '\\|')
            olids_val = f"{row['OLIDS_REGULAR_REGISTRATIONS']:,.0f}" if pd.notna(row['OLIDS_REGULAR_REGISTRATIONS']) else "—"
            emis_val = f"{row['EMIS_LIST_SIZE']:,.0f}" if pd.notna(row['EMIS_LIST_SIZE']) else "—"
            diff = f"{row['DIFFERENCE']:+,.0f}" if pd.notna(row['DIFFERENCE']) else "—"
            diff_pct = format_pct_diff(row['DIFFERENCE_PCT'], 1)
            category = row['acceptance_category']
            f.write(f"| `{row['PRACTICE_CODE']}` | {practice_name} | {olids_val} | {emis_val} | "
                   f"{diff} | {diff_pct} | {category} |\n")
        
        f.write("\n")
        
        f.write("---\n\n")
        f.write("*Report generated by OLIDS vs EMIS Acceptance Criteria Analysis*\n")


def main():
    """Execute analysis and generate report."""
    print("="*80)
    print("OLIDS vs EMIS Acceptance Criteria Analysis")
    print("="*80)
    
    # Read SQL file
    print("\nReading SQL file...")
    olids_emis_use, olids_emis_query = parse_sql_file(OLIDS_EMIS_QUERY_FILE)
    print("✓ SQL file read")
    
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
        # Execute query
        olids_emis_df = execute_query(session, olids_emis_use, olids_emis_query, "OLIDS vs EMIS")
        
        # Analyse results
        print("\nAnalysing results...")
        analysis = analyse_olids_vs_emis(olids_emis_df)
        
        # Generate report
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_file = OUTPUT_DIR / f"olids_emis_acceptance_{timestamp}.md"
        generate_markdown_report(analysis, report_file)
        
        print(f"\n✓ Analysis complete")
        print(f"✓ Report saved to: {report_file}")
        
        # Save CSV file for further analysis
        analysis['df'].to_csv(OUTPUT_DIR / "olids_emis_acceptance.csv", index=False)
        print(f"✓ CSV file saved to: {OUTPUT_DIR}")
        
        # Print summary
        print("\n" + "="*80)
        print("Summary")
        print("="*80)
        print(f"Aggregate Variance: {analysis['overall_diff_pct']:+.2f}%")
        if analysis['aggregate_meets_criteria']:
            print("✅ Aggregate acceptance criteria: PASS")
        else:
            print("❌ Aggregate acceptance criteria: FAIL")
        print(f"\nPractices meeting criteria: {analysis['category_counts'].get('Meets Criteria (<2% or <5 persons)', 0)}")
        print(f"Practices with 2-5% variance: {analysis['category_counts'].get('2-5% Variance', 0)}")
        print(f"Practices with 5-20% variance: {analysis['category_counts'].get('5-20% Variance', 0)}")
        print(f"Practices with 20%+ variance: {analysis['category_counts'].get('20%+ Variance', 0)}")
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        raise
    finally:
        session.close()
        print("\nConnection closed")


if __name__ == '__main__':
    main()

