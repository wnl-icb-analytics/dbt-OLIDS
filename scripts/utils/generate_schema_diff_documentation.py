"""
Generate schema difference documentation comparing current stable layer with git main branch.
Creates markdown handover document for other dbt project.
"""

import re
import subprocess
from pathlib import Path
from collections import defaultdict
from typing import Dict, Set, Tuple, List, Optional


def extract_columns_from_sql(content: str) -> List[str]:
    """
    Extract column names from SQL SELECT statement.
    Returns list of column names in order they appear.
    """
    # Find SELECT statement (handle both SELECT and select)
    select_match = re.search(r'select\s+(.*?)\s+from', content, re.DOTALL | re.IGNORECASE)
    if not select_match:
        return []
    
    select_clause = select_match.group(1)
    
    # Split by newline first (stable models typically have one column per line)
    # Then split by comma for columns on same line
    lines = []
    for line in select_clause.split('\n'):
        line = line.strip()
        if not line or line.startswith('{%'):
            continue
        # Split by comma if multiple columns on same line
        for part in line.split(','):
            part = part.strip()
            if part:
                lines.append(part)
    
    columns = []
    for line in lines:
        if not line:
            continue
        
        # Remove comments
        line = re.sub(r'--.*$', '', line).strip()
        if not line:
            continue
        
        # Skip jinja templating
        if '{{' in line or '}}' in line:
            continue
        
        # Extract column name (handle aliases)
        # Pattern: column_name or column_name AS alias or alias.column_name
        # Handle: src.column_name, column_name AS alias, just column_name
        alias_match = re.search(r'\s+as\s+(\w+)', line, re.IGNORECASE)
        if alias_match:
            # Has alias, use alias
            col_name = alias_match.group(1).lower()
        else:
            # No alias, extract the column name
            # Remove table prefix if present (e.g., src.column_name -> column_name)
            # Get the last word (column name)
            col_match = re.search(r'(\w+)\s*$', line.strip())
            if col_match:
                col_name = col_match.group(1).lower()
            else:
                continue
        
        # Skip if it's a function call (but allow simple column references)
        if '(' in line and ')' in line and line.count('(') == line.count(')'):
            # Might be a function, skip for now
            continue
            
        columns.append(col_name)
    
    return columns


def get_current_stable_schemas(stable_dir: Path) -> Dict[str, List[str]]:
    """
    Extract column schemas from current stable models.
    Returns dict mapping model_name -> list of column names.
    """
    schemas = {}
    
    for sql_file in stable_dir.glob("stable_*.sql"):
        model_name = sql_file.stem  # e.g., stable_observation
        
        with open(sql_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        columns = extract_columns_from_sql(content)
        if columns:
            schemas[model_name] = columns
    
    return schemas


def get_git_main_stable_schemas(stable_dir: Path) -> Dict[str, List[str]]:
    """
    Extract column schemas from git main branch stable models.
    Returns dict mapping model_name -> list of column names.
    """
    schemas = {}
    
    # Get list of stable model files
    current_files = list(stable_dir.glob("stable_*.sql"))
    
    # Resolve to absolute paths
    cwd = Path.cwd().resolve()
    
    for sql_file in current_files:
        model_name = sql_file.stem
        # Convert to relative path using forward slashes for git
        sql_file_abs = sql_file.resolve()
        relative_path = sql_file_abs.relative_to(cwd).as_posix()
        
        try:
            # Read file from git main branch
            result = subprocess.run(
                ['git', 'show', f'main:{relative_path}'],
                capture_output=True,
                text=True,
                check=True
            )
            content = result.stdout
            columns = extract_columns_from_sql(content)
            if columns:
                schemas[model_name] = columns
        except subprocess.CalledProcessError:
            # File might not exist in main branch, skip it
            continue
    
    return schemas


def compare_schemas(
    current: Dict[str, List[str]],
    main_branch: Dict[str, List[str]]
) -> Dict[str, Dict]:
    """
    Compare current schemas with main branch schemas.
    Returns dict with comparison results for each model.
    """
    results = {}
    
    all_models = set(current.keys()) | set(main_branch.keys())
    
    for model_name in sorted(all_models):
        current_cols = set(current.get(model_name, []))
        main_cols = set(main_branch.get(model_name, []))
        
        new_cols = current_cols - main_cols
        removed_cols = main_cols - current_cols
        unchanged_cols = current_cols & main_cols
        
        # Detect potential renames (heuristic: similar names)
        renamed_cols = {}
        for removed in list(removed_cols):
            # Look for similar column names in new columns
            # Simple heuristic: check if there's a column with similar name
            for new_col in new_cols:
                # Check if it might be a rename (e.g., lakehousedateprocessed -> lds_lakehouse_date_processed)
                if removed.lower().replace('_', '') == new_col.lower().replace('_', ''):
                    renamed_cols[removed] = new_col
                    new_cols.discard(new_col)
                    removed_cols.discard(removed)
                    break
        
        results[model_name] = {
            'status': 'changed' if (new_cols or removed_cols or renamed_cols) else 'unchanged',
            'new_columns': sorted(new_cols),
            'removed_columns': sorted(removed_cols),
            'renamed_columns': renamed_cols,
            'unchanged_columns': sorted(unchanged_cols)
        }
    
    return results


def generate_markdown(results: Dict[str, Dict]) -> str:
    """
    Generate markdown documentation from comparison results.
    """
    lines = []
    lines.append("# Stable Layer Schema Changes")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    
    changed_models = [m for m, r in results.items() if r['status'] == 'changed']
    total_new = sum(len(r['new_columns']) for r in results.values())
    total_removed = sum(len(r['removed_columns']) for r in results.values())
    total_renamed = sum(len(r['renamed_columns']) for r in results.values())
    
    lines.append(f"- Total models changed: {len(changed_models)}")
    lines.append(f"- Total columns added: {total_new}")
    lines.append(f"- Total columns removed: {total_removed}")
    lines.append(f"- Total columns renamed: {total_renamed}")
    lines.append("")
    lines.append("## Changes by Model")
    lines.append("")
    
    # Only show changed models
    for model_name in sorted(changed_models):
        result = results[model_name]
        lines.append(f"### {model_name}")
        lines.append(f"**Status:** {result['status'].title()}")
        lines.append("")
        
        if result['new_columns']:
            lines.append("- **New Columns:**")
            for col in result['new_columns']:
                lines.append(f"  - `{col}`")
            lines.append("")
        
        if result['removed_columns']:
            lines.append("- **Removed Columns:**")
            for col in result['removed_columns']:
                lines.append(f"  - `{col}`")
            lines.append("")
        
        if result['renamed_columns']:
            lines.append("- **Column Renames:**")
            for old_col, new_col in sorted(result['renamed_columns'].items()):
                lines.append(f"  - `{old_col}` → `{new_col}`")
            lines.append("")
    
    lines.append("## Migration Guide")
    lines.append("")
    lines.append("### For Downstream Models")
    lines.append("")
    lines.append("1. **Concept Mapping Fields:** All models with `*_source_concept_id` now include source/target debugging fields")
    lines.append("2. **Column Naming:** Standardised to use `lds_` prefix for lakehouse fields")
    lines.append("3. **PATIENT_PERSON:** Now uses native columns from source table instead of generated IDs")
    lines.append("")
    lines.append("### Breaking Changes")
    lines.append("")
    
    breaking_changes = []
    for model_name, result in results.items():
        if result['removed_columns'] or result['renamed_columns']:
            breaking_changes.append(model_name)
    
    if breaking_changes:
        for model_name in breaking_changes:
            result = results[model_name]
            change_desc = f"- `{model_name}`: "
            parts = []
            if result['removed_columns']:
                parts.append(f"Removed columns: {', '.join(f'`{c}`' for c in result['removed_columns'])}")
            if result['renamed_columns']:
                parts.append(f"Renamed columns: {', '.join(f'`{old}` → `{new}`' for old, new in result['renamed_columns'].items())}")
            change_desc += ". ".join(parts)
            change_desc += " - requires updates to any queries using old names"
            lines.append(change_desc)
    else:
        lines.append("- None")
    
    lines.append("")
    lines.append("### Non-Breaking Additions")
    lines.append("")
    lines.append("- All new columns are additions - existing queries will continue to work")
    lines.append("- New concept mapping fields provide debugging capabilities without affecting existing logic")
    lines.append("")
    
    return "\n".join(lines)


def main():
    """Main function to generate schema diff documentation."""
    stable_dir = Path("models/olids/stable")
    output_file = Path("docs/stable_layer_schema_changes.md")
    
    # Ensure docs directory exists
    output_file.parent.mkdir(exist_ok=True)
    
    print("Extracting current stable layer schemas...")
    current_schemas = get_current_stable_schemas(stable_dir)
    print(f"Found {len(current_schemas)} current models")
    
    print("Extracting git main branch stable layer schemas...")
    main_schemas = get_git_main_stable_schemas(stable_dir)
    print(f"Found {len(main_schemas)} models in main branch")
    
    print("Comparing schemas...")
    results = compare_schemas(current_schemas, main_schemas)
    
    print("Generating markdown documentation...")
    markdown = generate_markdown(results)
    
    print(f"Writing documentation to {output_file}...")
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(markdown)
    
    print("Documentation generated successfully!")
    changed_count = len([m for m, r in results.items() if r['status'] == 'changed'])
    print(f"  Changed models: {changed_count}")


if __name__ == "__main__":
    main()

