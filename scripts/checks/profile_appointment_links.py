"""Emit aggregate-only profiling SQL for the compiled appointment link model."""

import argparse
from pathlib import Path


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('compiled_sql', type=Path)
    args = parser.parse_args()
    model = args.compiled_sql.read_text().strip().rstrip(';')
    print(f"""ALTER SESSION SET USE_CACHED_RESULT=FALSE;
WITH candidate AS ({model})
SELECT clinical_record_type,
    COUNT(*) AS row_count,
    COUNT(DISTINCT appointment_id, clinical_record_type, clinical_record_id) AS grain_count,
    COUNT(DISTINCT appointment_id) AS appointment_count,
    COUNT_IF(appointment_id IS NULL OR clinical_record_type IS NULL OR clinical_record_id IS NULL
        OR encounter_id IS NULL OR patient_id IS NULL OR person_id IS NULL) AS incomplete_links,
    HASH_AGG(appointment_id, clinical_record_type, clinical_record_id,
        encounter_id, patient_id, person_id) AS output_hash
FROM candidate
GROUP BY clinical_record_type;
""")
