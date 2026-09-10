"""Emit a read-only Snowflake check of the compiled model against synthetic snapshots."""

import argparse
import re
from pathlib import Path


def fixture(name, columns, rows):
    return f"{name} ({columns}) AS (SELECT * FROM VALUES {rows})"


def snapshot(model, after=False):
    # The later snapshot reassigns a slot, moves a recorded link and removes
    # an observation entirely, without a deletion tombstone.
    first_person = 2 if after else 1
    moved_appointment = "'a2'" if after else "'a1'"
    observations = [
        "('different_practice', 'e2', 1, FALSE)",
        "('different_person', 'e2', 2, FALSE)",
        "('deleted_appointment', 'e3', 1, FALSE)",
        "('deleted_encounter', 'e4', 1, FALSE)",
        "('deleted_record', 'e2', 1, TRUE)",
        "('missing_encounter', 'absent', 1, FALSE)",
        "('no_encounter', NULL, 1, FALSE)",
        "('missing_appointment', 'e5', 1, FALSE)",
        "('unknown_person', 'e2', NULL, FALSE)",
        "('moved', 'e6', 1, FALSE)",
        "('old_slot_patient', 'e1', 1, FALSE)",
        "('null_deletion_flag', 'e2', 1, NULL)",
    ]
    if not after:
        observations.append("('removed_next_snapshot', 'e2', 1, FALSE)")
    # Clinical patient IDs differ from the appointment's practice registration.
    observations = [row[:-1] + ", 'clinical_practice')" for row in observations]
    fixtures = [
        fixture("fixture_appointment", "id, patient_id, person_id, lds_is_deleted", ",".join([
            f"('a1', 'practice_a', {first_person}, FALSE)",
            "('a2', 'practice_b', 1, FALSE)",
            "('a3', 'practice_b', 1, TRUE)",
        ])),
        fixture("fixture_encounter", "id, appointment_id, person_id, lds_is_deleted", ",".join([
            "('e1', 'a1', 1, FALSE)",
            "('e2', 'a2', 1, FALSE)",
            "('e3', 'a3', 1, FALSE)",
            "('e4', 'a2', 1, TRUE)",
            "('e5', 'absent', 1, FALSE)",
            f"('e6', {moved_appointment}, 1, FALSE)",
        ])),
        fixture("fixture_observation", "id, encounter_id, person_id, lds_is_deleted, patient_id", ",".join(observations)),
        fixture("fixture_medication_order", "id, encounter_id, person_id, lds_is_deleted", "('same_id', 'e2', 1, FALSE)"),
        fixture("fixture_medication_statement", "id, encounter_id, person_id, lds_is_deleted", "('same_id', 'e2', 1, FALSE)"),
    ]
    for entity in ("appointment", "encounter", "observation", "medication_order", "medication_statement"):
        model, count = re.subn(
            rf'\bOLIDS_ENGINEERING\.CONFORMED\.{entity}\b',
            f'fixture_{entity}', model, flags=re.IGNORECASE,
        )
        if count != 1:
            raise ValueError(f"Expected one compiled reference for {entity}, found {count}")
    return "WITH " + ",\n".join(fixtures) + ", result AS (\n" + model + "\n) SELECT * FROM result"


def check_sql(model):
    before = snapshot(model)
    after = snapshot(model, after=True)
    expected_before = [
        "('a2','observation','different_practice','e2','practice_b',1)",
        "('a1','observation','moved','e6','practice_a',1)",
        "('a1','observation','old_slot_patient','e1','practice_a',1)",
        "('a2','observation','null_deletion_flag','e2','practice_b',1)",
        "('a2','observation','removed_next_snapshot','e2','practice_b',1)",
        "('a2','medication_order','same_id','e2','practice_b',1)",
        "('a2','medication_statement','same_id','e2','practice_b',1)",
    ]
    expected_after = [row for row in expected_before if not any(
        label in row for label in ('moved', 'old_slot_patient', 'removed_next_snapshot')
    )] + ["('a2','observation','moved','e6','practice_b',1)"]
    columns = "appointment_id, clinical_record_type, source_record_id, encounter_id, patient_id, person_id"
    def expected(name, rows):
        raw = fixture(name + '_raw', columns, ','.join(rows))
        return raw + f""", {name} AS (
            SELECT appointment_id, clinical_record_type,
                UUID_STRING('6ba7b811-9dad-11d1-80b4-00c04fd430c8',
                    'olids:clinical_record:' || clinical_record_type || ':' || source_record_id)::UUID AS clinical_record_id,
                source_record_id, encounter_id, patient_id, person_id
            FROM {name}_raw)"""
    return f"""WITH before_result AS ({before}), after_result AS ({after}),
{expected('expected_before', expected_before)},
{expected('expected_after', expected_after)},
before_missing AS (SELECT * FROM expected_before MINUS SELECT * FROM before_result),
before_extra AS (SELECT * FROM before_result MINUS SELECT * FROM expected_before),
after_missing AS (SELECT * FROM expected_after MINUS SELECT * FROM after_result),
after_extra AS (SELECT * FROM after_result MINUS SELECT * FROM expected_after)
SELECT
    (SELECT COUNT(*) FROM before_result) AS before_rows,
    (SELECT COUNT(*) FROM after_result) AS after_rows,
    (SELECT COUNT(*) FROM before_missing) + (SELECT COUNT(*) FROM before_extra)
      + (SELECT COUNT(*) FROM after_missing) + (SELECT COUNT(*) FROM after_extra)
      + ABS((SELECT COUNT(*) FROM before_result) - {len(expected_before)})
      + ABS((SELECT COUNT(*) FROM after_result) - {len(expected_after)}) AS failures;
"""


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('compiled_sql', type=Path)
    args = parser.parse_args()
    print(check_sql(args.compiled_sql.read_text().strip().rstrip(';')))
