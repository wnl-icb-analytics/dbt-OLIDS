SELECT
    column1::VARCHAR AS patient_id,
    column2::DATE AS episode_of_care_start_date,
    column3::VARCHAR AS episode_type_source_code,
    column4::BOOLEAN AS lds_is_deleted
FROM VALUES
    ('SYN_registration_day_B', '2020-01-02', 'Regular', FALSE),
    ('SYN_single_import_A', '2020-02-01', 'Regular', FALSE),
    ('SYN_day31_A', '2020-02-01', 'Regular', FALSE),
    ('SYN_reregister_A', '2019-01-01', 'Regular', FALSE),
    ('SYN_reregister_A', '2020-02-01', 'Regular', FALSE),
    ('SYN_reregister_A', '2020-02-01', 'Regular', FALSE),
    ('SYN_reregister_A', '2021-02-01', 'Regular', FALSE),
    ('SYN_temporary_B', '2020-01-02', 'Temporary', FALSE),
    ('SYN_day0_A', '2020-02-01', 'Regular', FALSE),
    ('SYN_day30_A', '2020-02-01', 'Regular', FALSE),
    ('SYN_event_on_start_A', '2020-02-01', 'Regular', FALSE),
    ('SYN_before_start_A', '2020-02-01', 'Regular', FALSE)
