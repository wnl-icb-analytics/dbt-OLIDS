SELECT
    column1::VARCHAR AS id,
    column2::BOOLEAN AS is_gp2gp_copy,
    column3::VARCHAR AS gp2gp_original_id,
    column4::BOOLEAN AS is_gp2gp_import
FROM VALUES
    ('00000000-0000-0000-0000-000000000182', TRUE, '00000000-0000-0000-0000-000000000180', TRUE)
