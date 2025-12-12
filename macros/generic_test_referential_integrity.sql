{% test referential_integrity(model, column_name, to, field='id', tolerance_percent=1.0) %}

    {#-
        Generic test to check referential integrity with tolerance threshold.
        
        Checks that foreign key values exist in the referenced table's primary key column.
        Fails if orphaned FK percentage exceeds tolerance_percent.
        
        Default tolerance: 1.0%
        
        Usage:
            - name: patient_id
              tests:
                - referential_integrity:
                    arguments:
                      to: ref('base_olids_patient')
                      field: id
                      tolerance_percent: 1.0
            
            - name: encounter_id
              tests:
                - referential_integrity:
                    arguments:
                      to: ref('base_olids_encounter')
                      tolerance_percent: 1.0
    -#}

    with left_table as (
        select distinct {{ column_name }} as id
        from {{ model }}
        where {{ column_name }} is not null
    ),
    right_table as (
        select distinct {{ field }} as id
        from {{ to }}
        where {{ field }} is not null
    ),
    exceptions as (
        select
            left_table.id,
            right_table.id as right_id
        from left_table
        left join right_table
            on left_table.id = right_table.id
        where right_table.id is null
    ),
    stats as (
        select
            (select count(distinct id) from left_table) as total_distinct_fk,
            (select count(distinct id) from exceptions) as orphaned_distinct_fk,
            {{ tolerance_percent }} as tolerance_threshold
    )
    select *
    from exceptions
    where (
        select case 
            when total_distinct_fk > 0 
            then 100.0 * orphaned_distinct_fk / total_distinct_fk 
            else 0 
        end
        from stats
    ) > (select tolerance_threshold from stats)

{% endtest %}

