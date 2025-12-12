{% test concept_mapping_integrity(model, column_name, tolerance_percent=1.0) %}

    {#-
        Generic test to check concept mapping integrity with tolerance threshold.
        
        Similar to dbt_utils.relationships_where but with tolerance - checks that concept_id values 
        exist in base_olids_concept_map.source_code_id.
        Fails if failure rate exceeds tolerance_percent.
        
        Default tolerance: 1.0%
        Medications should use tolerance_percent=3.0 (dev only)
        
        Usage:
            - name: observation_source_concept_id
              tests:
                - concept_mapping_integrity:
                    arguments:
                      tolerance_percent: 1.0
            
            - name: medication_order_source_concept_id
              tests:
                - concept_mapping_integrity:
                    arguments:
                      tolerance_percent: 3.0
    -#}

    with left_table as (
        select distinct {{ column_name }} as id
        from {{ model }}
        where {{ column_name }} is not null
    ),
    right_table as (
        select distinct source_code_id as id
        from {{ ref('base_olids_concept_map') }}
        where source_code_id is not null
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
            (select count(distinct id) from left_table) as total_distinct,
            (select count(distinct id) from exceptions) as failed_distinct,
            {{ tolerance_percent }} as tolerance_threshold
    )
    select *
    from exceptions
    where (
        select case 
            when total_distinct > 0 
            then 100.0 * failed_distinct / total_distinct 
            else 0 
        end
        from stats
    ) > (select tolerance_threshold from stats)

{% endtest %}

