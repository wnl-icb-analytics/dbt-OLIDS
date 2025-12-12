{% test column_completeness(model, column_name, tolerance_percent=1.0) %}

    {#-
        Generic test to check column completeness with tolerance threshold.
        
        Checks that the percentage of NULL values in a column does not exceed tolerance_percent.
        Fails if NULL percentage exceeds tolerance_percent.
        
        Default tolerance: 1.0%
        sk_patient_id should use tolerance_percent=5.0
        
        Usage:
            - name: patient_id
              tests:
                - column_completeness:
                    tolerance_percent: 1.0
            
            - name: sk_patient_id
              tests:
                - column_completeness:
                    tolerance_percent: 5.0
    -#}

    with stats as (
        select
            count(*) as total_rows,
            sum(case when {{ column_name }} is null then 1 else 0 end) as null_count,
            {{ tolerance_percent }} as tolerance_threshold
        from {{ model }}
    ),
    null_percentage as (
        select
            total_rows,
            null_count,
            tolerance_threshold,
            case 
                when total_rows > 0 
                then 100.0 * null_count / total_rows 
                else 0 
            end as null_percentage
        from stats
    )
    select
        {{ column_name }}
    from {{ model }}
    where {{ column_name }} is null
        and (
            select null_percentage 
            from null_percentage
        ) > (
            select tolerance_threshold 
            from null_percentage
        )

{% endtest %}

