{{
  config(
    tags=["weekly"],
    materialized = 'table',
    table_type = 'iceberg'
    )
}}
select * from 
(
    select  *
    from    {{ ref('cvs_dispense_stg') }}
    union all
    select  *
    from    {{ ref('walgreens_dispense_stg') }}
) a
LEFT JOIN {{ ref('cvs_status_stg') }} b
ON a.id = b.dispense_id