{# unique key required if incremental_strategy is merge not required if append #}
{{
  
  config(
    tags=["athena"],
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'id',
    )
}}

select * from {{ ref('b_athena_incremental_ref') }}
