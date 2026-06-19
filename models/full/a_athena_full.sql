{{
  config(
    tags=["athena"],
    materialized = 'table',
    )
}}

select 1 as id, '{{ env_var("DBT_TEST", "Not found") }}' AS env_var_value
