{{
  config(
    tags=["weekly"],
    materialized = 'incremental',
    table_type = 'iceberg',
    incremental_strategy = 'append',
    )
}}
select  *
from    {{ ref('cvs_status_seed') }}