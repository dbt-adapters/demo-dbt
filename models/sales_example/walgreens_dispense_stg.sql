{{
  config(
    tags=["weekly"],
    materialized = 'incremental',
    table_type = 'iceberg',
    incremental_strategy = 'append',
    )
}}
select  *
from    {{ ref('walgreens_dispense_seed') }}