{{ config(materialized='table') }}

with stg_data as (
    select * from {{ ref('stg_clima') }}
)

select distinct
    ciudad as nombre_ciudad
from stg_data
where ciudad is not null
