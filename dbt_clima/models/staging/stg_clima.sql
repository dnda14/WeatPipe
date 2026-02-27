{{ config(materialized='view') }}

with raw as (
    select * from {{ source('public', 'raw_clima') }}
)

select
    "Ciudad" as ciudad,
    cast("Temperatura" as numeric(5,2)) as temperatura,
    cast("Humedad" as integer) as humedad,
    "Description" as descripcion,
    cast("Viento" as numeric(5,2)) as velocidad_viento,
    current_timestamp as fec_extraccion
from raw
