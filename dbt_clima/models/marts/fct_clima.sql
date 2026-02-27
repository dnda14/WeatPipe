{{ config(materialized='table') }}

with stg_data as (
    select * from {{ ref('stg_clima') }}
),

ciudades as (
    select * from {{ ref('dim_ciudades') }}
)

select
    c.nombre_ciudad,
    s.temperatura,
    s.humedad,
    s.velocidad_viento,
    s.descripcion,
    s.fec_extraccion
from stg_data s
join ciudades c on s.ciudad = c.nombre_ciudad
