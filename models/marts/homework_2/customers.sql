{{ config(
    materialized='table'
)}}

with customer_orders as (
    select
        customer_id,
        count(*) as number_of_orders,
        min(created_at) as first_order_at,
    from {{source('coffee_shop','orders')}}
    group by customer_id
)

select
    c.id AS customer_id,
    c.name,
    c.email,
    co.first_order_at,
    co.number_of_orders,

from {{source('coffee_shop','customers')}} as c
left join customer_orders as co
     on c.id = co.customer_id