with 

orders as (
    select * from {{ ref('stg_coffee_shop__orders')}}
),

order_items as (
    select * from {{ ref('stg_coffee_shop__order_items')}}
),

customers as (
    select * from {{ ref('stg_coffee_shop__customers')}}
),

products as (
    select * from {{ ref('stg_coffee_shop__products')}}
),

product_prices as (
    select * from {{ ref('stg_coffee_shop__product_prices')}}
),

transformed as (
    select
        orders.order_id,
        orders.created_at,
        products.product_id,
        products.product_name,
        products.category as product_category,
        product_prices.price, 
        orders.customer_id,
        -- this will view all orders by a customer and assign row number by created at date so we can determine the first order and when a customer is new
        row_number() over (partition by orders.customer_id order by orders.created_at) as customer_order_index
    from orders
    inner join order_items using (order_id)
    inner join products using (product_id)
    left outer join product_prices on product_prices.product_id = products.product_id
        and product_prices.created_at <= orders.created_at
        and product_prices.ended_at >= orders.created_at
),

final as (
        select
        order_id,
        created_at,
        product_id,
        product_name,
        product_category,
        price,
        customer_id,
        case 
            when customer_order_index = 1 then true 
            else false 
        end as is_new_customer
    from transformed
)

select * from final