with source as (

    select * from {{ source('web_tracking', 'pageviews') }}

),

renamed as (

    select
        id as pageview_id,
        timestamp as timestamp,
        visitor_id,
        customer_id,
        device_type,
        page
    from source

)

select * from renamed