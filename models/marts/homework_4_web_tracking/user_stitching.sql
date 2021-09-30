
with page_views as 
(
    select * from {{ ref('stg_web_tracking__pageviews') }}
), 

merged_visitor_id as ( 
    select 
        customer_id
        ,max(visitor_id) as visitor_id 
    from page_views 
    group by 1 
), 

final as 
(
    select  
        page_views.pageview_id
        ,coalesce(merged_visitor_id.visitor_id, page_views.visitor_id) as visitor_id
        ,page_views.customer_id
        ,page_views.device_type
        ,page_views.page
        ,page_views.timestamp as visit_time
        ,page_views.visitor_id as original_visitor_id
    from page_views 
    left join 
        merged_visitor_id on page_views.customer_id = merged_visitor_id.customer_id
)
select * from final 