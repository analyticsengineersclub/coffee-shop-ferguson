
with pageviews_with_last_pageview as 
(
    select 
        pageview_id,
        visitor_id,
        customer_id,
        device_type,
        page,
        visit_time,
        LAG(visit_time, 1) OVER (PARTITION BY visitor_id ORDER BY visit_time asc) AS last_view
from {{ref('user_stitching')}}
),

pageviews_with_first_session as 
(
    select 
        pageview_id,
        visitor_id,
        customer_id, 
        device_type, 
        page, 
        visit_time,
        last_view,
        CASE 
            WHEN DATETIME_DIFF(visit_time, last_view, MINUTE) >= 30 or last_view IS NULL THEN 1
            ELSE 0
        END AS is_new_session
    from pageviews_with_last_pageview 
),

stg_sessions as 

(
    select 
        pageview_id,
        visitor_id,
        customer_id, 
        device_type, 
        page, 
        visit_time,
        last_view,
        is_new_session,
        SUM(is_new_session) OVER (ORDER BY visitor_id, visit_time ) AS global_session_id,
        SUM(is_new_session) OVER (PARTITION BY visitor_id ORDER BY visit_time asc) AS user_session_id
    from pageviews_with_first_session 
),

sessions as 
(
    select 
        user_session_id || '-' || visitor_id AS session_id, 
        pageview_id,
        visitor_id,
        customer_id,
        device_type,
        page,
        visit_time
    from stg_sessions
),

final as 
(
    select 
        session_id, 
        pageview_id,
        visitor_id,
        customer_id,
        device_type,
        page,
        visit_time,
        MIN(visit_time) OVER (PARTITION BY session_id) AS session_start,
        MAX(visit_time) OVER (PARTITION BY session_id) AS session_end
    from sessions
)

select * from final
