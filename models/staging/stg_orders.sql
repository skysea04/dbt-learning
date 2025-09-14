with source as (
    select
        id as order_id,
        user_id as customer_id,
        order_date,
        status
    from {{ source("jaffle_shop", "orders")}}
),

order_status_mapping as (select * from {{ ref('seed_order_statuses') }}),

transformed as (
    select
        s.order_id,
        s.customer_id,
        s.order_date,
        s.status,
        osm.is_valid
    from source as s
    left join order_status_mapping as osm on s.status = osm.status

)

select * from transformed