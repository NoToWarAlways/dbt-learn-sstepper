with orders as (

    select * from {{ ref('stg_orders') }}

),

successful_payments as (

    select * from {{ ref('stg_payments') }}
    where status = 'success'
),

payments_aggregated as (
    
    select 
        order_id,

        sum(amount) as amount
    from successful_payments
    group by 1
),

final as (

    select 
        orders.order_id,
        orders.customer_id,
        orders.order_date,
        payments_aggregated.amount
    from orders
    left join payments_aggregated on orders.order_id = payments_aggregated.order_id

)

select * from final