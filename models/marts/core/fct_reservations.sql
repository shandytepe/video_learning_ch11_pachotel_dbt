with dim_dates as (
    select *
    from {{ ref("dim_dates") }}
), 

dim_customers as (
    select *
    from {{ ref("dim_customers") }}
),

stg_reservations as (
    select 
        reservation_id as nk_reservation_id,
        customer_id,
        reservation_date::date as reservation_date,
        start_date::date as start_date,
        end_date::date as end_date,
        total_price,
        review,
        rating
    from {{ ref("stg_pachotel_reservations") }}
),

stg_payments as (
    select 
        payment_id,
        reservation_id,
        provider,
        account_number,
        payment_status,
        payment_date::date as payment_date,
        expire_date::date as expire_date
    from {{ ref("stg_pachotel_payments") }}
),

final as (

    select
        {{ dbt_utils.generate_surrogate_key( ["nk_reservation_id"]) }} as sk_reservation_id,
        sr.nk_reservation_id,
        sp.payment_id as nk_payment_id, 
        dc.sk_customer_id,
        dd1.date_day as reservation_date,
        dd2.date_day as end_date,
        sp.provider,
        sp.account_number,
        sp.payment_status,
        dd3.date_day as payment_date,
        dd4.date_day as expire_date,
        sr.total_price,
        sr.review,
        sr.rating
    from stg_payments sp
    left join stg_reservations sr
    on sr.nk_reservation_id = sp.reservation_id
    left join dim_customers dc 
    on dc.nk_customer_id = sr.customer_id
    left join dim_dates dd1
    on dd1.date_day = sr.reservation_date
    left join dim_dates dd2
    on dd2.date_day = sr.end_date
    left join dim_dates dd3
    on dd3.date_day = sp.payment_date
    left join dim_dates dd4
    on dd4.date_day = sp.expire_date
)

select * from final