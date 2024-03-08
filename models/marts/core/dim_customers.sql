{{
    config(
        materialized='table'
    )
}}

with stg_dim_customers as (
    select
        customer_id as nk_customer_id,
        first_name,
        last_name,
        concat(first_name, ' ', last_name) as full_name,
        email,
        phone
    from {{ ref('stg_pachotel_customers') }}
),

final_dim_customers as (
    select
        {{ dbt_utils.generate_surrogate_key( ["nk_customer_id"] ) }} as sk_customer_id,
        *
    from stg_dim_customers
)

select * from final_dim_customers


