{{
    config(
        materialized='incremental'
    )
}}
select *,date_trunc('day', event_time) date_day from mkmotors_dev.staging.stgpay

{% if is_incremental() %}
where date_day >= (select max(date_day) from {{ this }})
{% endif %}