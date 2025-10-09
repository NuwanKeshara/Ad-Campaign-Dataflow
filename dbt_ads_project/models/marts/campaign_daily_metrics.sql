{{ config(materialized='table') }}

with base as (
    select
        ingestion_date as day,
        coalesce(xyz_campaign_id,'unknown') as campaign_id,
        sum(impressions) as total_impressions,
        sum(clicks) as total_clicks,
        sum(total_conversion) as total_conversions,
        sum(approved_conversion) as total_approved_conversions,
        sum(spent) as total_spent
    from {{ ref('stg_ad_logs') }}
    group by day, campaign_id
)
select
    day,
    campaign_id,
    total_impressions,
    total_clicks,
    total_conversions,
    total_approved_conversions,
    total_spent,
    case when total_impressions = 0 then 0 else total_clicks::float/total_impressions end as ctr,
    case when total_clicks = 0 then 0 else total_conversions::float/total_clicks end as cvr,
    case when total_clicks = 0 then 0 else total_spent/total_clicks end as cost_per_click,
    case when total_impressions = 0 then 0 else total_spent/total_impressions*1000 end as cpm
from base
order by day desc
