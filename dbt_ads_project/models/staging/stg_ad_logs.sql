{{ config(materialized='table') }}

with raw as (
    select * from {{ source('raw', 'raw_ad_logs') }}
)
select
    ad_id,
    xyz_campaign_id,
    fb_campaign_id,
    cast(age as int) as age,
    gender,
    interest,
    cast("Impressions" as int) as impressions,
    cast("Clicks" as int) as clicks,
    cast("Spent" as float) as spent,
    cast("Total_Conversion" as int) as total_conversion,
    cast("Approved_Conversion" as int) as approved_conversion,
    current_date as ingestion_date
from raw
