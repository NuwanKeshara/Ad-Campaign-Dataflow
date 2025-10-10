with base as (

    select
        campaign_id,
        fb_campaign_id,
        age,
        gender,
        interest,
        sum(impressions) as total_impressions,
        sum(clicks) as total_clicks,
        sum(spent) as total_spent,
        sum(total_conversion) as total_conversion,
        sum(approved_conversion) as approved_conversion
    from {{ source('dm_source', 'dm_ad_logs') }} 
    group by 1,2,3,4,5

),

metrics as (

    select
        campaign_id,
        fb_campaign_id,
        age,
        gender,
        interest,
        total_impressions,
        total_clicks,
        total_spent,
        total_conversion,
        approved_conversion,
        
        case when total_impressions > 0 then total_clicks::float / total_impressions else 0 end as click_rate,
        case when total_clicks > 0 then total_spent::float / total_clicks else 0 end as spent_rate,
        case when total_impressions > 0 then total_clicks::float / total_impressions else 0 end as reach
    from base

)

select * from metrics
