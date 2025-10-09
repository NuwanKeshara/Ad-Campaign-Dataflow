SELECT
    xyz_campaign_id,
    COUNT(DISTINCT ad_id) AS total_ads,
    SUM(impressions) AS total_impressions,
    SUM(clicks) AS total_clicks,
    SUM(spent) AS total_spent,
    SUM(total_conversion) AS total_conversion,
    SUM(approved_conversion) AS total_approved_conversion,
    ROUND(SUM(clicks)::NUMERIC / NULLIF(SUM(impressions),0), 4) AS ctr
FROM {{ ref('stg_ad_logs') }}
GROUP BY xyz_campaign_id
