WITH base AS (
    SELECT
        campaign_id,
        fb_campaign_id,
        age,
        gender,
        interest,
        SUM(impressions) AS total_impressions,
        SUM(clicks) AS total_clicks,
        SUM(spent) AS total_spent,
        SUM(total_conversion) AS total_conversion,
        SUM(approved_conversion) AS approved_conversion
    FROM {{ source('datamodel_source','dm_ad_logs_tbl') }} 
    GROUP BY 1, 2, 3, 4, 5
),

metrics AS (
    SELECT
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
        CASE WHEN total_impressions > 0 THEN CAST(total_clicks AS FLOAT64) / total_impressions ELSE 0 END AS click_rate,
        CASE WHEN total_clicks > 0 THEN CAST(total_spent AS FLOAT64) / total_clicks ELSE 0 END AS spent_rate,
        CASE WHEN total_impressions > 0 THEN CAST(total_clicks AS FLOAT64) / total_impressions ELSE 0 END AS reach
    FROM base
)

SELECT * FROM metrics
