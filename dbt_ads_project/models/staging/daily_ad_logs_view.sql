WITH raw AS (
    SELECT *
    FROM {{ source('staging_source', 'stg_ad_logs_tbl') }} 
)

SELECT
    ad_id,
    xyz_campaign_id,
    fb_campaign_id,
    age,
    LOWER(gender) AS gender,
    COALESCE(CAST(interest AS STRING), 'NA') AS interest,
    impressions,
    clicks,
    spent,
    total_conversion,
    approved_conversion
FROM raw
