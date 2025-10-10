WITH raw AS (
    SELECT *
    FROM {{ source('staging_source', 'stg_ad_logs') }} 
)

SELECT
    ad_id,
    xyz_campaign_id,
    fb_campaign_id,
    age,
    LOWER(gender) AS gender,
    COALESCE(interest::text, 'NA') AS interest,
    impressions,
    clicks,
    spent,
    total_conversion,
    approved_conversion
FROM raw
