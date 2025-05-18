{{ config(
    materialized = 'incremental',
    unique_key = ['date_time', 'extracted_at']
) }}

WITH base AS (
    SELECT 
        date_time,
        temperature,
        humidity,
        wind_speed,
        sky_condition,
        extracted_at,
        ROW_NUMBER() OVER (
            PARTITION BY date_time, extracted_at 
            ORDER BY extracted_at DESC
        ) AS rn
    FROM {{ ref('stg_weather_current') }}
)

SELECT
    date_time,
    temperature,
    humidity,
    wind_speed,
    sky_condition,
    extracted_at
FROM base
WHERE rn = 1

{% if is_incremental() %}
  AND (date_time, extracted_at) NOT IN (
    SELECT date_time, extracted_at FROM {{ this }}
  )
{% endif %}
