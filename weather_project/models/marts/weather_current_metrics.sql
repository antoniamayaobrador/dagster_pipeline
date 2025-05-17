SELECT date_time, temperature, humidity, wind_speed, sky_condition, extracted_at
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY date_time ORDER BY extracted_at DESC) as rn
  FROM {{ ref('stg_weather_current') }}
)
WHERE rn = 1
{% if is_incremental() %}
  AND extracted_at > (SELECT MAX(extracted_at) FROM {{ this }})
{% endif %}
