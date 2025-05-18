{{ config(
    materialized = 'view'
) }}

SELECT
    CASE 
        WHEN FECHA IS NULL THEN NULLIF(TO_TIMESTAMP_NTZ('1970-01-01'), '1970-01-01')
        ELSE TO_TIMESTAMP_NTZ(FECHA) 
    END AS date_time,
    CAST(TEMPERATURA_ACTUAL AS NUMBER(4,1)) AS temperature,
    CAST(HUMEDAD AS NUMBER(4,1)) AS humidity,
    CAST(VIENTO AS NUMBER(4,1)) AS wind_speed,
    STATESKY:description::STRING AS sky_condition,
    _AIRBYTE_EXTRACTED_AT AS extracted_at
FROM {{ source('raw', 'PALMA') }}
WHERE FECHA IS NOT NULL
