WITH transform AS (
  SELECT
    *,
    SAFE_CAST(LEFT(SAFE_CAST(cds AS STRING), 2) AS INT64) AS county_code,
    SAFE_CAST(SUBSTR(SAFE_CAST(cds AS STRING), 3, 5) AS INT64) AS district_code,
    SAFE_CAST(RIGHT(SAFE_CAST(cds AS STRING), 7) AS INT64) AS school_code, 
    CONCAT(cds, reportingyear, studentgroup) AS tertiary_key
  FROM
    {{ source('staging_us', 'academic_indicators_ela') }}
)

SELECT
  {{ generate_surrogate_key('TO_JSON_STRING(transform)') }} AS surrogate_key,
  transform.*
FROM
  transform