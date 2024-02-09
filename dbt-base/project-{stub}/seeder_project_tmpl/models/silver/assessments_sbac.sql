WITH raw_assessments AS (
SELECT
    *
FROM
    {{source('staging_us', 'assessments_sbac')}}
)

SELECT 
   {{ generate_surrogate_key('TO_JSON_STRING(raw_assessments)') }} AS surrogate_key,
   *
FROM raw_assessments