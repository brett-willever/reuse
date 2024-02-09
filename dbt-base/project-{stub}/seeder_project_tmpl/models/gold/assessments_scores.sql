WITH base AS (
  SELECT
    DISTINCT 
    {% for k, v in model.columns.items() %}
    CAST(sbac.{{k}} AS {{v.get('data_type')}}) AS {{k}}{% if not loop.last %},{% endif %}
    {% endfor %},
    CONCAT(sbac.test_id,
    ela.school_code,
    sbac.grade,
    sbac.student_group_id) AS _key,
  FROM
    {{ source(
      'silver_us',
      'assessments_sbac'
    ) }} AS sbac
    LEFT OUTER JOIN {{ source(
      'silver_us',
      'academic_indicators_ela'
    ) }} AS ela
    ON CAST(sbac.district_code AS INT64) = CAST(ela.district_code AS INT64)
    AND CAST(sbac.school_code AS INT64) = CAST(ela.school_code AS INT64)
),
add_composite AS (
SELECT
  _key,
  ROUND(
    NULLIF(safe_divide(
      SUM(
        CAST(Percentage_Standard_Met_and_Above AS FLOAT64) * CAST(Students_with_Scores AS INT64)
      ),
      SUM(CAST(Students_with_Scores AS INT64))
    ), 0.00),
    2 -- ROUND end
  ) AS ELA_Calculated_Composite --count_met_and_above
FROM
  base
WHERE
  Percentage_Standard_Met_and_Above IS NOT NULL 
  AND Students_with_Scores IS NOT NULL
  AND CAST(Percentage_Standard_Met_and_Above AS STRING) != '0' 
  AND CAST(Students_with_Scores AS STRING) != '0'
GROUP BY
  _key
)
SELECT 
  base.* EXCEPT(_key),
  add_composite.ELA_Calculated_Composite
  FROM base 
    LEFT OUTER JOIN add_composite 
      ON CAST(add_composite._key AS INT64) = CAST(base._key AS INT64)
