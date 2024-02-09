
  
    

    create or replace table `dvp-np-sandbox-b8d0`.`gold_us`.`assessments_scores`
      
    
    

    OPTIONS()
    as (
      WITH base AS (
  SELECT
    DISTINCT 
    
    CAST(sbac.surrogate_key AS INT64) AS surrogate_key,
    
    CAST(sbac.county_code AS INT64) AS county_code,
    
    CAST(sbac.district_code AS INT64) AS district_code,
    
    CAST(sbac.school_code AS INT64) AS school_code,
    
    CAST(sbac.filler AS STRING) AS filler,
    
    CAST(sbac.test_year AS INT64) AS test_year,
    
    CAST(sbac.student_group_id AS STRING) AS student_group_id,
    
    CAST(sbac.test_type AS STRING) AS test_type,
    
    CAST(sbac.total_tested_at_reporting_level AS STRING) AS total_tested_at_reporting_level,
    
    CAST(sbac.total_tested_with_scores_at_reporting_level AS STRING) AS total_tested_with_scores_at_reporting_level,
    
    CAST(sbac.grade AS INT64) AS grade,
    
    CAST(sbac.test_id AS INT64) AS test_id,
    
    CAST(sbac.students_enrolled AS INT64) AS students_enrolled,
    
    CAST(sbac.students_tested AS INT64) AS students_tested,
    
    CAST(sbac.mean_scale_score AS FLOAT64) AS mean_scale_score,
    
    CAST(sbac.percentage_standard_exceeded AS FLOAT64) AS percentage_standard_exceeded,
    
    CAST(sbac.percentage_standard_met AS FLOAT64) AS percentage_standard_met,
    
    CAST(sbac.Percentage_Standard_Met_And_Above AS FLOAT64) AS Percentage_Standard_Met_And_Above,
    
    CAST(sbac.percentage_standard_nearly_met AS FLOAT64) AS percentage_standard_nearly_met,
    
    CAST(sbac.percentage_standard_not_met AS FLOAT64) AS percentage_standard_not_met,
    
    CAST(sbac.Students_with_Scores AS INT64) AS Students_with_Scores,
    
    CAST(sbac.area_1_percentage_above_standard AS FLOAT64) AS area_1_percentage_above_standard,
    
    CAST(sbac.area_1_percentage_near_standard AS FLOAT64) AS area_1_percentage_near_standard,
    
    CAST(sbac.area_1_percentage_below_standard AS FLOAT64) AS area_1_percentage_below_standard,
    
    CAST(sbac.area_2_percentage_above_standard AS FLOAT64) AS area_2_percentage_above_standard,
    
    CAST(sbac.area_2_percentage_near_standard AS FLOAT64) AS area_2_percentage_near_standard,
    
    CAST(sbac.area_2_percentage_below_standard AS FLOAT64) AS area_2_percentage_below_standard,
    
    CAST(sbac.area_3_percentage_above_standard AS FLOAT64) AS area_3_percentage_above_standard,
    
    CAST(sbac.area_3_percentage_near_standard AS FLOAT64) AS area_3_percentage_near_standard,
    
    CAST(sbac.area_3_percentage_below_standard AS FLOAT64) AS area_3_percentage_below_standard,
    
    CAST(sbac.area_4_percentage_above_standard AS FLOAT64) AS area_4_percentage_above_standard,
    
    CAST(sbac.area_4_percentage_near_standard AS FLOAT64) AS area_4_percentage_near_standard,
    
    CAST(sbac.area_4_percentage_below_standard AS FLOAT64) AS area_4_percentage_below_standard,
    
    CAST(sbac.type_id AS STRING) AS type_id
    ,
    CONCAT(sbac.test_id,
    ela.school_code,
    sbac.grade,
    sbac.student_group_id) AS _key,
  FROM
    `dvp-np-sandbox-b8d0`.`silver_us`.`assessments_sbac` AS sbac
    LEFT OUTER JOIN `dvp-np-sandbox-b8d0`.`silver_us`.`academic_indicators_ela` AS ela
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
    );
  