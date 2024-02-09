/*
  Known tests case `ELA met and above` 
  School -> 6021042
  Composite -> 42.03%
*/
SELECT
  ela_calculated_composite
FROM
  {{ref('assessments_scores')}}
WHERE
  grade = 13
  AND school_code = 6021042
  AND test_id = 1
GROUP BY ela_calculated_composite
HAVING
  AVG(ela_calculated_composite) = 42.03