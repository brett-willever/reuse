WITH raw_sbac AS (
  SELECT
  {% for column in model.columns %}
    {{safe_cast_string_as_zero(column)}} AS {{ column }} {% if not loop.last %},{% endif %}
  {% endfor %}
 FROM
{{source('raw_us','sb_ca2023_all')}}
)

SELECT *,
    {{ metadata_columns('TO_JSON_STRING(raw_sbac)')}}
FROM raw_sbac
