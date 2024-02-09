{% macro safe_cast_string_as_zero(column_name) %}
SAFE_CAST(
  CASE
    WHEN SAFE_CAST({{ column_name }} AS STRING) = '*' THEN '0'
    ELSE CAST({{ column_name }} AS STRING)
  END AS STRING
)
{% endmacro %}

{% macro filter_out_values(
    column_name,
    values
  ) %}
WHERE
  {{ column_name }} NOT IN (
    {{ values |
    JOIN(", ") }}
  )
{% endmacro %}

{% macro metadata_columns(columns) %}
  STRUCT(current_datetime() AS updated_at, current_datetime() AS created_at, {{ generate_surrogate_key(columns) }} AS hash_key, 1 AS version, TRUE AS is_active) AS _dvp_metadata
{% endmacro %}

{% macro data_spine(
    materialized,
    start_date,
    end_date,
    interval = '1 day'
  ) %}
  WITH days AS (
    SELECT
      date_add(
        {{ start_date }},
        INTERVAL CAST(
          GENERATOR.value AS int64
        ) - 1 '{{ interval }}'
      ) AS date_day
    FROM
      unnest(
        generate_array(
          1,
          date_diff(
            {{ end_date }},
            {{ start_date }},
            {{ interval }}
          ) + 1
        )
      ) AS GENERATOR
  )
SELECT
  *
FROM
  days
{% endmacro %}
