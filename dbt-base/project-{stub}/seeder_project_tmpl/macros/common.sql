{% macro generate_base_model_query(
    input_reference,
    columns
  ) %}
  WITH ab1 AS (
    SELECT
      {% for column in columns %}
        CASE
          WHEN safe_cast(
            {{ column }} AS STRING
          ) = '*' THEN CAST(
            '0' AS STRING
          )
          ELSE CAST(
            {{ column }} AS STRING
          )
        END AS {{ column }}

        {% if not loop.last %},
        {% endif %}
      {% endfor %},
      MD5(
        CAST({% for column in columns %}
          COALESCE(CAST({{ column }} AS STRING), '') {% if not loop.last %}
            || '-' ||
          {% endif %}
        {% endfor %}

        AS STRING)
      ) AS _dvp_id,
      current_datetime() AS _dvp_emitted_at
    FROM
      {{ input_reference }}
  ),
  ab2 AS (
    SELECT
      {% for column in columns %}
        CAST(
          {{ column }} AS STRING
        ) AS {{ column }}
        _casted

        {% if not loop.last %},
      {% endif %}
    {% endfor %},
    _dvp_emitted_at
    FROM
      ab1
  ),
  ab3 AS (
    SELECT
      *,
      MD5(
        CAST({% for column in columns %}
          COALESCE(CAST({{ column }} AS STRING), '') {% if not loop.last %}
            || '-' ||
          {% endif %}
        {% endfor %}

        AS STRING)
      ) AS _dvp_pk_hashid
    FROM
      ab2
  )
SELECT
  {% for column in columns %}
    {{ column }},
  {% endfor %}

  _dvp_emitted_at,
  _dvp_pk_hashid
FROM
  ab3
{% endmacro %}

{% macro bigquery__merge_tables(
    dataset,
    tables
  ) %}
  WITH aggregated_data AS (
    SELECT
      {% for table in tables %}
        {{ table ~ '.* ,' }}
      {% endfor %}
    FROM
      {% for table in tables %}
        (
          SELECT
            {% for row in run_query(
                'SELECT COLUMN_NAME FROM ' ~ dataset ~ '.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = "' ~ table ~ '"'
              ) %}
              {{ table ~ '.' ~ row.column_name }} AS {{ table ~ '_' ~ row.column_name }},
            {% endfor %}

            '{{ table }}' AS source_table
          FROM
            {{ dataset }}.{{ table }}
        ) {{ table }}

        {% if not loop.last %}
          CROSS JOIN
        {% endif %}
      {% endfor %}
  )
SELECT
  *
EXCEPT(source_table)
FROM
  aggregated_data
{% endmacro %}

{% macro bigquery__generate_union(
    table_configs
  ) %}
  WITH union_tables AS ({% for config in table_configs %}
    {% for table in config.table %}
    SELECT
      {% for column, type in config.columns %}
        CASE
        WHEN safe_cast({{ column }} AS STRING) = '*' THEN CAST('0' AS {{ type }})
        ELSE CAST({{ column }} AS {{ type }})END AS {{ column }}

        {% if not loop.last %},
        {% endif %}
      {% endfor %}
    FROM
      `{{ config.source }}.{{ table }}` {% if not loop.last %}
      UNION ALL
      {% endif %}
    {% endfor %}
  {% endfor %})
SELECT
  *
FROM
  union_tables
{% endmacro %}

{% macro bigquery__generate_query(
    sources,
    columns
  ) %}
SELECT
  {% for column,
    data_type in columns %}
    CAST(
      {{ column }} AS {{ data_type }}
    ) AS {{ column }}

    {% if not loop.last %},
    {% endif %}
  {% endfor %}
FROM
  {% for _,
    tables in sources %}
    {% for table in tables %}
      `{{_}}.{{table}}` AS {{ table }}

      {% if not loop.last %},
      {% endif %}
    {% endfor %}
  {% endfor %}
{% endmacro %}

{% macro bigquery__generate_sql_from_json(contract) %}
  {% set model_contract = fromjson(contract) %}
  {% set model_columns = model_contract.columns %}
  {{ bigquery__generate_query(
    model_contract.source,
    model_columns,
  ) }}
{% endmacro %}

{% macro bigquery__generate_sql_from_yaml(yaml_string) %}
  {% set model_contract = fromyaml(yaml_string) %}
  {% set model_columns = model_contract.columns %}
  {{ bigquery__generate_query(
    model_contract.source,
    model_columns,
  ) }}
{% endmacro %}

{% macro char_replace_cast(
    character,
    replacement,
    source,
    target,
    data_type
  ) %}
  CAST(
    IF(safe_cast({{ source }} AS STRING) = '{{character}}', '{{replacement}}', {{ source }}) AS {{ data_type }}
  ) AS {{ target }}
{% endmacro %}

{% macro generate_profile_report(
    bq_dataset,
    bq_table,
    column_list
  ) %}
  {{ config(
    alias = bq_table + "_profile",
    materialized = "table",
    description = "Table for storing profiling results"
  ) }}

  SELECT
    generate_uuid() AS __key__,
    CURRENT_TIMESTAMP() AS __timestamp__,
    '{{bq_dataset}}.{{bq_table}}' AS __table_ref__,
    column_name,
    MIN(VALUE) AS __min__,
    MAX(VALUE) AS __max__,
    COUNT(NULLIF(VALUE, '')) AS __null_values__,
    COUNT(
      DISTINCT VALUE
    ) AS __cardinality__,
    ROUND(COUNT(DISTINCT VALUE) / COUNT(1) * 100, 2) AS __selectivity__,
    ROUND((COUNT(1) - COUNT(NULLIF(VALUE, ''))) / COUNT(1) * 100, 2) AS __density__
  FROM
    ({% for col in column_list %}
    SELECT
      '{{ col }}' AS column_name, CAST({{ col }} AS STRING) AS VALUE
    FROM
      `{{bq_dataset}}.{{bq_table}}` {% if not loop.last %}
      UNION ALL
      {% endif %}
    {% endfor %})
  GROUP BY
    column_name
{% endmacro %}

{% macro generate_scd_table(
    target_dataset,
    target_table,
    source_dataset,
    source_table,
    scd_key,
    scd_type,
    scd_columns,
    effective_date_column
  ) %}
  {{ config(
    alias = target_table,
    materialized = 'table',
    description = 'SCD Table for ' ~ target_table
  ) }}

  WITH latest_data AS (

    SELECT
      *,
      ROW_NUMBER() over (
        PARTITION BY {{ scd_key }}
        ORDER BY
          {{ effective_date_column }} DESC
      ) AS row_num
    FROM
      {{ ref(
        target_table
      ) }}
  )
SELECT
  CASE
    WHEN row_num = 1
    AND '{{ scd_type }}' = 'Type 1' THEN CURRENT_DATE()
    WHEN row_num = 1
    AND '{{ scd_type }}' = 'Type 2' THEN CURRENT_DATE()
    ELSE NULL
  END AS valid_from,
  CASE
    WHEN '{{ scd_type }}' = 'Type 1' THEN NULL
    WHEN row_num = 1 THEN NULL
    ELSE CURRENT_DATE()
  END AS valid_to,*,
  CASE
    WHEN '{{ scd_type }}' = 'Type 2' THEN COALESCE(LAG({{ effective_date_column }}) over (PARTITION BY {{ scd_key }}
    ORDER BY
      {{ effective_date_column }} DESC), CURRENT_DATE())
      WHEN '{{ scd_type }}' = 'Type 3' THEN LAG(
        {{ effective_date_column }}
      ) over (
        PARTITION BY {{ scd_key }}
        ORDER BY
          {{ effective_date_column }} DESC
      )
      ELSE NULL
  END AS prev_valid_from,
  CASE
    WHEN '{{ scd_type }}' = 'Type 2' THEN NULL
    ELSE COALESCE(LAG({{ effective_date_column }}) over (PARTITION BY {{ scd_key }}
    ORDER BY
      {{ effective_date_column }} DESC), CURRENT_DATE())END AS prev_valid_to
    FROM
      latest_data
{% endmacro %}

{% macro generate_surrogate_key(input_key) -%}
  ABS(
    farm_fingerprint(safe_cast({{ input_key }} AS STRING))
  )
{%- endmacro %}

{% macro scd_model(input_key) %}
  {{ config(
    materialized = 'table',
    unique_key = '_dvp_id',
    cluster_by = '_dvp_emitted_at'
  ) }}

  WITH source_data AS (

    SELECT
      generate_uuid() AS _dvp_id,
      current_datetime() AS _dvp_emitted_at,
      safe_cast(safe_cast(_data) AS STRING) AS _dvp_raw_data
    FROM
      gold_us.{{ model }} AS _data)
    SELECT
      _dvp_id,
      _dvp_emitted_at,
      ABS(farm_fingerprint(_dvp_raw_data)) AS _dvp_hashid,
      current_datetime() AS _dvp_normalized_at,
      _dvp_raw_data AS DATA
    FROM
      source_data
{% endmacro %}
