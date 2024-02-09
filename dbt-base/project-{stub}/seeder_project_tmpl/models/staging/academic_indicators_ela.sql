WITH raw_ela AS (
    SELECT
        {% for column in model.columns %}
          {{ safe_cast_string_as_zero(column) }} AS {{ column }}{% if not loop.last %},{% endif %}
        {% endfor %}
    FROM
        {{ source(
            'raw_us',
            'eladownload2023'
        ) }}
)
SELECT
    *,
    {{ metadata_columns('TO_JSON_STRING(raw_ela)') }}
FROM
    raw_ela
