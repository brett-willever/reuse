SELECT
    {{ generate_surrogate_key('TO_JSON_STRING(agr)') }} AS surrogate_key,
    agr.*
FROM
    {{ source(
        'staging_us',
        'academic_graduation_rate'
    ) }} AS agr