SELECT 
    {% for column in model.columns %}
        {{safe_cast_string_as_zero(column)}} AS {{ column }} {% if not loop.last %},{% endif %}
     {% endfor %}
FROM `raw_us.essappe2122_school_level`