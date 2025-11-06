{% macro generate_surrogate_key(columns) %}
    md5(concat({% for column in columns %}{{ column }}{% if not loop.last %}, '|', {% endif %}{% endfor %}))
{% endmacro %}
