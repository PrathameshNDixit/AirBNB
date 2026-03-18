{% macro trimmer(col_name) %}
  {{col_name|trim|upper}}
{% endmacro %}