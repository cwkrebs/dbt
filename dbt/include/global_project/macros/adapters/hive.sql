{% macro hive__create_table_as(temporary, relation, sql) -%}
  create {% if temporary: -%}temporary{%- endif %} table
    {{ relation.include(schema=(not temporary)) }}
  as
    {{ sql }}
{% endmacro %}

{% macro hive__create_view_as(relation, sql) -%}
  create or replace view {{ relation }} as
    {{ sql }}
{% endmacro %}
