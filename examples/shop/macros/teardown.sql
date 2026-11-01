{# `dbt run-operation teardown` drops everything the example created, in the three layers. #}
{% macro teardown() %}
  {% for layer in ['gold', 'silver', 'bronze'] %}
    {% set schema_relation = api.Relation.create(database=layer, schema='shop') %}
    {% for relation in adapter.list_relations_without_caching(schema_relation) %}
      {% do adapter.drop_relation(relation) %}
      {{ log('dropped ' ~ relation, info=True) }}
    {% endfor %}
    {% do run_query('drop schema if exists ' ~ layer ~ '.shop') %}
  {% endfor %}
{% endmacro %}
