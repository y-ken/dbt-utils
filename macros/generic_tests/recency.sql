{% test recency(model, field, datepart, interval, ignore_time_component=False, group_by_columns=[], row_condition=None) %}
  {{ return(adapter.dispatch('test_recency', 'dbt_utils')(model, field, datepart, interval, ignore_time_component, group_by_columns, row_condition)) }}
{% endtest %}

{% macro default__test_recency(model, field, datepart, interval, ignore_time_component, group_by_columns, row_condition) %}

{% set threshold = 'cast(' ~ dbt.dateadd(datepart, interval * -1, dbt.current_timestamp()) ~ ' as ' ~ ('date' if ignore_time_component else dbt.type_timestamp()) ~ ')'  %}

{% if group_by_columns|length() > 0 %}
  {% set select_gb_cols = group_by_columns|join(' ,') + ', ' %}
  {% set groupby_gb_cols = 'group by ' + group_by_columns|join(',') %}
{% endif %}


with recency as (

    select 

      {{ select_gb_cols }}
      {% if ignore_time_component %}
        cast(max({{ field }}) as date) as most_recent
      {%- else %}
        max({{ field }}) as most_recent
      {%- endif %}

    from {{ model }}

    {% if row_condition %}
    where {{ row_condition }}
    {% endif %}

    {{ groupby_gb_cols }}

)

select

    {{ select_gb_cols }}
    most_recent,
    {{ threshold }} as threshold

from recency
where most_recent < {{ threshold }}

{% endmacro %}
