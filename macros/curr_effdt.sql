{% macro current_effdt(
    source_table, 
    keys, 
    effdt_col='effdt', 
    effseq_col=None, 
    effstatus_col=None, 
    filter=None
) %}
select
  *
from {{ source_table }}
where
  {{ effdt_col }} <= current_timestamp
  {% if filter %}
    and ({{ filter }})
  {% endif %}
{% endmacro %}