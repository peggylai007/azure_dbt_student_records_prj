{% macro xlat(field_name, field_value, long=False) -%}

  {%- if field_name is string -%}
    {%- set keys = [field_name] -%}
  {%- else -%}
    {% set keys = field_name -%}
  {%- endif -%}


  (
    select {% if long %}xlatlongname{% else %}xlatshortname{% endif %}
    from {{ source('cs', 'psxlatitem') }}
    where
      fieldname in ({% for key in keys -%}'{{ key | upper }}'{% if not loop.last -%}, {% endif -%}{% endfor -%}) and
      fieldvalue = {{ field_value }} limit 1
  )

{%- endmacro %}