{%- set my_source = 'cs' -%}
{%- set my_table = 'ps_cip_code_tbl' -%}

{{ current_effdt(
    source(my_source, my_table),
    keys=['cip_code']
) }}