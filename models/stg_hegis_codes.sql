{%- set my_source = 'cs' -%}
{%- set my_table = 'ps_hegis_code_tbl' -%}

{{ current_effdt(
    source(my_source, my_table),
    keys=['hegis_code']
) }}