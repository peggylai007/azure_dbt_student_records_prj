{%- set my_source = 'cs' -%}
{%- set my_table = 'ps_acad_prog_tbl' -%}

{{ current_effdt(
    source(my_source, my_table),
    keys=['acad_prog']
) }}