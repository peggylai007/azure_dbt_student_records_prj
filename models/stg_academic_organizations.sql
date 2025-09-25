{%- set my_source = 'cs' -%}
{%- set my_table = 'ps_acad_org_tbl' -%}

{{ current_effdt(
    source(my_source, my_table),
    keys=['acad_org']
) }}
