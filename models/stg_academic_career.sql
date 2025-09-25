{%- set my_source = 'cs' -%}
{%- set my_table = 'ps_acad_car_tbl' -%}

{{ current_effdt(
    source(my_source, my_table),
    keys=['acad_career'],
    filter="institution = 'UMICH'"
) }}