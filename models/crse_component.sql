with
    course_components (select * from {{ source('cs', 'ps_crse_component') }}),
    final as (
        select
            cc.crse_id,
            cc.effdt as crse_effdt,
            cc.ssr_component as crse_component,
            {{ xlat('ssr_component', 'cc.ssr_component') }} as crse_component_descrshort,
            cc.optional_section
    from course_components cc
    )

select * from final