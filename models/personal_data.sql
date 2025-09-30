with personal_data as (

    select
        pd.emplid,
        v.first_name   as first_name,
        v.middle_name  as middle_name,
        v.last_name    as last_name,
        v.name         as name,
        v.name_prefix  as name_prefix,
        n.descr        as name_prefix_descr,
        n.descrshort   as name_prefix_descrshort, 
        v.name_suffix  as name_suffix,
        pd.sex,
        {{ xlat('sex', 'pd.sex') }} as sex_descrshort,
        to_date(date_format(pd.birthdate, 'dd-MMM-yyyy'), 'dd-MMM-yyyy') as birthdate,
        p.dt_of_death,
        pd.ferpa,
        pd.va_benefit,
        pd.campus_id,
        pd.name        as primary_name,
        pd.name_prefix as primary_name_prefix,
        pn.descr       as primary_name_prefix_descr,
        pn.descrshort  as primary_name_prefix_descrshort, 
        pd.name_suffix as primary_name_suffix,
        pd.first_name  as primary_first_name,
        pd.middle_name as primary_middle_name,
        pd.last_name   as primary_last_name,
        eth.out_eth_grp_cd,
        eth.out_eth_grp_cd_desc_l,
        eth.out_eth_grp_cd_desc_s,
        eth.out_eth_grp,
        eth.out_eth_multi,
        eth.out_eth_hisp_flag,
        eth.out_eth_validated,
        flterm.first_term_attended,
        flterm.first_term_attended_descrshort,
        flterm.last_term_attended,
        flterm.last_term_attended_descrshort,
        fgen.first_gen_stdnt_ind

    from {{ source('cs', 'ps_personal_data') }} pd
    join {{ source('cs', 'ps_acad_prog') }} car2
        on pd.emplid = car2.emplid
    join {{ source('cs', 'ps_person') }} p
        on pd.emplid = p.emplid
    join {{ source('cs', 'ps_m_cc_ordrnam_vw') }} v
        on pd.emplid = v.emplid
    join {{ source('cs', 'ps_name_prefix_tbl') }} as n
        on v.name_prefix = n.name_prefix
    join {{ source('cs', 'ps_name_prefix_tbl') }} as pn
        on pd.name_prefix = pn.name_prefix
    left join {{ ref('stg_ethnic') }} eth
        on pd.emplid = eth.emplid
    left join stg_first_last_term_attended flterm 
        on pd.emplid = flterm.emplid  
    left join stg_first_gen_ug_stdnt_ind fgen
        on pd.emplid = fgen.emplid  
),

final as (

    select distinct *
    from personal_data

)

select *
from final
