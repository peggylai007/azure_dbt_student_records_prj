with ethnic_grp_latest as (
    select
        *,
        row_number() over (
            partition by setid, ethnic_grp_cd
            order by effdt desc
        ) as rn
    from {{ source('cs', 'ps_ethnic_grp_tbl') }}
    where setid = 'USA'
      and eff_status = 'A'
      and effdt <= current_date
),

eth as (
    select
        a.emplid,
        a.m_cc_eth_2009rpt,
        a.m_cc_eth_2010rpt,
        a.m_cc_eth_hisp_flag,
        a.m_cc_eth_multi,
        a.eth_validated,
        b.descr50,
        b.descrshort,
        b.ethnic_group
    from {{ source('cs', 'ps_m_cc_eth_rpt_vw') }} a
    left join ethnic_grp_latest b
        on b.ethnic_grp_cd = a.m_cc_eth_2009rpt
        and b.setid = 'USA'
        and b.rn = 1
),

final as (
    select
        emplid,
        case
            when m_cc_eth_2010rpt = 'PACIF' then '7'
            when m_cc_eth_2010rpt = 'HISPA' then '3'
            when m_cc_eth_2010rpt = '2ORMORE' then 'T'
            else ethnic_group
        end as out_eth_grp,
        coalesce(m_cc_eth_2010rpt, '') as out_eth_grp_cd,
        case
            when m_cc_eth_2010rpt = 'PACIF' then 'Native Hawaiian/Oth Pac Island'
            when m_cc_eth_2010rpt = 'HISPA' then 'Hispanic/Latino'
            when m_cc_eth_2010rpt = '2ORMORE' then 'Two or More Races'
            else descr50
        end as out_eth_grp_cd_desc_l,
        case
            when m_cc_eth_2010rpt = 'PACIF' then 'Hawaiian'
            when m_cc_eth_2010rpt = 'HISPA' then 'Hispanic'
            when m_cc_eth_2010rpt = '2ORMORE' then 'Two or More'
            else descrshort
        end as out_eth_grp_cd_desc_s,
        coalesce(m_cc_eth_multi, 'N') as out_eth_multi,
        coalesce(m_cc_eth_hisp_flag, 'N') as out_eth_hisp_flag,
        coalesce(eth_validated, 'N') as out_eth_validated
    from eth
)

select *
from final