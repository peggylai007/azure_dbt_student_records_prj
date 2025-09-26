
with
  m_sr_cohort as (select * from {{ source('cs', 'ps_m_sr_cohort') }}),  
  term as (select * from {{ source('cs', 'ps_term_val_tbl') }}),
  prog as (select * from {{ ref('stg_acad_prog_tbl') }}),
  		
  final as (
    select 
      m_sr_cohort.m_sr_cohort_system as cohort_system,
      m_sr_cohort.m_sr_cohort_type as cohort_type,
      m_sr_cohort.strm as cohort_term,
      m_sr_cohort.emplid,
      m_sr_cohort.acad_prog as grad_acad_prog,
      {{ xlat('m_sr_cohort_system', 'm_sr_cohort.m_sr_cohort_system', long=true) }} as cohort_system_descr,
      {{ xlat('m_sr_cohort_type', 'm_sr_cohort.m_sr_cohort_type', long=true) }} as cohort_type_descr,
      term_cohort.descr as cohort_term_descr,
      prog.descr as grad_acad_prog_descr,
      m_sr_cohort.acad_career as init_home_acad_career,
      {{ xlat('acad_career', 'm_sr_cohort.acad_career', long=true) }} as init_home_acad_career_descr,
      {{ xlat('acad_career', 'm_sr_cohort.acad_career') }} as init_home_acad_career_descrs,
      m_sr_cohort.from_term as init_home_from_term, 
      term_init.descr as init_home_term,
      m_sr_cohort.adm_appl_nbr as adm_appl_nbr
    from m_sr_cohort 
      left join term term_cohort 
        on m_sr_cohort.strm = term_cohort.strm
      left join prog 
        on m_sr_cohort.acad_prog = prog.acad_prog
      left join term term_init 
        on m_sr_cohort.from_term = term_init.strm
  )

select * from final