with
  subject as (select * from {{ source('cs', 'ps_subject_tbl') }}),
  org as (select * from {{ ref('stg_academic_organizations') }}),
  cip as (select * from {{ ref('stg_cip_codes') }}),
  hegis as (select * from {{ ref('stg_hegis_codes') }}),

  final as (
    select
      subject.subject,
      subject.effdt as subject_effdt,
      subject.eff_status as subject_eff_status,
      subject.descr as subject_descr,
      subject.descrshort as subject_descrshort,
      org.acad_org as deptid,
      org.descrshort as dept_descrshort,
      org.descrformal as dept_descrformal,
      cip.cip_code,
      cip.descr as cip_descr,
      cip.descr254 as cip_descr254,
      hegis.hegis_code,
      hegis.descr as hegis_descr,
      hegis.descr60 as hegis_descr60
    from subject
      left join org on
        subject.acad_org = org.acad_org
      left join cip on
        subject.cip_code = cip.cip_code
      left join hegis on
        subject.hegis_code = hegis.hegis_code
  )

select * from final