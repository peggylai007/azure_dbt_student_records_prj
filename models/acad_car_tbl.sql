select
  acad_career,
  effdt as acad_career_effdt,
  eff_status as acad_career_eff_status,
  descr as acad_career_descr,
  descrshort as acad_career_descrshort
from {{ source('cs','ps_acad_car_tbl') }}