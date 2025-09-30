with final as (
   select
     -- Use subqueries here, not the macro!
     M1.EMPLID,
     first_value(M1.STRM) over (partition by M1.EMPLID order by M1.STRM asc) as first_term_attended,
     first_value(M2.DESCRSHORT) over (partition by M1.EMPLID order by M1.STRM asc) as first_term_attended_descrshort,
     last_value(M1.STRM, true)
       over (partition by M1.EMPLID
             order by
                 case when to_date(M2.TERM_BEGIN_DT) <= current_date() then M1.STRM end asc
             rows between unbounded preceding and unbounded following)
         as last_term_attended,
     last_value(M2.DESCRSHORT, true)
       over (partition by M1.EMPLID
             order by
                 case when to_date(M2.TERM_BEGIN_DT) <= current_date() then M1.STRM end asc
             rows between unbounded preceding and unbounded following)
         as last_term_attended_descrshort
   from {{ source('cs', 'ps_stdnt_car_term') }} M1
   join {{ source('cs', 'ps_term_tbl') }} M2
     on M1.INSTITUTION = M2.INSTITUTION
    and M1.ACAD_CAREER = M2.ACAD_CAREER
    and M1.STRM = M2.STRM
   where
     M1.INSTITUTION = 'UMICH'
     and M1.M_SR_REG_STATUS = 'RGSD'
)

select * from final