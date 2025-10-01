with ranked_first_term_app as (
    -- For each emplid, find the most recent admit_term academic program row
    select
        emplid,
        admit_term,
        adm_appl_nbr,
        effdt,
        effseq,
        row_number() over (
            partition by emplid 
            order by effdt desc, effseq desc, adm_appl_nbr desc
        ) as rn
    from {{ source('cs', 'ps_acad_prog') }}
),

first_term_app as (
    select 
        emplid,
        admit_term,
        adm_appl_nbr as firt_term_adm_appl_nbr
    from ranked_first_term_app
    where rn = 1
),

-- Step 2: Try to get rating by first term's adm_appl_nbr
rating_1 as (
    select
        a.emplid,
        cmp.rating_cmp_value,
        1 as priority
    from {{ source('cs', 'ps_adm_appl_eval') }} eval
    join {{ source('cs', 'ps_adm_appl_cmp') }} cmp
      on eval.emplid = cmp.emplid
     and eval.acad_career = cmp.acad_career
     and eval.stdnt_car_nbr = cmp.stdnt_car_nbr
     and eval.adm_appl_nbr = cmp.adm_appl_nbr
     and eval.appl_prog_nbr = cmp.appl_prog_nbr
     and eval.evaluation_code = cmp.evaluation_code
     and eval.appl_eval_nbr = cmp.appl_eval_nbr
    join first_term_app a
      on eval.emplid = a.emplid
     and eval.adm_appl_nbr = a.firt_term_adm_appl_nbr
    where cmp.rating_cmp = 'EFGI'
    qualify row_number() over (
        partition by eval.emplid
        order by cmp.rating_cmp_value desc, cmp.appl_eval_nbr desc
    ) = 1
),

-- Step 3: If nothing for first term, any adm_appl_nbr for that emplid
rating_2 as (
    select
        eval.emplid,
        cmp.rating_cmp_value,
        2 as priority
    from {{ source('cs', 'ps_adm_appl_eval') }} eval
    join {{ source('cs', 'ps_adm_appl_cmp') }} cmp
      on eval.emplid = cmp.emplid
     and eval.acad_career = cmp.acad_career
     and eval.stdnt_car_nbr = cmp.stdnt_car_nbr
     and eval.adm_appl_nbr = cmp.adm_appl_nbr
     and eval.appl_prog_nbr = cmp.appl_prog_nbr
     and eval.evaluation_code = cmp.evaluation_code
     and eval.appl_eval_nbr = cmp.appl_eval_nbr
    where cmp.rating_cmp = 'EFGI'
    qualify row_number() over (
        partition by eval.emplid
        order by cmp.rating_cmp_value desc, cmp.appl_eval_nbr desc
    ) = 1
),

-- Consolidate: For each emplid, get the preferred (rating_1, else rating_2)
priority_cmp as (
    select * from rating_1
    union all
    select * from rating_2
),

final_cmp as (
    select
        emplid,
        rating_cmp_value
    from (
        select
            *,
            row_number() over (
                partition by emplid
                order by priority
            ) as rn
        from priority_cmp
    )
    where rn = 1
),

-- Step 4: Get description for that rating_cmp_value
descrs as (
    select
        cmp.emplid,
        rvt.descr as est_gross_fam_inc_des,
        row_number() over (
            partition by cmp.emplid
            order by rvt.effdt desc
        ) as rn
    from final_cmp cmp
    join {{ source('cs', 'ps_rating_val_tbl') }} rvt
      on rvt.rating_value = cmp.rating_cmp_value
    where rvt.rating_cmp = 'EFGI'
      and rvt.rating_scheme = 'UGAAPPLF'
)

select
    emplid,
    est_gross_fam_inc_des
from descrs
where rn = 1
