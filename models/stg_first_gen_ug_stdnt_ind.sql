with cmp as (
    select
        emplid,
        -- Only consider the relevant ratings
        max(rating_cmp_value) as max_rating_cmp_value
    from {{ source('cs', 'ps_adm_appl_cmp') }}
    where
        rating_cmp = 'LVLED'
        and rating_cmp_value <> 0
    group by emplid
),

student_first_gen as (
    select
        emplid,
        case
            when coalesce(max_rating_cmp_value, 0) between 202 and 206 then 'Y'
            else 'N'
        end as first_gen_stdnt_ind
    from cmp
)

select *
from student_first_gen
