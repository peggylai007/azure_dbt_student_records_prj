with
    attributes (select * from {{ source('cs','ps_class_attribute') }}),
    final as (
        select
            crse_id,
            crse_offer_nbr,
            strm as term,
            session_code,
            class_section,
            crse_attr,
            crse_attr_value
        from attributes
    )

select * from final