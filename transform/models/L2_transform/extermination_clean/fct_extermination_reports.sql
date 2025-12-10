with final as (

    select
        date_at
        , category
        , address
        , location
        , room
        , pest
        , report
        , reporter
        , action_a
        , action_b
        , addtl_info

    from {{ ref("stg_extermination") }}
)

select * from final
