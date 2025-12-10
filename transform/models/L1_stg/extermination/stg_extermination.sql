with raw_source as (

    select *
    from {{ source('raw', 'extermination') }}

)

   , final as (select
    date::          timestamp as date_at
    , category::    varchar as category
    , address::     varchar as address
    , location::    varchar as location
    , room::        varchar as room
    , pest::        varchar as pest
    , report::      varchar as report
    , reporter::    varchar as reporter
    , "action a"::  varchar as action_a
    , "action b"::  varchar as action_b
    , "addtl info"::varchar as addtl_info

from raw_source

    )

select * from final
