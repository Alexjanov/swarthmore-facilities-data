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
from {{ ref('fct_extermination_reports') }}
