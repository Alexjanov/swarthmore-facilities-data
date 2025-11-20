    select
        work_order_no
        , title
        , wo_status
        , priority
        , work_type
        , source_type
        , cost_center
        , origin_type
        , originator
        , originator_pm
        , originated_at
        , expected_at
        , assigned_at
        , completed_at
        , labor_hours
        , labor_cost
        , part_cost
        , other_hours
        , other_cost
        , part_non_inventory_cost
        , estimated_hours
        , estimated_cost
        , work_requested
        , action_taken
        , comment
        , source_estimated_hours
        , location_name
        , asset_name
from {{ ref('fct_all_work_orders') }}