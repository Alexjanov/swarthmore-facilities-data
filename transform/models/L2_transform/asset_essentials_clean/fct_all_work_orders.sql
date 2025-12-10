with first as (

    select
        work_order_no
        , name as title
{#        , parent_work_order_id#}
{#        , parent_work_order_no#}
{#        , wo_status_id#}
{#        , wo_status_no#}
        , wo_status_name as wo_status
{#        , priority_id#}
{#        , priority_no#}
        , priority_name as priority
{#        , work_category_id#}
{#        , work_category_no#}
        , work_category_name as work_category
{#        , work_type_id#}
{#        , work_type_no#}
        , work_type_name as work_type
        , source_type
{#        , problem_id#}
{#        , problem_no#}
{#        , problem_name#}
{#        , cause_id#}
{#        , cause_no#}
{#        , cause_name#}
{#        , cost_center_id#}
{#        , cost_center_no#}
        , cost_center_name as cost_center
{#        , project_id#}
{#        , project_no#}
{#        , project_name#}
        , origin_type
{#        , originator_id#}
{#        , originator_user_no#}
        , originator_user_first_name
        , originator_user_last_name
        , concat_ws(' ', originator_user_first_name, originator_user_last_name) as originator
{#        , originator_pm_no#}
        , originator_pm_name as originator_pm
        , date_originated as originated_at
        , date_expected as expected_at
        , date_assigned as assigned_at
        , date_completed as completed_at
{#        , date_updated#}
        , labor_hours
        , labor_cost
        , part_cost
        , other_hours
        , other_cost
        , part_non_inventory_cost
        , estimated_hours
        , estimated_cost
{#        , down_time#}
        , work_requested
{#        , requester_availability_notes#}
        , action_taken
        , comment
{#        , last_signed_on#}
{#        , last_signed_by#}
{#        , work_order_creation_reading#}
{#        , work_order_trigger_date#}
{#        , work_order_trigger_meter_title#}
{#        , work_order_trigger_asset#}
        , source_estimated_hours_1 as source_estimated_hours
{#        , site_id_1#}
{#        , site_no_1#}
{#        , site_name_1 as site_name#}
{#        , location_id_1#}
{#        , location_no_1#}
{#        , location_site_name_1#}
        , location_name_1 as location_name
{#        , asset_id_1#}
{#        , asset_no_1#}
{#        , asset_site_name_1#}
        , asset_name_1 as asset_name
{#        , meter_title_id_1#}
{#        , meter_title_no_1#}
{#        , meter_title_name_1#}

    from {{ ref("stg_full_import") }}
)

, second as (
    select
        work_order_no
        , title
        , wo_status
        , priority
        , work_type
        , work_category
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
from first
)

select * from second
