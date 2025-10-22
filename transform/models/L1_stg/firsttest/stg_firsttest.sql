with raw_source as (

    select *
    from {{ source('raw', 'facilities') }}

)

   , final as (select
    "WorkOrderNo"::varchar AS work_order_no
    , "Name"::varchar AS name
    , "ParentWorkOrderId"::varchar AS parent_work_order_id
    , "ParentWorkOrderNo"::varchar AS parent_work_order_no
    , "WOStatusId"::varchar AS wo_status_id
    , "WOStatusNo"::varchar AS wo_status_no
    , "WOStatusName"::varchar AS wo_status_name
    , "PriorityId"::varchar AS priority_id
    , "PriorityNo"::varchar AS priority_no
    , "PriorityName"::varchar AS priority_name
    , "WorkCategoryId"::varchar AS work_category_id
    , "WorkCategoryNo"::varchar AS work_category_no
    , "WorkCategoryName"::varchar AS work_category_name
    , "WorkTypeId"::varchar AS work_type_id
    , "WorkTypeNo"::varchar AS work_type_no
    , "WorkTypeName"::varchar AS work_type_name
    , "SourceType"::varchar AS source_type
    , "ProblemId"::varchar AS problem_id
    , "ProblemNo"::varchar AS problem_no
    , "ProblemName"::varchar AS problem_name
    , "CauseId"::varchar AS cause_id
    , "CauseNo"::varchar AS cause_no
    , "CauseName"::varchar AS cause_name
    , "CostCenterId"::varchar AS cost_center_id
    , "CostCenterNo"::varchar AS cost_center_no
    , "CostCenterName"::varchar AS cost_center_name
    , "ProjectId"::varchar AS project_id
    , "ProjectNo"::varchar AS project_no
    , "ProjectName"::varchar AS project_name
    , "OriginType"::varchar AS origin_type
    , "OriginatorId"::varchar AS originator_id
    , "OriginatorUserNo"::varchar AS originator_user_no
    , "OriginatorUserFirstName"::varchar AS originator_user_first_name
    , "OriginatorUserLastName"::varchar AS originator_user_last_name
    , "OriginatorPMNo"::varchar AS originator_pm_no
    , "OriginatorPMName"::varchar AS originator_pm_name
    , "DateOriginated"::varchar AS date_originated
    , "DateExpected"::varchar AS date_expected
    , "DateAssigned"::varchar AS date_assigned
    , "DateCompleted"::varchar AS date_completed
    , "DateUpdated"::varchar AS date_updated
    , "LaborHours"::varchar AS labor_hours
    , "LaborCost"::varchar AS labor_cost
    , "PartCost"::varchar AS part_cost
    , "OtherHours"::varchar AS other_hours
    , "OtherCost"::varchar AS other_cost
    , "PartNonInventoryCost"::varchar AS part_non_inventory_cost
    , "EstimatedHours"::varchar AS estimated_hours
    , "EstimatedCost"::varchar AS estimated_cost
    , "Downtime"::varchar AS down_time
    , "WorkRequested"::varchar AS work_requested
    , "RequesterAvailabilityNotes"::varchar AS requester_availability_notes
    , "Action"::varchar AS action_taken
   , "Comment":: varchar AS comment
   , "LastSignedOn":: varchar AS last_signed_on
   , "LastSignedBy":: varchar AS last_signed_by
   , "WorkOrderCreationReading":: varchar AS work_order_creation_reading
   , "WorkOrderTriggerDate":: varchar AS work_order_trigger_date
   , "WorkOrderTriggerMeterTitle":: varchar AS work_order_trigger_meter_title
   , "WorkOrderTriggerAsset":: varchar AS work_order_trigger_asset
   , "SourceEstimatedHours_1":: varchar AS sourc_eestimated_hours_1
   , "SiteId_1":: varchar AS site_id_1
   , "SiteNo_1":: varchar AS site_no_1
   , "SiteName_1":: varchar AS site_name_1
   , "LocationId_1":: varchar AS location_id_1
   , "LocationNo_1":: varchar AS location_no_1
   , "LocationSiteName_1":: varchar AS location_site_name_1
   , "LocationName_1":: varchar AS location_name_1
   , "AssetId_1":: varchar AS asset_id_1
   , "AssetNo_1":: varchar AS asset_no_1
   , "AssetSiteName_1":: varchar AS asset_site_name_1
   , "AssetName_1":: varchar AS asset_name_1
   , "MeterTitleId_1":: varchar AS meter_title_id_1
   , "MeterTitleNo_1":: varchar AS meter_title_no_1
   , "MeterTitleName_1":: varchar AS meter_title_name_1

from raw_source

    )

select * from final
