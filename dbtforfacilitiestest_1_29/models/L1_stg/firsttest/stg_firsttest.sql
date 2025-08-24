with raw_source as (

    select *
    from {{ source('facilitiesfulltestdatabase_1_29', 'firsttest') }}

)

   , final as (select "WorkOrderId"::varchar AS workorderid
    , "WorkOrderNo"::varchar AS workorderno
    , "Name"::varchar AS name
    , "ParentWorkOrderId"::varchar AS parentworkorderid
    , "ParentWorkOrderNo"::varchar AS parentworkorderno
    , "WOStatusId"::varchar AS wostatusid
    , "WOStatusNo"::varchar AS wostatusno
    , "WOStatusName"::varchar AS wostatusname
    , "PriorityId"::varchar AS priorityid
    , "PriorityNo"::varchar AS priorityno
    , "PriorityName"::varchar AS priorityname
    , "WorkCategoryId"::varchar AS workcategoryid
    , "WorkCategoryNo"::varchar AS workcategoryno
    , "WorkCategoryName"::varchar AS workcategoryname
    , "WorkTypeId"::varchar AS worktypeid
    , "WorkTypeNo"::varchar AS worktypeno
    , "WorkTypeName"::varchar AS worktypename
    , "SourceType"::varchar AS sourcetype
    , "ProblemId"::varchar AS problemid
    , "ProblemNo"::varchar AS problemno
    , "ProblemName"::varchar AS problemname
    , "CauseId"::varchar AS causeid
    , "CauseNo"::varchar AS causeno
    , "CauseName"::varchar AS causename
    , "CostCenterId"::varchar AS costcenterid
    , "CostCenterNo"::varchar AS costcenterno
    , "CostCenterName"::varchar AS costcentername
    , "ProjectId"::varchar AS projectid
    , "ProjectNo"::varchar AS projectno
    , "ProjectName"::varchar AS projectname
    , "OriginType"::varchar AS origintype
    , "OriginatorId"::varchar AS originatorid
    , "OriginatorUserNo"::varchar AS originatoruserno
    , "OriginatorUserFirstName"::varchar AS originatoruserfirstname
    , "OriginatorUserLastName"::varchar AS originatoruserlastname
    , "OriginatorPMNo"::varchar AS originatorpmno
    , "OriginatorPMName"::varchar AS originatorpmname
    , "DateOriginated"::varchar AS dateoriginated
    , "DateExpected"::varchar AS dateexpected
    , "DateAssigned"::varchar AS dateassigned
    , "DateCompleted"::varchar AS datecompleted
    , "DateUpdated"::varchar AS dateupdated
    , "LaborHours"::varchar AS laborhours
    , "LaborCost"::varchar AS laborcost
    , "PartCost"::varchar AS partcost
    , "OtherHours"::varchar AS otherhours
    , "OtherCost"::varchar AS othercost
    , "PartNonInventoryCost"::varchar AS partnoninventorycost
    , "EstimatedHours"::varchar AS estimatedhours
    , "EstimatedCost"::varchar AS estimatedcost
    , "Downtime"::varchar AS downtime
    , "WorkRequested"::varchar AS workrequested
    , "RequesterAvailabilityNotes"::varchar AS requesteravailabilitynotes
    , "Action"::varchar AS action
   , "Comment":: varchar AS comment
   , "LastSignedOn":: varchar AS lastsignedon
   , "LastSignedBy":: varchar AS lastsignedby
   , "WorkOrderCreationReading":: varchar AS workordercreationreading
   , "WorkOrderTriggerDate":: varchar AS workordertriggerdate
   , "WorkOrderTriggerMeterTitle":: varchar AS workordertriggermetertitle
   , "WorkOrderTriggerAsset":: varchar AS workordertriggerasset
   , "SourceEstimatedHours_1":: varchar AS sourceestimatedhours_1
   , "SiteId_1":: varchar AS siteid_1
   , "SiteNo_1":: varchar AS siteno_1
   , "SiteName_1":: varchar AS sitename_1
   , "LocationId_1":: varchar AS locationid_1
   , "LocationNo_1":: varchar AS locationno_1
   , "LocationSiteName_1":: varchar AS locationsitename_1
   , "LocationName_1":: varchar AS locationname_1
   , "AssetId_1":: varchar AS assetid_1
   , "AssetNo_1":: varchar AS assetno_1
   , "AssetSiteName_1":: varchar AS assetsitename_1
   , "AssetName_1":: varchar AS assetname_1
   , "MeterTitleId_1":: varchar AS metertitleid_1
   , "MeterTitleNo_1":: varchar AS metertitleno_1
   , "MeterTitleName_1":: varchar AS metertitlename_1

from raw_source

    )

select * from final