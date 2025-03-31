with 
    product as (
        select 
            productid 
            , name as product_name
            , productnumber 
            , makeflag
            , finishedgoodsflag
            , color
            , safetystocklevel
            , reorderpoint
            , standardcost
            , listprice
            , size 
            , sizeunitmeasurecode
            , weightunitmeasurecode
            , weight
            , daystomanufacture
            , productline
            , class
            , style
            , productsubcategoryid
            , productmodelid
            , date(sellstartdate) as sellstartdate
            , date(sellenddate) as sellenddate
            , discontinueddate
            , rowguid
            , date(modifieddate) as modifieddate
        from {{ source('production', 'product') }}
    )

select *
from product