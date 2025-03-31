with 
    salesperson as (
        select 
            businessentityid 
            , territoryid 
            , salesquota
            , bonus
            , commissionpct
            , salesytd
            , saleslastyear
            , rowguid
            , date(modifieddate) as modifieddate
        from {{ source('sales', 'salesperson') }}
    )

select *
from salesperson