with 
    store as (
        select 
            businessentityid 
            , name as namestore
            , salespersonid 
            , demographics
            , rowguid
            , date(modifieddate) as modifieddate
        from {{ source('sales', 'store') }}
    )

select *
from store