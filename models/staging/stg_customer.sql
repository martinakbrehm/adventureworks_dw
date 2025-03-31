with 
    customer as (
        select 
            customerid 
            , personid
            , storeid 
            , territoryid 
            , rowguid
            , date(modifieddate) as modifieddate 
        from {{ source('sales', 'customer') }}
    )

select *
from customer