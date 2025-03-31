with 
    salesreason as (
        select 
            salesreasonid 
            , name as salesreason_name
            , reasontype 
            , date(modifieddate) as modifieddate 
        from {{ source('sales', 'salesreason') }}
    )

select *
from salesreason