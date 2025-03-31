with 
    salesorderheadersalesreason as (
        select 
            salesorderid 
            , salesreasonid 
            , date(modifieddate) as modifieddate
        from {{ source('sales', 'salesorderheadersalesreason') }}
    )

select *
from salesorderheadersalesreason