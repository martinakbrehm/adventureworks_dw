with 
    address as (
        select 
            addressid 
            , addressline1
            , addressline2
            , city
            , stateprovinceid 
            , postalcode
            , spatiallocation
            , rowguid
            , date(modifieddate) as modifieddate
        from {{ source('person', 'address') }}
    )

select *
from address