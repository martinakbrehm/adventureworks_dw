with
    stateprovince as (
        select 
            stateprovinceid 
            , stateprovincecode 
            , countryregioncode 
            , isonlystateprovinceflag
            , name as stateprovince_name
            , territoryid 
            , rowguid
            , date(modifieddate) as modifieddate
        from {{ source('person', 'stateprovince') }}
    )

select *
from stateprovince