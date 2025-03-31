with 
    salesterritory as (
        select 
            territoryid 
            , case
                when name = 'Australia' then 'Austrália'
                when name = 'Canada' then 'Canadá'
                when name = 'Germany' then 'Alemanha'
                when name = 'France' then 'França'
                when name = 'United Kingdom' then 'Reino Unido'
                when name = 'Southeast' then 'Sudeste'
                when name = 'Northwest' then 'Noroeste'
                when name = 'Southwest' then 'Sudoeste'
                when name = 'Central' then 'Central'
                when name = 'Northeast' then 'Nordeste'
            end as territory_name
            , countryregioncode 
            , "GROUP" as territorygroup
            , rowguid
            , date(modifieddate) as modifieddate 
        from {{ source('sales', 'salesterritory') }}
    )

select *
from salesterritory