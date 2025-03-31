with 
    countryregion as (
        select 
            countryregioncode 
            , case
                when name = 'France' then 'França'
                when name = 'Canada' then 'Canadá'
                when name = 'United States' then 'Estados Unidos'
                when name = 'Germany' then 'Alemanha'
                when name = 'United Kingdom' then 'Reino Unido'
                when name = 'Australia' then 'Austrália'
            end as countryregion_name
            , date(modifieddate) as modifieddate
        from {{ source('person', 'countryregion') }}
    )

select *
from countryregion