with 
    stg_orderheader as (
        select 
            distinct billtoaddressid
            , territoryid
        from {{ ref('stg_salesorderheader') }}
    ),

    stg_address as (
        select 
            distinct addressid
            , city
            , stateprovinceid
            , postalcode
        from {{ ref('stg_address') }}
    ),

    stg_stateprovince as (
        select 
            distinct stateprovinceid
            , stateprovincecode
            , countryregioncode
            , stateprovince_name
        from {{ ref('stg_stateprovince') }}
    ),

    stg_salesterritory as (
        select 
            distinct territoryid
            , territory_name
            , countryregioncode
        from {{ ref('stg_salesterritory') }}
    ),

    stg_countryregion as (
        select 
            distinct countryregioncode
            , countryregion_name
        from {{ ref('stg_countryregion') }}
    ),

    transformed_data as (
        select
            {{ dbt_utils.generate_surrogate_key(['stg_orderheader.billtoaddressid']) }} as locationsk
            , stg_orderheader.billtoaddressid as addressid
            , stg_address.city
            , stg_address.postalcode
            , stg_stateprovince.stateprovincecode
            , stg_stateprovince.stateprovince_name
            , stg_salesterritory.territory_name
            , stg_countryregion.countryregioncode
            , stg_countryregion.countryregion_name
        from stg_orderheader 
        left join stg_address 
            on stg_orderheader.billtoaddressid = stg_address.addressid
        left join stg_stateprovince
            on stg_address.stateprovinceid = stg_stateprovince.stateprovinceid
        left join stg_salesterritory
            on stg_orderheader.territoryid = stg_salesterritory.territoryid
        left join stg_countryregion
            on stg_stateprovince.countryregioncode = stg_countryregion.countryregioncode
    )

select *
from transformed_data
