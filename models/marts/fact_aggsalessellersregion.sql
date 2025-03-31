with 
    stg_salesorderheader as (
        select 
            distinct(salesorderid),
            salespersonid,
            territoryid
        from {{ ref('stg_salesorderheader') }}
    )

    , stg_salesperson as (
        select 
            businessentityid
        from {{ ref('stg_salesperson') }}
    )

    , stg_employee as (
        select 
            businessentityid
            , loginid
            , jobtitle
            , gender
            , currentflag
        from {{ ref('stg_employee') }}
    )

    , stg_salesterritory as (
        select 
            territoryid
            , territory_name
            , countryregioncode
        from {{ ref('stg_salesterritory') }}
    )

    , stg_countryregion as (
        select 
            countryregioncode
            , countryregion_name
        from {{ ref('stg_countryregion') }}
    )

    , transformed_data as (
        select
            stg_salesorderheader.salespersonid
            , stg_salesorderheader.salesorderid
            , stg_employee.loginid
            , stg_employee.jobtitle
            , stg_employee.gender
            , stg_employee.currentflag
            , stg_countryregion.countryregion_name
        from stg_salesorderheader
        left join stg_salesperson
            on stg_salesorderheader.salespersonid = stg_salesperson.businessentityid
        left join stg_employee
            on stg_salesperson.businessentityid = stg_employee.businessentityid
        left join stg_salesterritory
            on stg_salesorderheader.territoryid = stg_salesterritory.territoryid
        left join stg_countryregion
            on  stg_salesterritory.countryregioncode = stg_countryregion.countryregioncode
        where stg_salesorderheader.salespersonid is not null
    )

    , aggregated_data as (
        select
            {{ dbt_utils.generate_surrogate_key([
                'countryregion_name'
                , 'jobtitle'
                , 'gender']) 
            }} as agg_salesregionpersonsk
            , countryregion_name
            , jobtitle
            , gender
            , count(salesorderid) as totalsalesorders
        from transformed_data
        group by
            countryregion_name
            , jobtitle
            , gender
    )

select *
from aggregated_data