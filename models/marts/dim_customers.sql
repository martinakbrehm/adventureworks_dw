with 
    stg_orderheader as (
        select 
            distinct customerid
        from {{ ref('stg_salesorderheader') }}
    )

    , stg_customer as (
        select 
            customerid
            , personid
            , storeid
        from {{ ref('stg_customer') }}
    )

    , stg_person as (
        select 
            businessentityid
            , firstname
            , lastname
            , concat(firstname, ' ', lastname) AS fullname
            , persontype
        from {{ ref('stg_person') }}
    )

    , stg_store as (
        select 
            businessentityid
            , namestore
        from {{ ref('stg_store') }}
    )

    , transformed_data as (
        select
            {{ dbt_utils.generate_surrogate_key(['stg_orderheader.customerid']) }} as customersk
            , stg_customer.customerid
            , stg_person.firstname as customerfirstname
            , stg_person.lastname as customerlastname
            , stg_person.fullname as customerfullname
            , stg_person.persontype as customerpersontype
            , stg_store.namestore
        from stg_orderheader 
        left join stg_customer 
            on stg_orderheader.customerid = stg_customer.customerid
         left join stg_person
            on stg_customer.personid = stg_person.businessentityid
        left join stg_store
            on stg_customer.storeid = stg_store.businessentityid     
    )

select *
from transformed_data