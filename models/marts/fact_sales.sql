with 
    stg_orderheader as (
        select 
            distinct salesorderid
            , orderdate
            , shipdate
            , statussales
            , onlineorderflag
            , customerid
            , salespersonid
            , territoryid
            , billtoaddressid
            , creditcardid
            , subtotal
            , taxamt
            , freight
            , totaldue
        from adventureworks_dw.dbt_transformed.stg_salesorderheader
    ),

    dim_customers as (
        select
            customersk
            , customerid
        from adventureworks_dw.dbt_transformed.dim_customers
    ),

    dim_locations as (
        select
            locationsk
            , addressid
            , city
            , postalcode
            , stateprovince_name
            , territory_name
            , countryregion_name
        from adventureworks_dw.dbt_transformed.dim_locations
    ),

    dim_creditcards as (
        select
            creditcardsk
            , creditcardid
        from adventureworks_dw.dbt_transformed.dim_creditcards
    ),

    dim_reasons as (
        select
            reasonsk
            , salesorderid
            , salesreason_name
            , reasontype
            , Price
            , Manufacturer
            , Quality
            , Promotion
            , Review
            , Other
            , Television
        from adventureworks_dw.dbt_transformed.dim_reasons
    ),

    join_orderheader as (
        select
            stg_orderheader.salesorderid
            , dim_customers.customersk as customerfk
            , dim_locations.locationsk as locationfk
            , dim_creditcards.creditcardsk as creditcardfk
            , dim_reasons.reasonsk as reasonfk
            , stg_orderheader.orderdate
            , stg_orderheader.shipdate
            , stg_orderheader.statussales
            , stg_orderheader.onlineorderflag
            , stg_orderheader.customerid
            , stg_orderheader.salespersonid
            , stg_orderheader.territoryid
            , stg_orderheader.billtoaddressid
            , stg_orderheader.creditcardid
            , stg_orderheader.subtotal 
            , stg_orderheader.taxamt
            , stg_orderheader.freight
            , stg_orderheader.totaldue
        from stg_orderheader
        left join dim_customers 
            on stg_orderheader.customerid = dim_customers.customerid
        left join dim_locations 
            on stg_orderheader.billtoaddressid = dim_locations.addressid
        left join dim_creditcards 
            on stg_orderheader.creditcardid = dim_creditcards.creditcardid
        left join dim_reasons 
            on stg_orderheader.salesorderid = dim_reasons.salesorderid
    ),

    stg_orderdetail as (
        select 
            salesorderid
            , salesorderdetailid
            , orderqty
            , productid
            , unitprice
            , unitpricediscount
            , orderqty * (unitprice - unitpricediscount) as amountpaidproduct
        from adventureworks_dw.dbt_transformed.stg_salesorderdetail
    ),

    dim_products as (
        select
            productsk
            , productid
        from adventureworks_dw.dbt_transformed.dim_products
    ),

    stg_product as (
        select 
            productid
            , standardcost
            , listprice
        from adventureworks_dw.dbt_transformed.stg_product
    ),

    join_orderdetail as (
        select
            stg_orderdetail.salesorderid
            , dim_products.productsk as productfk
            , stg_orderdetail.salesorderdetailid
            , stg_orderdetail.orderqty
            , stg_orderdetail.productid
            , stg_orderdetail.unitprice
            , stg_orderdetail.unitpricediscount
            , stg_orderdetail.amountpaidproduct
            , stg_product.standardcost
            , stg_product.listprice
        from stg_orderdetail
        left join dim_products 
            on stg_orderdetail.productid = dim_products.productid
        left join stg_product 
            on stg_orderdetail.productid = stg_product.productid
    ),

    transformed_data as (
        select
            md5(cast(coalesce(cast(join_orderheader.salesorderid as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(join_orderheader.customerfk as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(join_orderheader.locationfk as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(join_orderheader.creditcardfk as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(join_orderheader.reasonfk as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(join_orderheader.orderdate as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(join_orderdetail.productfk as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as factsalessk
            , join_orderheader.salesorderid
            , join_orderheader.customerfk
            , join_orderheader.locationfk
            , join_orderheader.creditcardfk
            , join_orderheader.reasonfk
            , join_orderdetail.productfk
            , join_orderheader.orderdate
            , join_orderheader.shipdate
            , join_orderheader.statussales
            , join_orderheader.onlineorderflag
            , join_orderheader.subtotal 
            , join_orderheader.taxamt
            , join_orderheader.freight
            , join_orderheader.totaldue
            , join_orderdetail.orderqty
            , join_orderdetail.unitprice
            , join_orderdetail.unitpricediscount
            , join_orderdetail.amountpaidproduct
            , join_orderdetail.standardcost
            , join_orderdetail.listprice
            , dim_locations.city
            , dim_locations.postalcode
            , dim_locations.stateprovince_name
            , dim_locations.territory_name
            , dim_locations.countryregion_name
            , dim_reasons.salesreason_name
            , dim_reasons.reasontype
            , dim_reasons.Price
            , dim_reasons.Manufacturer
            , dim_reasons.Quality
            , dim_reasons.Promotion
            , dim_reasons.Review
            , dim_reasons.Other
            , dim_reasons.Television
        from join_orderdetail
        left join join_orderheader
            on join_orderdetail.salesorderid = join_orderheader.salesorderid
        left join dim_locations
            on join_orderheader.billtoaddressid = dim_locations.addressid
        left join dim_reasons
            on join_orderheader.salesorderid = dim_reasons.salesorderid
        order by factsalessk
    )

select *
from transformed_data
