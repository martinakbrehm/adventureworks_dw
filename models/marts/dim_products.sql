with 
    stg_salesorderdetail as (
        select 
            productid
        from {{ ref('stg_salesorderdetail') }}
        where productid is not null
        group by productid
    )
    
    , stg_product as (
        select 
            productid,
            productsubcategoryid,
            product_name
        from {{ ref('stg_product') }}
    )

    , stg_productsubcategory as (
        select 
            productsubcategoryid,
            productcategoryid,
            productsubcategory_name
        from {{ ref('stg_productsubcategory') }}
    )

    , stg_productcategory as (
        select 
            productcategoryid,
            productcategory_name
        from {{ ref('stg_productcategory') }}
    )

    , transformed_data as (
        select
            {{ dbt_utils.generate_surrogate_key(['stg_salesorderdetail.productid']) }} as productsk,
            stg_salesorderdetail.productid,
            stg_product.product_name,
            stg_productsubcategory.productsubcategory_name,
            stg_productcategory.productcategory_name
        from stg_salesorderdetail 
        left join stg_product 
            on stg_salesorderdetail.productid = stg_product.productid
        left join stg_productsubcategory 
            on stg_product.productsubcategoryid = stg_productsubcategory.productsubcategoryid
        left join stg_productcategory 
            on stg_productsubcategory.productcategoryid = stg_productcategory.productcategoryid    
    )

select *
from transformed_data
