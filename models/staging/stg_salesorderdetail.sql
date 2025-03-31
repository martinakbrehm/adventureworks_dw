with 
    salesorderdetail as (
        select 
            salesorderid 
            , salesorderdetailid 
            , carriertrackingnumber
            , orderqty
            , productid 
            , specialofferid
            , unitprice
            , unitpricediscount
            , rowguid
            , date(modifieddate) as modifieddate
        from {{ source('sales', 'salesorderdetail') }}
    )

select *
from salesorderdetail