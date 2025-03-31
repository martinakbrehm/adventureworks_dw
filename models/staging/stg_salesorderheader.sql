with 
    salesorderheader as (
        select 
            salesorderid 
            , revisionnumber
            , date(orderdate) as orderdate
            , date(duedate) as duedate
            , date(shipdate) as shipdate
            , case
                when status = 1 then 'Em processo'
                when status = 2 then 'Aprovado'
                when status = 3 then 'Pedido em espera'
                when status = 4 then 'Rejeitado'
                when status = 5 then 'Enviado'
                when status = 6 then 'Cancelado'
            end as statussales
            , onlineorderflag
            , purchaseordernumber
            , accountnumber
            , customerid 
            , salespersonid 
            , territoryid 
            , billtoaddressid
            , shiptoaddressid
            , shipmethodid
            , creditcardid 
            , creditcardapprovalcode
            , currencyrateid
            , subtotal
            , taxamt
            , freight
            , totaldue
            , comment
            , rowguid
            , date(modifieddate) as modifieddate
        from {{ source('sales', 'salesorderheader') }}
    )

select *
from salesorderheader