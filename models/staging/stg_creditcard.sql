with 
    creditcard as (
        select 
            creditcardid
            , cardtype
            , cardnumber
            , expmonth
            , expyear
            , date(modifieddate) as modifieddate
        from {{ source('sales', 'creditcard') }}
    )

select *
from creditcard