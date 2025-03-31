with 
    stg_salesorderheader as (
        select 
            distinct creditcardid
        from {{ ref('stg_salesorderheader') }}
        where creditcardid is not null
    ),
    
    stg_creditcard as (
        select 
            creditcardid,
            cardtype,
            expmonth,
            expyear
        from {{ ref('stg_creditcard') }}
    ),

    transformed_data as (
        select
            {{ dbt_utils.generate_surrogate_key(['stg_salesorderheader.creditcardid']) }} as creditcardsk,  -- Gerando chave substituta
            stg_salesorderheader.creditcardid,
            stg_creditcard.cardtype  
        from stg_salesorderheader 
        left join stg_creditcard 
            on stg_salesorderheader.creditcardid = stg_creditcard.creditcardid
    )

select *
from transformed_data
