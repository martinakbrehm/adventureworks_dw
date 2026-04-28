with
    stg_salesorderheadersalesreason as (
    select 
        distinct salesorderid,
        salesreasonid
    from {{ ref('stg_salesorderheadersalesreason') }}
),

    stg_salesreason as (
    select 
        salesreasonid,
        salesreason_name,
        reasontype
    from {{ ref('stg_salesreason') }}
),

    stg_orderreasons as (
    select
        stg_salesorderheadersalesreason.salesorderid,
        LISTAGG(stg_salesreason.salesreason_name, ' | ') WITHIN GROUP (ORDER BY stg_salesreason.salesreason_name) as salesreasonname, 
        LISTAGG(stg_salesreason.reasontype, ' | ') WITHIN GROUP (ORDER BY stg_salesreason.reasontype) as reasontype
    from stg_salesorderheadersalesreason 
    left join stg_salesreason 
        on stg_salesorderheadersalesreason.salesreasonid = stg_salesreason.salesreasonid
    group by stg_salesorderheadersalesreason.salesorderid
),

    transformed_data as (
    select
        {{ dbt_utils.generate_surrogate_key(['salesorderid']) }} as reasonsk,
        salesorderid,
        reasontype,
        case 
            when salesreasonname = 'Manufacturer | Quality' then 'Fabricante | Qualidade'
            when salesreasonname = 'On Promotion' then 'Em promoção'
            when salesreasonname = 'Review' then 'Análise'
            when salesreasonname = 'Price' then 'Preço'
            when salesreasonname = 'Price | On Promotion' then 'Preço | Em promoção'
            when salesreasonname = 'Manufacturer' then 'Fabricante'
            when salesreasonname = 'Price | On Promotion | Other' then 'Preço | Em promoção | Outro' 
            when salesreasonname = 'Price | Other' then 'Preço | Outro'
            when salesreasonname = 'Television Advertisement' then 'Anúncio de televisão'
            when salesreasonname = 'Television Advertisement | Other' then 'Anúncio de televisão | Outro'
            when salesreasonname = 'On Promotion | Other' then 'Em promoção | Outro'
            when salesreasonname = 'Manufacturer | Other' then 'Fabricante | Outro'
            else salesreasonname
        end as salesreason_name,       
        case when salesreasonname like '%Price%' then 1 else 0 end           as price,
        case when salesreasonname like '%Manufacturer%' then 1 else 0 end    as manufacturer,
        case when salesreasonname like '%Quality%' then 1 else 0 end         as quality,
        case when salesreasonname like '%On Promotion%' then 1 else 0 end    as promotion,
        case when salesreasonname like '%Review%' then 1 else 0 end          as review,
        case when salesreasonname like '%Other%' then 1 else 0 end           as other,
        case when salesreasonname like '%Television Advertisement%' then 1 else 0 end as television
    from stg_orderreasons
)

select *
from transformed_data
