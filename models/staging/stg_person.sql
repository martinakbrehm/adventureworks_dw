with 
    person as (
        select 
            businessentityid 
            , case
                when persontype  = 'SC' then 'Contato da loja' 
                when persontype  = 'IN' then 'Cliente individual' 
                when persontype  = 'SP' then 'Vendedor'
                when persontype  = 'EM' then 'Funcionário' 
                when persontype  = 'VC' then 'Contato do fornecedor'
                when persontype  = 'GC' then 'Contato geral'
                else 'Desconhecido'
            end as persontype
            , namestyle
            , title
            , firstname
            , middlename
            , lastname
            , suffix
            , emailpromotion
            , additionalcontactinfo
            , demographics
            , rowguid
            , date(modifieddate) as modifieddate
        from {{ source('person', 'person') }}
    )

select *
from person