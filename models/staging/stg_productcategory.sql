with 
    productcategory as (
        select 
            productcategoryid 
            , case
                when name = 'Accessories' then 'Acessórios'
                when name = 'Bikes' then 'Bicicletas'
                when name = 'Clothing' then 'Roupas'
                when name = 'Components' then 'Componentes'
            end as productcategory_name
            , rowguid
            , date(modifieddate) as modifieddate
        from {{ source('production', 'productcategory') }}
    )

select *
from productcategory
