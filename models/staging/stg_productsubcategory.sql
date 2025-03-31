with 
productsubcategory as (
    select 
        productsubcategoryid, 
        productcategoryid, 
        case
            when name = 'Bib-Shorts' then 'Shorts'
            when name = 'Bike Racks' then 'Bicicletários'
            when name = 'Bike Stands' then 'Suportes para bicicletas'
            when name = 'Bottom Brackets' then 'Colchetes Inferiores'
            when name = 'Bottles and Cages' then 'Garrafas e gaiolas'
            when name = 'Brakes' then 'Freios'
            when name = 'Caps' then 'Cápsulas'
            when name = 'Chains' then 'Correntes'
            when name = 'Cleaners' then 'Limpadores'
            when name = 'Cranksets' then 'Pedaleiras'
            when name = 'Derailleurs' then 'Desviadores'
            when name = 'Fenders' then 'Pára-lamas'
            when name = 'Forks' then 'Garfos'
            when name = 'Gloves' then 'Luvas'
            when name = 'Headsets' then 'Fones de ouvido'
            when name = 'Helmets' then 'Capacetes'
            when name = 'Hydration Packs' then 'Pacotes de hidratação'
            when name = 'Jerseys' then 'Camisas'
            when name = 'Lights' then 'Luzes'
            when name = 'Locks' then 'Fechaduras'
            when name = 'Mountain Bikes' then 'Bicicletas de montanha'
            when name = 'Mountain Frames' then 'Quadros de montanha'
            when name = 'Panniers' then 'Cestos'
            when name = 'Pedals' then 'Pedais'
            when name = 'Pumps' then 'Bombas'
            when name = 'Road Bikes' then 'Bicicletas de estrada'
            when name = 'Road Frames' then 'Quadros de estrada'
            when name = 'Saddles' then 'Selas'
            when name = 'Shorts' then 'Shorts'
            when name = 'Socks' then 'Meias'
            when name = 'Tights' then 'Meia-calça'
            when name = 'Tires and Tubes' then 'Pneus e Câmaras de Ar'
            when name = 'Touring Bikes' then 'Bicicletas de turismo'
            when name = 'Touring Frames' then 'Quadros de turismo'
            when name = 'Vests' then 'Coletes'
            when name = 'Wheels' then 'Rodas'
            else 'Outro'  
        end as productsubcategory_name,
        rowguid,
        date(modifieddate) as modifieddate 
    from {{ source('production', 'productsubcategory') }}
)

select *
from productsubcategory
