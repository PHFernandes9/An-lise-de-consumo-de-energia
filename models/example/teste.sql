with Osbis_2 as (

    select * from {{ref('query')}}
)

SELECT Usina FROM Dataset.Osbis_2
Where Usina = 'Fazenda Vale do Sol'
 