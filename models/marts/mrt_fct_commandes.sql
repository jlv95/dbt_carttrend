-- Les 3 lignes ci-dessous font que dbt créera une table et non une vue

{{ config(
    materialized='table'
) }}

-- le but de cette table est de créer une table de faits, qui joint les données de commandes et details_commandes 
-- cela sert à assurer la schématisation étoile adéquate à Power BI 

SELECT
    *
FROM {{ ref('stg_commandes') }} AS c
JOIN {{ ref('stg_details_commandes') }} AS d
USING (id_commande)
