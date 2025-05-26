-- TO DO : Ajouter une colonne 'nom de produit' 

-- je crée une table produits_favoris, qui va me donner par produit, le nombre de fois où il a été mis en favori

-- Les 3 lignes ci-dessous font que dbt créera une table et non une vue

{{ config(
    materialized='table'
) }}

SELECT 
    TRIM(produit_id) AS produit_id,
    COUNT(*) AS nombre_fois_favori
FROM {{ ref('stg_clients') }},
UNNEST(SPLIT(favoris, ',')) AS produit_id 
GROUP BY produit_id


