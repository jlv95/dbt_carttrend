-- Les 3 lignes ci-dessous font que dbt créera une table et non une vue

{{ config(
    materialized='table'
) }}

-- je vais récupérer tous les champs du stg_produits  dans la table mrt_produits_favoris
{{ config(
    materialized='table'
) }}

SELECT
    -- tous les champs existants
    *,
    
    -- on remplace la marque vide ou nulle par 'Divers'
    CASE 
        WHEN marque IS NULL OR TRIM(marque) = '' THEN 'Divers'
        ELSE marque
    END AS marque_transformed,

    -- on remplace la catégorie vide ou nulle par la sous-catégorie
    CASE 
        WHEN categorie IS NULL OR TRIM(categorie) = '' THEN sous_categorie
        ELSE categorie
    END AS sous_categorie_transformee

FROM {{ ref('stg_produits') }}
