-- Les 3 lignes ci-dessous font que dbt créera une table et non une vue

{{ config(
    materialized='table'
) }}

-- je vais récupérer tous les champs du stg_produits + ajouter le champ 'nombre_fois_favori' créé dans la table mrt_produits_favoris

SELECT 
    p.*,  -- tous les champs de stg_produits
    f.nombre_fois_favori -- je viens ajouter le champ 'nombre_fois_favori' créé dans la table mrt_produits_favoris
FROM {{ ref('stg_produits') }} AS p -- grâce au REF, alors DBT comprend que stg_produits est UPSTREAM et mrt_produits_favoris est DOWNSTREAM ou exécuté après 
LEFT JOIN {{ ref('mrt_produits_favoris') }} AS f -- jointure sur l'id du produit
    ON p.id = f.produit_id