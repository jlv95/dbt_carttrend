-- Les 3 lignes ci-dessous font que dbt créera une table et non une vue

{{ config(
    materialized='table'
) }}

{{ config(
    materialized='table'
) }}

SELECT 
    id_promotion,
    id_produit,
    type_promotion,

    -- Nettoyage de la colonne valeur_promotion qui comporte soit des % soit des float mais avec la devise
    CASE -- CASE, c'est une condition, similaire à un IF 
        WHEN type_promotion = 'Pourcentage' THEN 
            SAFE_CAST(REPLACE(valeur_promotion, '%', '') AS FLOAT64) / 100 -- Je supprime le sigle %, je convertis en float et je divise par 100 pour respecter le pourcentage
        WHEN type_promotion = 'Remise fixe' THEN 
            SAFE_CAST(REGEXP_REPLACE(valeur_promotion, r'[^\d\.]', '') AS FLOAT64) -- Avec cette regEx, je remplace tout ce qui n'est pas (^) un digit (d) ou un point (.) par '' soit une suppression des caractères non souhaités comme le sigle €
        ELSE NULL
    END AS valeur_promotion,

    date_debut,
    date_fin,
    responsable_promotion

FROM {{ ref('stg_promotions') }}
