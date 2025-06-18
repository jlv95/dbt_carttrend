-- Les 3 lignes ci-dessous font que dbt créera une table et non une vue

{{ config(
    materialized='table'
) }}

SELECT 
    id_promotion,
    id_produit,
    type_promotion,

    -- Nettoyage de la colonne valeur_promotion : on traite les % ou les montants (avec € et virgules)
    
    CASE -- j'identifie 2 cas : promotion en pourcentage (cas 1), promotion en remise fixe (cas 2)
        WHEN type_promotion = 'Pourcentage' THEN -- cas (1) le pourcentage 
            SAFE_CAST(REPLACE(valeur_promotion, '%', '') AS FLOAT64) / 100 -- je supprime le %, je convertis en FLOAT et je divise par 100
        WHEN type_promotion = 'Remise fixe' THEN -- cas (2) la remise fixe
            SAFE_CAST(
                REGEXP_REPLACE(REPLACE(valeur_promotion, ',', '.'), r'[^\d\.]', '') -- je remplace les virgules par des points, je supprime tous les caractères non désirés
                AS FLOAT64
            )
        ELSE NULL
    END AS valeur_promotion,

    date_debut,
    date_fin,
    responsable_promotion

FROM {{ ref('stg_promotions') }}

