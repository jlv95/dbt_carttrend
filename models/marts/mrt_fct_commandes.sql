-- Les 3 lignes ci-dessous font que dbt créera une table et non une vue

{{ config(
    materialized='table'
) }}

-- Le but de cette table est de créer une table de faits, qui joint les données de commandes et details_commandes 

SELECT
    c.id_commande,
    c.id_client,
    c.id_entrepot_depart,
    c.date_commande,
    c.statut_commande,

    -- Transformation conditionnelle de id_promotion_appliquee : 'P099' =>  'PROM099'
    CASE 
        WHEN TRIM(c.id_promotion_appliquee) IS NOT NULL AND TRIM(c.id_promotion_appliquee) != '' 
        THEN CONCAT('PROM', SUBSTR(c.id_promotion_appliquee, 2))
        ELSE NULL
    END AS id_promotion_appliquee,

    c.mode_de_paiement,
    c.numero_tracking, 
    c.date_livraison_estimee,
    d.id_produit, 
    d.quantite, 
    d.emballage_special

FROM {{ ref('stg_commandes') }} AS c
LEFT JOIN {{ ref('stg_details_commandes') }} AS d
USING (id_commande)
