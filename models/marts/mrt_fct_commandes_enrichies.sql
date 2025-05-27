-- Métérialisation d'une table 

{{ config(
    materialized='table'
) }}

-- Le but de cette table est de créer une table de faits commandes, qui joint les données de commandes, details_commandes, promotions, produits, afin de calculer des montants de commandes finaux 
-- Ajout d'un contrôle : les commandes à montant négatif, sont ramenées à 0 par défaut. 

SELECT
    c.id_commande,
    c.id_client,
    c.id_entrepot_depart,
    c.date_commande,
    c.statut_commande,
    c.mode_de_paiement,
    c.numero_tracking, 
    c.date_livraison_estimee,
    c.emballage_special,
    c.id_produit, 
    c.quantite, 
    prod.prix AS prix_unitaire_avant_promotion,

    -- Calcul du montant avant promotion
    ROUND(c.quantite * prod.prix, 2) AS montant_commande_avant_promotion,

    -- Transformation conditionnelle de id_promotion_appliquee : 'P099' => 'PROM099'
    id_promotion_appliquee,

    -- Indique si une promotion est appliquée : TRUE si id_promotion_appliquee contient une valeur non vide
    CASE 
        WHEN TRIM(c.id_promotion_appliquee) IS NOT NULL AND TRIM(c.id_promotion_appliquee) != '' 
        THEN TRUE 
        ELSE FALSE 
    END AS promotion_oui_non,

    prom.type_promotion, 
        prom.valeur_promotion,

    -- Montant après promotion avec contrôle : pas de valeur négative
    CASE
        WHEN prom.type_promotion = 'Remise fixe' THEN 
            GREATEST(ROUND((c.quantite * prod.prix) - prom.valeur_promotion, 2), 0)
        WHEN prom.type_promotion = 'Pourcentage' THEN 
            GREATEST(ROUND((c.quantite * prod.prix) * (1 - prom.valeur_promotion), 2), 0)
        ELSE 
            ROUND((c.quantite * prod.prix), 2)
    END AS montant_commande_apres_promotion

FROM {{ ref('mrt_fct_commandes') }} AS c
LEFT JOIN {{ ref('mrt_dim_produits') }} AS prod ON c.id_produit = prod.id_produit
LEFT JOIN {{ ref('mrt_dim_promotions') }} AS prom ON c.id_promotion_appliquee = prom.id_promotion
