-- Ce bloc configure le modèle pour que dbt matérialise le résultat sous forme de table (et non de vue)
{{ config(
    materialized='table'
) }}

-- Objectif : éclater les produits favoris (liste séparée par des virgules) en lignes individuelles
WITH exploded_favoris AS (
    SELECT
        id_client,  -- On garde l’identifiant du client

        -- On extrait chaque favori, on enlève les espaces autour (TRIM)
        -- Puis on remplace le 'P' du début par 'P00' pour transformer par exemple 'P951' en 'P00951'
        -- Cela permet d’harmoniser les formats d’ID pour la jointure avec stg_produits
        REGEXP_REPLACE(TRIM(favori), r'^P', 'P00') AS id_produit_favori

    -- On part de la table stg_clients
    FROM {{ ref('stg_clients') }},

    -- On éclate la chaîne 'favoris' (ex: 'P951,P422') en un tableau, puis on déroule ce tableau en lignes avec UNNEST
    UNNEST(SPLIT(favoris, ',')) AS favori
)

-- Sélection finale des données
SELECT
    ef.id_client,  -- Identifiant client
    ef.id_produit_favori,  -- Identifiant du produit favori, formaté avec 'P00' ajouté
    p.produit AS nom_produit_favori  -- Nom du produit récupéré via la jointure

-- Jointure gauche entre les favoris éclatés et la table des produits
-- Objectif : récupérer le nom du produit correspondant à chaque id_produit_favori
FROM exploded_favoris ef
LEFT JOIN {{ ref('stg_produits') }} p
    ON ef.id_produit_favori = p.id_produit  -- On fait la jointure sur l’identifiant produit
