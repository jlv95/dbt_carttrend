{{ config(
    materialized='table'
) }}

-- Objectif : éclater les produits favoris (liste séparée par des virgules) en lignes individuelles
WITH exploded_favoris AS (
    SELECT
        id_client,  -- On garde l’identifiant du client

        -- On extrait chaque favori, on enlève les espaces autour => TRIM
        -- Puis on extrait la partie numérique après le "P" (ou sans P si jamais) => REGEXP_EXTRACT
        -- Et on complète à gauche avec des zéros jusqu’à 5 chiffres => LPAD( .....,5,'0')
        -- Et puis on réinsère le "P" => 'P' ||
        'P' || LPAD(REGEXP_EXTRACT(TRIM(favori), r'P?(\d+)'), 5, '0') AS id_produit_favori

    FROM {{ ref('stg_clients') }},

    UNNEST(SPLIT(favoris, ',')) AS favori
)

SELECT
    ef.id_client,
    ef.id_produit_favori,
    p.produit AS nom_produit_favori

FROM exploded_favoris ef
LEFT JOIN {{ ref('stg_produits') }} p
    ON ef.id_produit_favori = p.id_produit