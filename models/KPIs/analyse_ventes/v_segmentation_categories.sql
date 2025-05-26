-- ============================================================================
-- Modèle dbt : v_segmentation_categories
-- Objectif : Segmenter les catégories de produits selon leurs performances commerciales
-- Indicateurs : Volume (quantité vendue) et Chiffre d'affaires (quantité × prix)
-- Méthode : Analyse croisée par médianes et attribution d'un segment
-- Ajout : RANGS pour faciliter les tris dans les outils BI
-- ============================================================================

-- Étape 1 : Calcul des ventes par catégorie (volume + CA)
WITH ventes_categorie AS (
  SELECT
    p.categorie,
    SUM(f.quantite) AS quantite_totale,                  -- Total des unités vendues pour la catégorie
    SUM(f.quantite * p.prix) AS chiffre_affaires         -- Chiffre d'affaires généré par la catégorie
  FROM {{ ref('mrt_fct_commandes') }} f
  JOIN {{ ref('mrt_dim_produits') }} p ON f.id_produit = p.id_produit
  GROUP BY p.categorie
),

-- Étape 2 : Calcul des médianes de référence (volume et CA)
seuils AS (
  SELECT
    APPROX_QUANTILES(quantite_totale, 2)[OFFSET(1)] AS q_median,       -- Médiane du volume vendu
    APPROX_QUANTILES(chiffre_affaires, 2)[OFFSET(1)] AS ca_median      -- Médiane du chiffre d'affaires
  FROM ventes_categorie
),

-- Étape 3 : Ajout des rangs pour chaque catégorie (volume et CA)
classement AS (
  SELECT
    *,
    RANK() OVER (ORDER BY quantite_totale DESC) AS rang_volume,        -- Classement par volume décroissant
    RANK() OVER (ORDER BY chiffre_affaires DESC) AS rang_valeur        -- Classement par chiffre d'affaires décroissant
  FROM ventes_categorie
)

-- Étape 4 : Attribution du segment stratégique à chaque catégorie
SELECT
  c.*,  -- Inclut nom_categorie, quantite_totale, chiffre_affaires, rang_volume, rang_valeur
  CASE
    WHEN c.quantite_totale >= s.q_median AND c.chiffre_affaires >= s.ca_median THEN 'Star'
    WHEN c.quantite_totale >= s.q_median AND c.chiffre_affaires < s.ca_median THEN 'Populaire peu rentable'
    WHEN c.quantite_totale < s.q_median AND c.chiffre_affaires >= s.ca_median THEN 'Premium discret'
    ELSE 'En perte de vitesse'
  END AS segment_categorie
FROM classement c
CROSS JOIN seuils s

