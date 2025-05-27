-- ===================================================================
-- Modèle dbt : v_segmentation_produits
-- Objectif : Segmenter les produits selon leur performance commerciale
-- Critères : Croisement de la quantité vendue et du chiffre d'affaires
-- Résultat : Label 'Star', 'Flop', etc. + rangs de classement
-- ===================================================================

-- Étape 1 : Calcul des ventes totales (quantité + CA) par produit (c'est un commentaire)
WITH ventes AS (
  SELECT
    p.id_produit,
    p.produit,
    SUM(f.quantite) AS quantite_totale,  -- Volume total vendu
    SUM(f.quantite * p.prix) AS chiffre_affaires  -- CA total généré
  FROM {{ ref('mrt_fct_commandes') }} f
  JOIN {{ ref('mrt_dim_produits') }} p ON f.id_produit = p.id_produit
  GROUP BY p.id_produit, p.produit
),

-- Étape 2 : Calcul des médianes (quantiles) pour déterminer les seuils
seuils AS (
  SELECT
    APPROX_QUANTILES(quantite_totale, 2)[OFFSET(1)] AS q_median,      -- Médiane du volume
    APPROX_QUANTILES(chiffre_affaires, 2)[OFFSET(1)] AS ca_median     -- Médiane du CA
  FROM ventes
),

-- Étape 3 : Attribution des rangs pour chaque produit
classement AS (
  SELECT
    *,
    RANK() OVER (ORDER BY quantite_totale DESC) AS rang_volume,        -- Classement par quantité
    RANK() OVER (ORDER BY chiffre_affaires DESC) AS rang_valeur        -- Classement par CA
  FROM ventes
)

-- Étape 4 : Segmentation finale des produits selon les seuils
SELECT
  c.*,  -- Toutes les colonnes issues du classement
  CASE
    WHEN c.quantite_totale >= s.q_median AND c.chiffre_affaires >= s.ca_median THEN 'Star'
    WHEN c.quantite_totale >= s.q_median AND c.chiffre_affaires < s.ca_median THEN 'Populaire peu rentable'
    WHEN c.quantite_totale < s.q_median AND c.chiffre_affaires >= s.ca_median THEN 'Premium peu vendu'
    ELSE 'Flop'
  END AS segment_produit  -- Label de performance attribué
FROM classement c
CROSS JOIN seuils s  -- Appliquer les seuils sur l’ensemble des produits
