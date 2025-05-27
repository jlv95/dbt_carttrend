-- ============================================================================
-- Modèle dbt : v_segmentation_categories_par_age
-- Objectif : Identifier les catégories de produits les plus plébiscitées par tranche d'âge
-- Méthode : Agrégation par catégorie + tranche d'âge, classement volume / valeur, segmentation croisée
-- ============================================================================

-- Étape 1 : Regrouper les ventes par tranche d’âge et catégorie
WITH ventes_age_cat AS (
  SELECT
    CASE
      WHEN c.age < 25 THEN 'Moins de 25 ans'
      WHEN c.age BETWEEN 25 AND 40 THEN '25–40 ans'
      WHEN c.age BETWEEN 41 AND 60 THEN '41–60 ans'
      ELSE 'Plus de 60 ans'
    END AS tranche_age,
    p.categorie,
    SUM(f.quantite) AS quantite_totale,
    SUM(f.quantite * p.prix) AS chiffre_affaires
  FROM {{ ref('mrt_fct_commandes') }} f
  JOIN {{ ref('mrt_dim_clients') }} c ON f.id_client = c.id_client
  JOIN {{ ref('mrt_dim_produits') }} p ON f.id_produit = p.id_produit
  GROUP BY tranche_age, p.categorie
),

-- Étape 2 : Médianes par tranche d’âge
seuils AS (
  SELECT
    tranche_age,
    APPROX_QUANTILES(quantite_totale, 2)[OFFSET(1)] AS mediane_volume,
    APPROX_QUANTILES(chiffre_affaires, 2)[OFFSET(1)] AS mediane_ca
  FROM ventes_age_cat
  GROUP BY tranche_age
),

-- Étape 3 : Ajout des rangs
classement AS (
  SELECT
    v.*,
    RANK() OVER (PARTITION BY v.tranche_age ORDER BY v.quantite_totale DESC) AS rang_volume,
    RANK() OVER (PARTITION BY v.tranche_age ORDER BY v.chiffre_affaires DESC) AS rang_valeur
  FROM ventes_age_cat v
)

-- Étape 4 : Segmentation finale
SELECT
  c.*,
  s.mediane_volume,
  s.mediane_ca,
  CASE
    WHEN c.quantite_totale >= s.mediane_volume AND c.chiffre_affaires >= s.mediane_ca THEN 'Star'
    WHEN c.quantite_totale >= s.mediane_volume AND c.chiffre_affaires < s.mediane_ca THEN 'Populaire peu rentable'
    WHEN c.quantite_totale < s.mediane_volume AND c.chiffre_affaires >= s.mediane_ca THEN 'Premium discret'
    ELSE 'Faible'
  END AS segment_categorie
FROM classement c
JOIN seuils s ON c.tranche_age = s.tranche_age
