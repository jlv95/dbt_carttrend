-------------------------------
-- v_sensibilite_promotions.sql
--------------------------------

SELECT
  p.id_produit,
  p.produit,
  p.categorie,

  -- Volume vendu avec promotion (champ non vide)
  SUM(CASE WHEN TRIM(f.id_promotion_appliquee) != '' THEN f.quantite ELSE 0 END) AS quantite_en_promo,

  -- Volume vendu sans promotion (champ vide)
  SUM(CASE WHEN TRIM(f.id_promotion_appliquee) = '' THEN f.quantite ELSE 0 END) AS quantite_hors_promo,

  -- CA total
  SUM(f.quantite * p.prix) AS ca_total,

  -- CA en promotion
  SUM(CASE WHEN TRIM(f.id_promotion_appliquee) != '' THEN f.quantite * p.prix ELSE 0 END) AS ca_promo,

  -- Taux de sensibilité en volume
  SAFE_DIVIDE(
    SUM(CASE WHEN TRIM(f.id_promotion_appliquee) != '' THEN f.quantite ELSE 0 END),
    SUM(f.quantite)
  ) AS taux_sensibilite_volume,

  -- Taux de sensibilité en valeur
  SAFE_DIVIDE(
    SUM(CASE WHEN TRIM(f.id_promotion_appliquee) != '' THEN f.quantite * p.prix ELSE 0 END),
    SUM(f.quantite * p.prix)
  ) AS taux_sensibilite_valeur,

  -- Classement des produits par sensibilité (volume)
  RANK() OVER (ORDER BY SAFE_DIVIDE(
    SUM(CASE WHEN TRIM(f.id_promotion_appliquee) != '' THEN f.quantite ELSE 0 END),
    SUM(f.quantite)
  ) DESC) AS rang_sensibilite_volume

FROM {{ ref('mrt_fct_commandes') }} f
JOIN {{ ref('mrt_dim_produits') }} p ON f.id_produit = p.id_produit

GROUP BY p.id_produit, p.produit, p.categorie
HAVING SUM(f.quantite) >= 50
ORDER BY taux_sensibilite_volume DESC
