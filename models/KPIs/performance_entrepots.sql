-- Calculer le volume total traité par entrepôt et le nombre de commandes expédiées
-- nb_retards : nombre de commandes livrées avec un délai supérieur à la médiane globale
-- COALESCE(..., 0) : remplace les valeurs NULL (absence de données) par 0
-- ROUND(...) : calcule le taux de retard en pourcentage, arrondi à 2 décimales

WITH volumes_par_entrepot AS (
  -- Agrège le volume total traité par chaque entrepôt
  SELECT
    m.id_entrepot,
    SUM(m.volume_traite) AS total_volumes
  FROM {{ ref('mrt_fct_machines') }} m
  GROUP BY m.id_entrepot
),

delais AS (
  -- Calcule le délai (en jours) entre commande et livraison estimée
  SELECT
    DATE_DIFF(DATE(c.date_livraison_estimee), DATE(c.date_commande), DAY) AS delai
  FROM {{ ref('mrt_fct_commandes') }} c
),

mediane_delai AS (
  -- Calcule la médiane globale des délais
  SELECT
    PERCENTILE_CONT(delai, 0.5) OVER () AS mediane
  FROM delais
  LIMIT 1
),

commandes_par_entrepot AS (
  -- Compte les commandes et les retards (délai > médiane globale) par entrepôt
  SELECT
    c.id_entrepot_depart,
    COUNT(*) AS nb_commandes,
    SUM(CASE 
      WHEN DATE_DIFF(DATE(c.date_livraison_estimee), DATE(c.date_commande), DAY) > m.mediane
      THEN 1 ELSE 0 END
    ) AS nb_retards
  FROM {{ ref('mrt_fct_commandes') }} c
  CROSS JOIN mediane_delai m
  GROUP BY c.id_entrepot_depart
)

-- Jointure finale avec la dimension entrepôts pour enrichir les données
SELECT 
  e.id_entrepot, 
  e.localisation, 
  e.taux_remplissage,
  COALESCE(v.total_volumes, 0) AS total_volumes, 
  COALESCE(c.nb_commandes, 0) AS nb_commandes,
  COALESCE(c.nb_retards, 0) AS nb_retards,
  ROUND(
    100.0 * COALESCE(c.nb_retards, 0) / NULLIF(c.nb_commandes, 0),
    2
  ) AS taux_retards
FROM {{ ref('mrt_dim_entrepots') }} e
LEFT JOIN volumes_par_entrepot v ON e.id_entrepot = v.id_entrepot
LEFT JOIN commandes_par_entrepot c ON e.id_entrepot = c.id_entrepot_depart
ORDER BY total_volumes DESC
