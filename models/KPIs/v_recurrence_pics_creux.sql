-- ==============================================================================
-- Vue : v_recurrence_pics_creux (US007)
-- Objectif : Identifier les pics/creux de commandes par jour du mois
-- Méthode : Analyse statistique (z-score) + médiane
-- ==============================================================================

WITH commandes_par_jour AS (
  -- Étape 1 : Comptage quotidien des commandes (hors commandes annulées)
  SELECT 
    DATE(date_commande) AS jour,
    EXTRACT(DAY FROM DATE(date_commande)) AS jour_du_mois,
    EXTRACT(DAYOFWEEK FROM DATE(date_commande)) AS jour_de_semaine,
    COUNT(*) AS nb_commandes
  FROM {{ ref('mrt_fct_commandes') }}
  WHERE statut_commande != 'Annulée'
  GROUP BY jour, jour_du_mois, jour_de_semaine
),

stats_par_jour_du_mois AS (
  -- Étape 2 : Statistiques par jour du mois (1 à 31)
  SELECT 
    jour_du_mois,
    APPROX_QUANTILES(nb_commandes, 2)[OFFSET(1)] AS mediane_commandes,
    AVG(nb_commandes) AS moyenne,
    STDDEV(nb_commandes) AS ecart_type
  FROM commandes_par_jour
  GROUP BY jour_du_mois
),

anomalies_par_jour AS (
  -- Étape 3 : Calcul des écarts + z-score
  SELECT 
    c.jour,
    c.jour_du_mois,
    c.jour_de_semaine,
    c.nb_commandes,
    s.moyenne,
    s.mediane_commandes,
    s.ecart_type,
    SAFE_DIVIDE(c.nb_commandes - s.moyenne, s.ecart_type) AS z_score,
    CASE 
      WHEN SAFE_DIVIDE(c.nb_commandes - s.moyenne, s.ecart_type) >= 1.5 THEN 'pic'
      WHEN SAFE_DIVIDE(c.nb_commandes - s.moyenne, s.ecart_type) <= -1.5 THEN 'creux'
      ELSE 'normal'
    END AS statut
  FROM commandes_par_jour c
  JOIN stats_par_jour_du_mois s 
    ON c.jour_du_mois = s.jour_du_mois
)

-- Étape 4 : Résultat enrichi
SELECT 
  jour,
  jour_du_mois,
  jour_de_semaine,
  nb_commandes,
  ROUND(moyenne, 2) AS moyenne_attendue,
  ROUND(mediane_commandes, 2) AS mediane_commandes,
  ROUND(ecart_type, 2) AS ecart_type_jour,
  ROUND(z_score, 2) AS z_score,
  statut
FROM anomalies_par_jour
ORDER BY jour
