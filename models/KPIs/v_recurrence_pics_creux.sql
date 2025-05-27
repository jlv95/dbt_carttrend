-- Vue : v_recurrence_pics_creux (US007)
-- Objectif : identifier les jours du mois où le nombre de commandes s’écarte 
-- significativement de la moyenne observée pour ce jour du mois (pic ou creux).

WITH commandes_par_jour AS (
  -- Étape 1 : Compter le nombre de commandes par jour
  SELECT 
    DATE(date_commande) AS jour,
    EXTRACT(DAY FROM DATE(date_commande)) AS jour_du_mois,
    EXTRACT(DAYOFWEEK FROM DATE(date_commande)) AS jour_de_semaine,
    COUNT(*) AS nb_commandes
  FROM {{ ref('mrt_fct_commandes') }}
  GROUP BY 
    DATE(date_commande),
    EXTRACT(DAY FROM DATE(date_commande)),
    EXTRACT(DAYOFWEEK FROM DATE(date_commande))
),

stats_par_jour_du_mois AS (
  -- Étape 2 : Calcul de la moyenne et de l’écart-type pour chaque jour du mois (1 à 31)
  SELECT 
    jour_du_mois,
    AVG(nb_commandes) AS moyenne,
    STDDEV(nb_commandes) AS ecart_type
  FROM commandes_par_jour
  GROUP BY jour_du_mois
),

anomalies_par_jour AS (
  -- Étape 3 : Calcul du z-score et classification des jours comme pic / creux / normal
  SELECT 
    c.jour,
    c.jour_du_mois,
    c.jour_de_semaine,
    c.nb_commandes,
    s.moyenne,
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

-- Étape 4 : Résultat final avec classification des jours
SELECT 
  jour,
  jour_du_mois,
  jour_de_semaine,
  nb_commandes,
  ROUND(moyenne, 2) AS moyenne_attendue,
  ROUND(ecart_type, 2) AS ecart_type_jour,
  ROUND(z_score, 2) AS z_score,
  statut
FROM anomalies_par_jour
ORDER BY jour
