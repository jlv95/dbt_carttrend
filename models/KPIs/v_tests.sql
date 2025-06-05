WITH base_jour AS (
  SELECT
    CAST(date_commande AS DATE) AS jour,
    EXTRACT(DAY FROM date_commande) AS jour_du_mois,
    COUNT(*) AS nb_commandes
  FROM {{ ref('mrt_fct_commandes') }}
  WHERE statut_commande != 'Annulée'
  GROUP BY jour, jour_du_mois
),

stats AS (
  SELECT
    jour_du_mois,
    AVG(nb_commandes) AS moyenne,
    STDDEV(nb_commandes) AS ecart_type
  FROM base_jour
  GROUP BY jour_du_mois
)

SELECT
  b.jour,
  b.jour_du_mois,
  b.nb_commandes,
  ROUND(SAFE_DIVIDE(b.nb_commandes - s.moyenne, s.ecart_type), 2) AS z_score
FROM base_jour b
JOIN stats s ON b.jour_du_mois = s.jour_du_mois
ORDER BY jour

