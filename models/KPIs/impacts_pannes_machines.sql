-- Agrégation des données machines : somme du temps d'arrêt EN PANNE et du volume traité par machine
WITH machines_ag AS (
  SELECT
    id_machine,
    id_entrepot,
    type_machine,
    -- On somme le temps d'arrêt seulement si la machine est en panne
    SUM(CASE WHEN etat_machine = 'En panne' THEN temps_arret ELSE 0 END) AS temps_arret_panne,
    SUM(volume_traite) AS total_volume_traite
  FROM {{ ref('mrt_fct_machines') }}
  GROUP BY id_machine, id_entrepot, type_machine
),

-- Calcul de la médiane du délai de livraison par entrepôt (version BigQuery)
livraison_stats AS (
  SELECT
    e.id_entrepot,
    APPROX_QUANTILES(
      DATE_DIFF(DATE(c.date_livraison_estimee), DATE(c.date_commande), DAY), 2
    )[OFFSET(1)] AS jours_median_livraison_entrepot
  FROM {{ ref('mrt_fct_commandes') }} c
  JOIN {{ ref('mrt_dim_entrepots') }} e
    ON c.id_entrepot_depart = e.id_entrepot
  GROUP BY e.id_entrepot
)

-- Jointure finale des machines avec la médiane de livraison de leur entrepôt
SELECT
  m.id_machine,
  m.id_entrepot,
  m.type_machine,
  m.temps_arret_panne,
  m.total_volume_traite AS volume_traite,
  l.jours_median_livraison_entrepot
FROM machines_ag m
LEFT JOIN livraison_stats l
  ON m.id_entrepot = l.id_entrepot
