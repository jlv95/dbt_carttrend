-- Agrégation des données machines : somme du temps d'arrêt EN PANNE, volume traité, nombre de pannes et annee_mois
WITH machines_ag AS (
  SELECT
    id_machine,
    id_entrepot,
    type_machine,
    annee_mois,

    -- Somme du temps d'arrêt pour les machines en panne
    SUM(CASE WHEN etat_machine = 'En panne' THEN temps_arret ELSE 0 END) AS temps_arret_panne,

    -- Nombre de fois où la machine est en panne
    SUM(CASE WHEN etat_machine = 'En panne' THEN 1 ELSE 0 END) AS nb_pannes_machines,

    -- Volume total traité
    SUM(volume_traite) AS total_volume_traite

  FROM {{ ref('mrt_fct_machines') }}
  GROUP BY id_machine, id_entrepot, type_machine, annee_mois
),

-- Calcul de la médiane du délai de livraison par entrepôt ET par mois
livraison_stats AS (
  SELECT
    e.id_entrepot,
    FORMAT_DATE('%Y-%m', DATE(c.date_commande)) AS annee_mois,
    
    APPROX_QUANTILES(
      DATE_DIFF(DATE(c.date_livraison_estimee), DATE(c.date_commande), DAY), 2
    )[OFFSET(1)] AS jours_median_livraison_entrepot

  FROM {{ ref('mrt_fct_commandes') }} c
  JOIN {{ ref('mrt_dim_entrepots') }} e
    ON c.id_entrepot_depart = e.id_entrepot
  GROUP BY e.id_entrepot, annee_mois
)

-- Jointure finale : machines avec médiane de livraison par entrepôt et mois
SELECT
  m.id_machine,
  m.id_entrepot,
  m.type_machine,
  m.annee_mois,
  m.temps_arret_panne,
  m.nb_pannes_machines,
  m.total_volume_traite AS volume_traite,
  l.jours_median_livraison_entrepot
FROM machines_ag m
LEFT JOIN livraison_stats l
  ON m.id_entrepot = l.id_entrepot 
ORDER BY temps_arret_panne ASC
