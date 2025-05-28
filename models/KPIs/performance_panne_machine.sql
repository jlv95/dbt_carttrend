--- l'impact des pannes machines sur le volume traité et sur les retards de livraisons
--- Le délai de livraison pour détecter s’il y a un retard (> 10 jours).
--- le volume moyen traité par machine.
--- le temps total d’arrêt de chaque machine
WITH commandes_details AS (
  SELECT
    c.id_commande,
    c.date_commande,
    c.date_livraison_estimee,
    DATE_DIFF(DATE(c.date_livraison_estimee), DATE(c.date_commande), DAY) > 10 AS retard_livraison,
    FORMAT_DATE('%Y-%m', DATE(c.date_livraison_estimee)) AS annee_mois,
    c.id_entrepot_depart
  FROM {{ ref('mrt_fct_commandes') }} c
),

machines_details AS (
  SELECT
    m.id_machine,
    m.id_entrepot,
    m.type_machine,
    m.etat_machine,
    m.volume_traite,
    m.temps_arret
  FROM {{ ref('mrt_fct_machines') }} m
),

joined_data AS (
  SELECT
    cd.id_entrepot_depart AS id_entrepot,
    cd.retard_livraison,
    cd.annee_mois,
    md.id_machine,
    md.type_machine,
    md.etat_machine,
    md.volume_traite,
    md.temps_arret
  FROM commandes_details cd
  LEFT JOIN machines_details md ON cd.id_entrepot_depart = md.id_entrepot
)

SELECT
  id_entrepot,
  id_machine,
  type_machine,
  etat_machine,
  COUNT(*) AS nb_commandes,
  SUM(CASE WHEN retard_livraison THEN 1 ELSE 0 END) AS nb_retards,
  ROUND(100 * SUM(CASE WHEN retard_livraison THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS taux_retards,
  ROUND(AVG(volume_traite), 2) AS volume_moyen_traite,
  SUM(temps_arret) AS total_temps_arret
FROM joined_data
GROUP BY id_entrepot, id_machine, type_machine, etat_machine
ORDER BY taux_retards DESC

