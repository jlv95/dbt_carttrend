-- -calculer le total des volumes traités par chaque entrepôt
-- - Le nombre de commandes avec un retard de livraison supérieur à 10 jours
-- (nb_retards)
-- - les temps d’arrêt des machines par entrepôt
-- - le nombre le nom et l'etat de machines en panne  
WITH volumes_par_entrepot_type_etat AS (
  SELECT
    m.id_entrepot,
    m.type_machine,
    m.etat_machine,
    SUM(m.volume_traite) AS total_volumes
  FROM {{ ref('mrt_fct_machines') }} m  
  GROUP BY m.id_entrepot, m.type_machine, m.etat_machine
),

retards_par_entrepot AS (
  SELECT
    c.id_entrepot_depart,
    COUNT(*) AS nb_commandes,
    SUM(CASE WHEN DATE_DIFF(DATE(c.date_livraison_estimee), DATE(c.date_commande), DAY) > 10 THEN 1 ELSE 0 END) AS nb_retards
  FROM {{ ref('mrt_fct_commandes') }} c  
  GROUP BY c.id_entrepot_depart
),

pannes_par_entrepot_type_etat AS (
  SELECT
    m.id_entrepot,
    m.type_machine,
    m.etat_machine,
    SUM(m.temps_arret) AS temps_panne_total, -- voir le temps darret (mrt-fct-machine)
    COUNT(CASE WHEN m.etat_machine = 'en panne' THEN 1 END) AS nb_pannes
  FROM {{ ref('mrt_fct_machines') }} m
  GROUP BY m.id_entrepot, m.type_machine, m.etat_machine
)

SELECT
  e.id_entrepot,
  e.localisation,
  v.total_volumes,
  r.nb_commandes,
  r.nb_retards,
  ROUND(100 * r.nb_retards / NULLIF(r.nb_commandes, 0), 2) AS taux_retards,
  p.nb_pannes,
  p.temps_panne_total,
  v.type_machine,
  v.etat_machine
FROM {{ ref('mrt_dim_entrepots') }} e
LEFT JOIN volumes_par_entrepot_type_etat v ON e.id_entrepot = v.id_entrepot
LEFT JOIN retards_par_entrepot r ON e.id_entrepot = r.id_entrepot_depart
LEFT JOIN pannes_par_entrepot_type_etat p ON e.id_entrepot = p.id_entrepot
ORDER BY total_volumes DESC
