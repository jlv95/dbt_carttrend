-- Calcule le volume total traité par entrepôt, les commandes avec retards, et le volume par machine

WITH volumes_par_entrepot AS (
  SELECT
    m.id_entrepot,
    SUM(m.volume_traite) AS total_volumes
  FROM {{ ref('mrt_fct_machines') }} m
  GROUP BY m.id_entrepot
),

delais AS (
  SELECT
    DATE_DIFF(DATE(c.date_livraison_estimee), DATE(c.date_commande), DAY) AS delai
  FROM {{ ref('mrt_fct_commandes') }} c
),

mediane_delai AS (
  SELECT
    PERCENTILE_CONT(delai, 0.5) OVER () AS mediane
  FROM delais
  LIMIT 1
),

commandes_par_entrepot AS (
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
),

nombre_machines_par_entrepot AS (
  SELECT
    id_entrepot,
    COUNT(DISTINCT id_machine) AS nombre_machines
  FROM {{ ref('mrt_fct_machines') }}
  GROUP BY id_entrepot
)

-- Résultat final : enrichi avec le nombre de machines et le volume moyen par machine
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
  ) AS taux_retards,

  COALESCE(nm.nombre_machines, 0) AS nombre_machines,

  ROUND(
    COALESCE(v.total_volumes, 0) / NULLIF(nm.nombre_machines, 0),
    2
  ) AS volume_par_machine

FROM {{ ref('mrt_dim_entrepots') }} e
LEFT JOIN volumes_par_entrepot v ON e.id_entrepot = v.id_entrepot
LEFT JOIN commandes_par_entrepot c ON e.id_entrepot = c.id_entrepot_depart
LEFT JOIN nombre_machines_par_entrepot nm ON e.id_entrepot = nm.id_entrepot

ORDER BY total_volumes DESC
