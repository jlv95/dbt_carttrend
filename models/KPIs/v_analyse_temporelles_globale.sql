-- =================================================================================================
-- Vue : v_analyse_temporelles_globale
-- Objectif : Vue unique pour toutes les analyses temporelles (jours, semaines, mois) en volume et CA
-- Méthodologie : Agrégations + z-score sur volume et CA + marquage pics/creux + typologie des semaines
-- =================================================================================================

WITH base_jour AS (
  SELECT
    CAST(f.date_commande AS DATE) AS jour,
    EXTRACT(YEAR FROM f.date_commande) AS annee,
    EXTRACT(MONTH FROM f.date_commande) AS mois,
    EXTRACT(DAY FROM f.date_commande) AS jour_du_mois,
    EXTRACT(WEEK FROM f.date_commande) AS semaine,
    MOD(EXTRACT(DAYOFWEEK FROM f.date_commande) + 5, 7) + 1 AS jour_semaine_fr,
    FORMAT_DATE('%A', f.date_commande) AS jour_en,
    COUNT(*) AS nb_commandes,
    SUM(f.montant_commande_apres_promotion) AS ca_journalier
  FROM {{ ref('mrt_fct_commandes') }} f
  WHERE f.statut_commande != 'Annulée'
  GROUP BY jour, annee, mois, jour_du_mois, semaine, jour_semaine_fr, jour_en
),

jours_traduits AS (
  SELECT 'Monday' AS en, 'Lundi' AS fr UNION ALL
  SELECT 'Tuesday', 'Mardi' UNION ALL
  SELECT 'Wednesday', 'Mercredi' UNION ALL
  SELECT 'Thursday', 'Jeudi' UNION ALL
  SELECT 'Friday', 'Vendredi' UNION ALL
  SELECT 'Saturday', 'Samedi' UNION ALL
  SELECT 'Sunday', 'Dimanche'
),

stats_volume_jour_mois AS (
  SELECT
    jour_du_mois,
    AVG(nb_commandes) AS moyenne_cmd,
    STDDEV(nb_commandes) AS ecart_type_cmd
  FROM base_jour
  GROUP BY jour_du_mois
),

stats_volume_jour_semaine AS (
  SELECT
    jour_semaine_fr,
    AVG(nb_commandes) AS moyenne_cmd_jour,
    STDDEV(nb_commandes) AS ecart_type_cmd_jour
  FROM base_jour
  GROUP BY jour_semaine_fr
),

stats_ca_mois AS (
  SELECT
    mois,
    AVG(ca_journalier) AS moyenne_ca_mois,
    STDDEV(ca_journalier) AS ecart_type_ca_mois
  FROM base_jour
  GROUP BY mois
),

stats_ca_jour_semaine AS (
  SELECT
    jour_semaine_fr,
    AVG(ca_journalier) AS moyenne_ca_jour,
    STDDEV(ca_journalier) AS ecart_type_ca_jour
  FROM base_jour
  GROUP BY jour_semaine_fr
),

base_avec_scores AS (
  SELECT
    b.jour,
    b.annee,
    b.mois,
    b.jour_du_mois,
    b.semaine,
    b.jour_semaine_fr,
    j.fr AS nom_jour_fr,
    b.nb_commandes,
    b.ca_journalier,

    -- Z-scores sur volume
    SAFE_DIVIDE(b.nb_commandes - vmo.moyenne_cmd, vmo.ecart_type_cmd) AS z_score_cmd_mois,
    SAFE_DIVIDE(b.nb_commandes - vsj.moyenne_cmd_jour, vsj.ecart_type_cmd_jour) AS z_score_cmd_jour,

    -- Z-scores sur CA
    SAFE_DIVIDE(b.ca_journalier - ca_m.moyenne_ca_mois, ca_m.ecart_type_ca_mois) AS z_score_ca_mois,
    SAFE_DIVIDE(b.ca_journalier - ca_j.moyenne_ca_jour, ca_j.ecart_type_ca_jour) AS z_score_ca_jour

  FROM base_jour b
  LEFT JOIN jours_traduits j ON b.jour_en = j.en
  LEFT JOIN stats_volume_jour_mois vmo ON b.jour_du_mois = vmo.jour_du_mois
  LEFT JOIN stats_volume_jour_semaine vsj ON b.jour_semaine_fr = vsj.jour_semaine_fr
  LEFT JOIN stats_ca_mois ca_m ON b.mois = ca_m.mois
  LEFT JOIN stats_ca_jour_semaine ca_j ON b.jour_semaine_fr = ca_j.jour_semaine_fr
),

marquage_semaines AS (
  SELECT
    annee,
    semaine,
    COUNT(*) AS nb_jours,
    SUM(CASE WHEN z_score_ca_mois >= 1.5 OR z_score_ca_jour >= 1.5 THEN 1 ELSE 0 END) AS nb_jours_lucratifs,
    CASE 
      WHEN SUM(CASE WHEN z_score_ca_mois >= 1.5 OR z_score_ca_jour >= 1.5 THEN 1 ELSE 0 END) >= 3 THEN 'Semaine lucrative'
      ELSE 'Semaine normale'
    END AS statut_lucratif_semaine
  FROM base_avec_scores
  GROUP BY annee, semaine
)

-- Résultat final enrichi
SELECT
  b.*,
  -- Statuts volume
  CASE 
    WHEN z_score_cmd_mois >= 1.5 OR z_score_cmd_jour >= 1.5 THEN 'pic_volume'
    WHEN z_score_cmd_mois <= -1.5 OR z_score_cmd_jour <= -1.5 THEN 'creux_volume'
    ELSE 'normal'
  END AS statut_volume,

  -- Statuts CA
  CASE 
    WHEN z_score_ca_mois >= 1.5 OR z_score_ca_jour >= 1.5 THEN 'pic_ca'
    WHEN z_score_ca_mois <= -1.5 OR z_score_ca_jour <= -1.5 THEN 'creux_ca'
    ELSE 'normal'
  END AS statut_ca,

  m.statut_lucratif_semaine

FROM base_avec_scores b
LEFT JOIN marquage_semaines m
  ON b.annee = m.annee AND b.semaine = m.semaine
ORDER BY jour
