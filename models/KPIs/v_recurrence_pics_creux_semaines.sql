-- =============================================================================
-- Vue : v_recurrence_pics_creux_semaines
-- Objectif : Identifier les jours/semaine avec des pics de ventes inhabituels
-- Méthodologie : Z-score mensuel ET hebdomadaire sur le CA réel (après promo)
-- Marquage des semaines lucratives : ≥ 3 jours marqués comme "pic"
-- =============================================================================

WITH ventes_par_jour AS (
  SELECT
    CAST(f.date_commande AS DATE) AS jour,
    EXTRACT(YEAR FROM f.date_commande) AS annee,
    EXTRACT(MONTH FROM f.date_commande) AS mois,
    EXTRACT(WEEK FROM f.date_commande) AS semaine,
    EXTRACT(DAYOFWEEK FROM f.date_commande) AS jour_semaine,
    SUM(f.montant_commande_apres_promotion) AS ca_journalier
  FROM `carttrend-460508.dev_sbeghin.mrt_fct_commandes` f
  WHERE f.statut_commande != 'Annulée'
  GROUP BY jour, annee, mois, semaine, jour_semaine
),

stats_mensuelles AS (
  SELECT
    annee,
    mois,
    AVG(ca_journalier) AS moyenne_mois,
    STDDEV(ca_journalier) AS ecart_type_mois
  FROM ventes_par_jour
  GROUP BY annee, mois
),

stats_hebdomadaires AS (
  SELECT
    jour_semaine,
    AVG(ca_journalier) AS moyenne_jour,
    STDDEV(ca_journalier) AS ecart_type_jour
  FROM ventes_par_jour
  GROUP BY jour_semaine
),

classement_journalier AS (
  SELECT
    v.jour,
    v.annee,
    v.mois,
    v.semaine,
    v.jour_semaine,
    v.ca_journalier,

    SAFE_DIVIDE(v.ca_journalier - m.moyenne_mois, m.ecart_type_mois) AS z_score_mois,
    SAFE_DIVIDE(v.ca_journalier - h.moyenne_jour, h.ecart_type_jour) AS z_score_jour,

    CASE 
      WHEN SAFE_DIVIDE(v.ca_journalier - m.moyenne_mois, m.ecart_type_mois) >= 1.5 
           OR SAFE_DIVIDE(v.ca_journalier - h.moyenne_jour, h.ecart_type_jour) >= 1.5 
      THEN 'pic'
      ELSE 'normal'
    END AS statut_lucratif_jour
  FROM ventes_par_jour v
  JOIN stats_mensuelles m ON v.annee = m.annee AND v.mois = m.mois
  JOIN stats_hebdomadaires h ON v.jour_semaine = h.jour_semaine
),

marquage_semaines AS (
  SELECT
    annee,
    semaine,
    COUNT(*) AS nb_jours,
    SUM(CASE WHEN statut_lucratif_jour = 'pic' THEN 1 ELSE 0 END) AS nb_jours_lucratifs,
    CASE 
      WHEN SUM(CASE WHEN statut_lucratif_jour = 'pic' THEN 1 ELSE 0 END) >= 3 
      THEN 'Semaine lucrative'
      ELSE 'Semaine normale'
    END AS statut_lucratif_semaine,
    SUM(ca_journalier) AS ca_total_semaine
  FROM classement_journalier
  GROUP BY annee, semaine
)

-- Résultat final pour Power BI
SELECT
  annee,
  semaine,
  ca_total_semaine,
  nb_jours,
  nb_jours_lucratifs,
  statut_lucratif_semaine,
  CONCAT(CAST(annee AS STRING), '-S', LPAD(CAST(semaine AS STRING), 2, '0')) AS semaine_affichee
FROM marquage_semaines
ORDER BY annee, semaine
