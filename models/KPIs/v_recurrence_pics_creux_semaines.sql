-- =============================================================================
-- Vue : v_recurrence_pics_creux_semaines
-- Objectif : Identifier les semaines lucratives à partir du z-score hebdomadaire
-- Méthode : z-score par jour de semaine (pas de regroupement par mois)
-- =============================================================================
WITH ventes_par_jour AS (
  SELECT
    CAST(f.date_commande AS DATE) AS jour,
    EXTRACT(YEAR FROM f.date_commande) AS annee,
    EXTRACT(WEEK FROM f.date_commande) AS semaine,
    EXTRACT(DAYOFWEEK FROM f.date_commande) AS jour_semaine,
    SUM(f.montant_commande_apres_promotion) AS ca_journalier
  FROM {{ ref('mrt_fct_commandes') }} f
  WHERE f.statut_commande != 'Annulée'
  GROUP BY jour, annee, semaine, jour_semaine
),

stats_hebdo AS (
  SELECT
    jour_semaine,
    AVG(ca_journalier) AS moyenne_jour,
    STDDEV(ca_journalier) AS ecart_type_jour
  FROM ventes_par_jour
  GROUP BY jour_semaine
),

scores_journaliers AS (
  SELECT
    v.annee,
    v.semaine,
    SAFE_DIVIDE(v.ca_journalier - s.moyenne_jour, s.ecart_type_jour) AS z_score_jour
  FROM ventes_par_jour v
  JOIN stats_hebdo s ON v.jour_semaine = s.jour_semaine
),

semaines_lucratives AS (
  SELECT
    annee,
    semaine,
    COUNT(*) AS nb_jours,
    SUM(CASE WHEN z_score_jour >= 1.5 THEN 1 ELSE 0 END) AS nb_jours_lucratifs,
    CASE 
      WHEN SUM(CASE WHEN z_score_jour >= 1.5 THEN 1 ELSE 0 END) >= 3 THEN 'Semaine lucrative'
      ELSE 'Semaine normale'
    END AS statut_lucratif_semaine
  FROM scores_journaliers
  GROUP BY annee, semaine
)

SELECT *
FROM semaines_lucratives
ORDER BY annee, semaine
