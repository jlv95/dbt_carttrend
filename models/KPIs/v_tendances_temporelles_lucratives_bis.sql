-- =============================================================================
-- Vue : v_tendances_temporelles_lucratives_bis (US006)
-- Objectif : Identifier les jours/semaine avec des pics de ventes inhabituels
-- Méthodologie : Z-score mensuel et hebdomadaire sur le CA réel (après promo)
-- Marquage des semaines lucratives via z-score
-- =============================================================================

WITH ventes_par_jour AS (
  SELECT
    CAST(f.date_commande AS DATE) AS jour,
    EXTRACT(YEAR FROM CAST(f.date_commande AS DATE)) AS annee,
    EXTRACT(MONTH FROM CAST(f.date_commande AS DATE)) AS mois,
    EXTRACT(WEEK FROM CAST(f.date_commande AS DATE)) AS semaine,
    EXTRACT(DAYOFWEEK FROM CAST(f.date_commande AS DATE)) AS jour_semaine,
    SUM(f.montant_commande_apres_promotion) AS ca_journalier
  FROM {{ ref('mrt_fct_commandes') }} f
  WHERE f.statut_commande != 'Annulée'
  GROUP BY jour, annee, mois, semaine, jour_semaine
),

stats_mensuelles AS (
  SELECT
    mois,
    AVG(ca_journalier) AS moyenne_mois,
    STDDEV(ca_journalier) AS ecart_type_mois
  FROM ventes_par_jour
  GROUP BY mois
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

    m.moyenne_mois,
    m.ecart_type_mois,
    SAFE_DIVIDE(v.ca_journalier - m.moyenne_mois, m.ecart_type_mois) AS z_score_mois,

    h.moyenne_jour,
    h.ecart_type_jour,
    SAFE_DIVIDE(v.ca_journalier - h.moyenne_jour, h.ecart_type_jour) AS z_score_jour,

    RANK() OVER (PARTITION BY v.mois ORDER BY v.ca_journalier DESC) AS rang_dans_mois,
    RANK() OVER (PARTITION BY v.jour_semaine ORDER BY v.ca_journalier DESC) AS rang_dans_jour
  FROM ventes_par_jour v
  JOIN stats_mensuelles m ON v.mois = m.mois
  JOIN stats_hebdomadaires h ON v.jour_semaine = h.jour_semaine
),

-- Nouveau : marquage des semaines lucratives
marquage_semaines AS (
  SELECT
    annee,
    semaine,
    COUNT(*) AS nb_jours,
    SUM(CASE WHEN z_score_mois >= 1.5 OR z_score_jour >= 1.5 THEN 1 ELSE 0 END) AS nb_jours_lucratifs,
    CASE 
      WHEN SUM(CASE WHEN z_score_mois >= 1.5 OR z_score_jour >= 1.5 THEN 1 ELSE 0 END) >= 3 THEN 'Semaine lucrative'
      ELSE 'Semaine normale'
    END AS statut_lucratif_semaine
  FROM classement_journalier
  GROUP BY annee, semaine
)

-- Résultat final enrichi
SELECT
  c.jour,
  c.annee,
  c.mois,
  c.semaine,
  c.jour_semaine,
  c.ca_journalier,

  ROUND(c.z_score_mois, 2) AS z_score_mois,
  ROUND(c.z_score_jour, 2) AS z_score_jour,
  c.rang_dans_mois,
  c.rang_dans_jour,

  CASE 
    WHEN c.z_score_mois >= 1.5 OR c.z_score_jour >= 1.5 THEN 'pic'
    ELSE 'normal'
  END AS statut_lucratif_jour,

  m.statut_lucratif_semaine

FROM classement_journalier c
LEFT JOIN marquage_semaines m 
  ON c.annee = m.annee AND c.semaine = m.semaine
ORDER BY c.jour
