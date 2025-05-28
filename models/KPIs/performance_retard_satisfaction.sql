-- Analyser la corrélation :  
-- Retard estimé : si la livraison est prévue avec un délai supérieur à la médiane globale
-- annee_mois : extraite de la date de livraison estimée
-- Sélectionner uniquement les commandes qui ont été notées (note_client)

WITH delais AS (
  -- Calcule le délai (en jours) entre commande et livraison estimée
  SELECT
    DATE_DIFF(DATE(c.date_livraison_estimee), DATE(c.date_commande), DAY) AS delai
  FROM {{ ref('mrt_fct_commandes') }} c
),

mediane_delai AS (
  -- Calcule la médiane globale des délais
  SELECT
    PERCENTILE_CONT(delai, 0.5) OVER () AS mediane
  FROM delais
  LIMIT 1
),

commandes_avec_retard AS (
  -- Marque les commandes comme en retard si leur délai dépasse la médiane globale,
  -- et expose la valeur de la médiane dans chaque ligne
  SELECT
      c.id_commande,
      c.date_commande,
      c.date_livraison_estimee,
      FORMAT_DATE('%Y-%m', DATE(c.date_livraison_estimee)) AS annee_mois,
      DATE_DIFF(DATE(c.date_livraison_estimee), DATE(c.date_commande), DAY) AS delai_commande,
      m.mediane,
      DATE_DIFF(DATE(c.date_livraison_estimee), DATE(c.date_commande), DAY) > m.mediane AS retard_livraison
  FROM {{ ref('mrt_fct_commandes') }} c
  CROSS JOIN mediane_delai m
)

-- Filtrage final : ne garder que les commandes notées
SELECT
    cwr.*,
    s.note_client
FROM commandes_avec_retard cwr
JOIN {{ ref('mrt_fct_satisfaction') }} s
  ON cwr.id_commande = s.id_commande