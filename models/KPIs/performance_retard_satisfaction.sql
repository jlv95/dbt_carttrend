--- analyser la corrélation:  
-- Retard estimé : si la livraison est prévue >10 jours après la commande
--- annee_mois extraite de date de livraison estimee
--- selectionner uniquement les commandes qui ont été notées ( note_client)
WITH commandes_avec_retard AS (
    SELECT
        c.id_commande,
        c.date_commande,
        c.date_livraison_estimee,
        DATE_DIFF(DATE(c.date_livraison_estimee), DATE(c.date_commande), DAY) > 10 AS retard_livraison,
        FORMAT_DATE('%Y-%m', DATE(c.date_livraison_estimee)) AS annee_mois
    FROM {{ ref('mrt_fct_commandes') }} c
)

SELECT
    cwr.*,
    s.note_client
FROM commandes_avec_retard cwr
JOIN {{ ref('mrt_fct_satisfaction') }} s   ---prendre les commandes qui ont été notées
  ON cwr.id_commande = s.id_commande
