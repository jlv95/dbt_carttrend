-- Afin d'analyser les impacts des réseaux sociaux sur le volume de commandes, nous allons créer un tableau avec autant de lignes que de mois analysés
-- Puis afficher en colonne les différentes métriques souhaitées

-- agréger les données des posts par mois
WITH posts_agg AS (
    SELECT
        FORMAT_DATE('%Y-%m', SAFE_CAST(date_post AS DATE)) AS annee_mois_post,
        SUM(CASE WHEN sentiment_global = 'Neutre' THEN volume_mentions ELSE 0 END) AS volumes_mentions_neutres,
        SUM(CASE WHEN sentiment_global = 'Positif' THEN volume_mentions ELSE 0 END) AS volumes_mentions_positives,
        SUM(CASE WHEN sentiment_global = 'Négatif' THEN volume_mentions ELSE 0 END) AS volumes_mentions_negatives,
        SUM(volume_mentions) AS volume_mentions_total -- Ajout du total des mentions
    FROM {{ ref('mrt_fct_posts') }}
    GROUP BY annee_mois_post
),

-- agréger les données des commandes par mois
commandes_agg AS (
    SELECT
        FORMAT_DATE('%Y-%m', SAFE_CAST(date_commande AS DATE)) AS annee_mois_commande,
        COUNTIF(statut_commande != 'Annulée') AS volume_commandes_viables, -- soit les commandes non annulées
        COUNTIF(statut_commande = 'Annulée') AS volume_commandes_annulees,
        COUNT(*) AS volume_commandes_total -- Ajout du total des commandes
    FROM {{ ref('mrt_fct_commandes') }}
    GROUP BY annee_mois_commande
)

-- Requête principale pour joindre les données des posts et des commandes
SELECT
    p.annee_mois_post,
    p.volumes_mentions_neutres,
    p.volumes_mentions_positives,
    p.volumes_mentions_negatives,
    c.volume_commandes_viables,
    c.volume_commandes_annulees,
    c.volume_commandes_total, -- Ajout du total des commandes
    p.volume_mentions_total -- Ajout du total des mentions
FROM posts_agg p
LEFT JOIN commandes_agg c
    ON p.annee_mois_post = c.annee_mois_commande
ORDER BY p.annee_mois_post
