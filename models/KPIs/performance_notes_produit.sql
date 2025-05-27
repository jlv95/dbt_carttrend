-- Moyenne des notes par produit et par mois de commande
SELECT
    p.id_produit,
    p.produit AS nom_produit,
    p.categorie,
    FORMAT_DATE('%Y-%m', DATE(c.date_commande)) AS annee_mois,
    AVG(s.note_client) AS note_moyenne,
    COUNT(*) AS nb_avis
FROM {{ ref('mrt_fct_satisfaction') }} s
JOIN {{ ref('mrt_fct_commandes') }} c ON s.id_commande = c.id_commande
JOIN {{ ref('mrt_dim_produits') }} p ON c.id_produit = p.id_produit
GROUP BY p.id_produit, p.produit, p.categorie, annee_mois
ORDER BY annee_mois ASC, note_moyenne DESC
