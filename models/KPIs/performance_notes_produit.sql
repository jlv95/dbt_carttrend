-- Moyenne des notes par produit et par mois de commande
SELECT 
    s.id_commande,
    s.note_client,
    c.date_commande,
    c.date_livraison_estimee,
    FORMAT_DATE('%Y-%m', DATE(c.date_commande)) AS mois_annee,
    c.id_produit,
    p.produit AS nom_produit,
    p.categorie
FROM {{ ref('mrt_fct_satisfaction') }} s
JOIN {{ ref('mrt_fct_commandes') }} c ON s.id_commande = c.id_commande
JOIN {{ ref('mrt_dim_produits') }} p ON c.id_produit = p.id_produit
