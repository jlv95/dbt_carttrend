-- Vue : v_produits_achetes_par_client
-- Objectif : identifier les paires de produits achetés par les mêmes clients,
-- avec un score d'affinité (confiance conditionnelle)

WITH achats_clients AS (
  -- Liste des produits achetés par chaque client (distincts)
  SELECT 
    id_client,
    id_produit
  FROM {{ ref('mrt_fct_commandes') }}
  GROUP BY id_client, id_produit
),

-- Clients ayant acheté chaque paire de produits
paires_par_client AS (
  SELECT 
    a.id_produit AS produit_1,
    b.id_produit AS produit_2,
    COUNT(DISTINCT a.id_client) AS nb_clients_ayant_les_deux
  FROM achats_clients a
  JOIN achats_clients b 
    ON a.id_client = b.id_client
    AND a.id_produit < b.id_produit
  GROUP BY produit_1, produit_2
),

-- Nombre de clients ayant acheté chaque produit individuellement
achats_par_produit AS (
  SELECT 
    id_produit,
    COUNT(DISTINCT id_client) AS nb_clients_produit
  FROM achats_clients
  GROUP BY id_produit
),

-- Détails des produits (noms, catégories...)
produits_details AS (
  SELECT 
    id_produit,
    produit,
    categorie,
    sous_categorie,
    marque
  FROM {{ ref('mrt_dim_produits') }}
)

-- Résultat final enrichi avec les informations produits + score
SELECT 
  p.produit_1,
  d1.produit AS produit_libelle_1,
  d1.categorie AS categorie_1,
  d1.marque AS marque_1,

  p.produit_2,
  d2.produit AS produit_libelle_2,
  d2.categorie AS categorie_2,
  d2.marque AS marque_2,

  p.nb_clients_ayant_les_deux,

  ap1.nb_clients_produit AS nb_clients_produit_1,
  ap2.nb_clients_produit AS nb_clients_produit_2,

  -- Confiance (conditionnelle) : parmi ceux qui ont acheté produit_1, combien ont aussi acheté produit_2
  SAFE_DIVIDE(p.nb_clients_ayant_les_deux, ap1.nb_clients_produit) AS confiance_1_vers_2,
  SAFE_DIVIDE(p.nb_clients_ayant_les_deux, ap2.nb_clients_produit) AS confiance_2_vers_1

FROM paires_par_client p
JOIN achats_par_produit ap1 ON p.produit_1 = ap1.id_produit
JOIN achats_par_produit ap2 ON p.produit_2 = ap2.id_produit
JOIN produits_details d1 ON p.produit_1 = d1.id_produit
JOIN produits_details d2 ON p.produit_2 = d2.id_produit

WHERE p.nb_clients_ayant_les_deux >= 3
ORDER BY p.nb_clients_ayant_les_deux DESC

