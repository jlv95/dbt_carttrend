
SELECT 
    id AS id_produit, 
    categorie,
    marque,
    produit,
    prix,
    sous_categorie,
    variation
FROM {{ source('dataset_airflow', 'produits') }}
