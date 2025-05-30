SELECT 
    id AS id_produit, 
    categorie,
    marque,
    produit,
    CAST (prix AS FLOAT64) AS prix,
    sous_categorie,
    variation
FROM {{ source('dataset_airflow', 'produits') }}
