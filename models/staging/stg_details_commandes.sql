SELECT
    id_commande,
    id_produit, 
    CAST (quantite AS INTEGER) AS quantite,
    emballage_special
FROM {{ source('dataset_airflow', 'details_commandes') }}
