SELECT
    id_promotion,
    id_produit,
    type_promotion,
    valeur_promotion,
    CAST (date_debut AS DATE) AS date_debut, 
    CAST (date_fin AS DATE) AS date_fin, 
    responsable_promotion
FROM {{ source('dataset_airflow', 'promotions') }}
