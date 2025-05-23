
SELECT *
FROM {{ source('dataset_airflow', 'produits') }}
