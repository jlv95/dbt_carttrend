
SELECT *
FROM {{ source('dataset_airflow', 'details_commandes') }}
