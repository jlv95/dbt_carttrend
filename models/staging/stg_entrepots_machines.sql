
SELECT *
FROM {{ source('dataset_airflow', 'entrepots_machines') }}
