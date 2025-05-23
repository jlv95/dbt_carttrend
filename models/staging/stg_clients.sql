
SELECT *
FROM {{ source('dataset_airflow', 'clients') }}
