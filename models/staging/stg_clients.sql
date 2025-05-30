-- je ne sélectionne que les champs voulus (ex : je ne sélectionne pas les champs à caractère personnel, car ils n'ont pas leur utilité en BI)

WITH source AS (
    SELECT
        id_client,
        CAST (age AS INTEGER) AS age,
        genre,
        CAST (frequence_visites AS INTEGER) AS frequence_visites,
        CAST (date_inscription AS DATE) AS date_inscription,
        favoris
    FROM {{ source('dataset_airflow', 'clients') }}
)

SELECT * FROM source
