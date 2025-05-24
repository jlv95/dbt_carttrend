-- je ne sélectionne que les champs voulus (ex : je ne sélectionne pas les champs à caractère personnel, car ils n'ont pas leur utilité en BI)

WITH source AS (
    SELECT
        id_client,
        age,
        genre,
        frequence_visites,
        date_inscription,
        favoris
    FROM {{ source('dataset_airflow', 'clients') }}
)

SELECT * FROM source
