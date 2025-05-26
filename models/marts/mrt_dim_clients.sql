-- Les 3 lignes ci-dessous font que dbt créera une table et non une vue

{{ config(
    materialized='table'
) }}

-- je ne sélectionne que les champs voulus (ex : je ne sélectionne pas les champs à caractère personnel, car ils n'ont pas leur utilité en BI)

WITH source AS (
    SELECT
        id_client,
        age,
        genre,
        frequence_visites,
        date_inscription
    FROM {{ ref('stg_clients') }}
)

SELECT * FROM source
