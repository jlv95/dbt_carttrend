-- Les 3 lignes ci-dessous font que dbt créera une table et non une vue

{{ config(
    materialized='table'
) }}

WITH date_spine AS (
    SELECT 
        DATE_ADD('2000-01-01', INTERVAL day_num DAY) AS date_jour
    FROM UNNEST(GENERATE_ARRAY(0, 365 * 7)) AS day_num
)

SELECT
    date_jour,
    EXTRACT(YEAR FROM date_jour) AS annee,
    EXTRACT(MONTH FROM date_jour) AS mois,
    EXTRACT(DAY FROM date_jour) AS jour,
    FORMAT_DATE('%A', date_jour) AS jour_semaine
FROM date_spine
