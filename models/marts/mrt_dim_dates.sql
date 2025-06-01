-- Les 3 lignes ci-dessous font que dbt créera une table et non une vue

{{ config(
    materialized='table'
) }}

WITH date_spine AS (
    SELECT 
        DATE_ADD('2020-01-01', INTERVAL day_num DAY) AS date_jour
    FROM UNNEST(GENERATE_ARRAY(0, 365 * 7)) AS day_num
)

SELECT
    date_jour,
    EXTRACT(YEAR FROM date_jour) AS annee,
    EXTRACT(MONTH FROM date_jour) AS mois,
    EXTRACT(DAY FROM date_jour) AS jour,
    FORMAT_DATE('%A', date_jour) AS jour_semaine,
    FORMAT_DATE('%Y-%m', date_jour) AS annee_mois  -- Colonne au format AAAA-MM,  utile pour faire des groupements et croisements avec d'autres vues ou tables
FROM date_spine
