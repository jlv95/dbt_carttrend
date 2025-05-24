-- Les 3 lignes ci-dessous font que dbt créera une table et non une vue

{{ config(
    materialized='table'
) }}

SELECT
  id_campagne,
  CAST(date AS DATE) AS date, -- je ne garde que la date au format AAAA-MM-JJ et j'omets l'heure 
  evenement_oui_non,
  evenement_type,
  canal,
  budget,
  impressions,
  clics,
  conversions,
  ctr
FROM {{ ref('stg_campagnes') }}
