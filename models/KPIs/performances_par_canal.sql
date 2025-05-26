-- Avoir un tableau avec en colonnes : nom du canal ;  coût par conversion

SELECT
    canal,
    budget,
    conversions,
    budget / conversions AS cout_par_acquisition
FROM {{ ref('mrt_fct_campagnes') }}
