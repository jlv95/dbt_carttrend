-- Avoir un tableau avec en colonnes : canal ;  coût par conversion ; cout_par_clic

SELECT
    canal,
    budget,
    conversions,
    budget / conversions AS cout_par_acquisition
FROM {{ ref('mrt_fct_campagnes') }}
