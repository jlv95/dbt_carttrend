-- Avoir un tableau avec en colonnes : canal ; nombre_de_clics, cout_par_clic, nombre_de_conversions, cout_par_conversion 

SELECT
    id_campagne,
    date,
    canal,
    budget,
    clics AS nombre_de_clics,
    budget / clics AS cout_par_clic, 
    conversions AS nombre_de_conversions, 
    budget / conversions AS cout_par_acquisition
FROM {{ ref('mrt_fct_campagnes') }}