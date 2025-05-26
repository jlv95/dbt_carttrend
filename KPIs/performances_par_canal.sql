-- Avoir un tableau avec en colonnes : nom du canal ; coût par clic ;  coût par conversion

SELECT
    canal,
    budget,
    conversion,
    
    -- Calcul du coût par acquisition
    CASE 
        WHEN conversion IS NOT NULL AND conversion != 0 THEN budget / conversion
        ELSE NULL
    END AS cout_par_acquisition

FROM {{ ref('mrt_fct_campagnes') }}
