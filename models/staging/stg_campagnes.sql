SELECT
    id_campagne, 
    CAST(date AS DATE) AS date,
    evenement_oui_non,
    evenement_type,
    canal,
    CAST(budget AS INTEGER) AS budget,
    CAST(impressions AS INTEGER) AS impressions,
    CAST(clics AS INTEGER) AS clics,
    CAST(conversions AS INTEGER) AS conversions,
    ROUND(CAST(ctr AS FLOAT64),4) AS ctr

FROM {{ source('dataset_airflow', 'campagnes') }}
