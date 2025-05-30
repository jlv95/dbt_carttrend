SELECT
    id_entrepot,
    localisation,
    CAST (capacite_max AS INTEGER) AS capacite_max,
    CAST (volume_stocke AS INTEGER) AS volume_stocke,
    CAST(taux_remplissage AS FLOAT64) AS taux_remplissage,
    CAST(temperature_moyenne_entrepot AS FLOAT64) AS temperature_moyenne_entrepot 

FROM {{ source('dataset_airflow', 'entrepots') }}
