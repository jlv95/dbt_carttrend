SELECT
    id AS id_entrepot_machine, 
    id_machine, 
    id_entrepot, 
    type_machine, 
    etat_machine, 
    CAST (temps_darret AS INTEGER) AS temps_arret,
    CAST (volume_traite AS INTEGER) AS volume_traite,
    mois AS annee_mois
FROM {{ source('dataset_airflow', 'entrepots_machines') }}