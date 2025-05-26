
SELECT
    id AS id_entrepot_machine, 
    id_machine, 
    id_entrepot, 
    type_machine, 
    etat_machine, 
    temps_darret AS temps_arret,
    volume_traite,
    mois AS annee_mois
FROM {{ source('dataset_airflow', 'entrepots_machines') }}