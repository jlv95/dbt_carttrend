-- Les 3 lignes ci-dessous font que dbt créera une table et non une vue

{{ config(
    materialized='table'
) }}

-- Je fais un select, renommage et jointure entre la table machines_entrepots et entrepots, afin de conserver l'optique modèle étoile 

SELECT
    -- Renommage de l'ID principal de stg_machines
    machines.id AS id_entrepot_machine, -- je clarifie les noms de colonnes afin de comprendre les champs en BI par la suite 
    machines.id_entrepot,
    machines.id_machine,
    machines.mois AS mois_machine,
    machines.volume_traite AS volume_traite_machine,
    machines.temps_darret AS temps_arret_machine, 
    machines.etat_machine,
    machines.type_machine,
    
    -- Champs provenant de stg_entrepots
    entrepots.localisation AS localisation_entrepot,
    entrepots.capacite_max AS capacite_max_entrepot,
    entrepots.volume_stocke AS volume_stock_entrepot,
    entrepots.taux_remplissage AS taux_remplissage_entrepot,
    entrepots.temperature_moyenne_entrepot

FROM {{ ref('stg_entrepots_machines') }} AS machines
LEFT JOIN {{ ref('stg_entrepots') }} AS entrepots
    ON machines.id_entrepot = entrepots.id_entrepot
