SELECT
    m.id_entrepot,
    e.localisation,
    m.annee_mois,
    m.id_machine,
    m.type_machine,
    m.volume_traite,
    e.capacite_max AS capacite_max_globale,
    e.volume_stocke AS volume_stock_global,
    e.taux_remplissage AS taux_remplissage_global, 

FROM {{ ref('mrt_fct_machines') }} AS m 

JOIN {{ ref('mrt_dim_entrepots') }} AS e
ON m.id_entrepot = e.id_entrepot 

