SELECT
    id_commande,
    id_client,
    id_entrepot_depart,
    CAST (date_commande AS DATE) AS date_commande,
    statut_commande, 
    id_promotion_appliquee AS id_promotion,
    mode_de_paiement,
    numero_tracking,
    CAST (date_livraison_estimee AS DATE) AS date_livraison_estimee

FROM {{ source('dataset_airflow', 'commandes') }}
