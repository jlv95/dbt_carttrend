SELECT
    id_satisfaction,
    id_commande,
    CAST(note_client AS INTEGER) AS note_client,
    commentaire,
    plainte,
    CAST(temps_reponse_support AS INTEGER) AS temps_reponse_support,
    type_plainte, 
    employe_support

FROM {{ source('dataset_airflow', 'satisfaction') }}
