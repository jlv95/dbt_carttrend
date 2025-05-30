SELECT
    id_post,
    CAST (date_post AS DATE) AS date_post,
    canal_social, 
    CAST (volume_mentions AS INTEGER) AS volume_mentions, 
    sentiment_global, 
    contenu_post
FROM {{ source('dataset_airflow', 'posts') }}
