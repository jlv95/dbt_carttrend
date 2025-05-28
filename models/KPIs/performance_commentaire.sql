---choisir le mots_cles a partir du commentaire
-- choisir le sentiment en fonction de la note 
WITH base AS ( 
    SELECT
        id_commande,
        commentaire,
        note_client,
        CASE 
            WHEN note_client >= 3 THEN 'positif'
            WHEN note_client <= 2 THEN 'negatif'
            ELSE 'neutre'
        END AS sentiment
    FROM {{ ref('mrt_fct_satisfaction') }}
    WHERE commentaire IS NOT NULL
),

replaced AS (
    SELECT
        id_commande,
        note_client,
        sentiment,
        CASE
            WHEN LOWER(commentaire) = 'Could be better.' THEN 'be better'
            WHEN LOWER(commentaire) = 'excellent product highly recommend!' THEN 'recommend'
            WHEN LOWER(commentaire) = 'fast delivery and good service.' THEN 'good service'
            WHEN LOWER(commentaire) = 'good product, happy with the purchase.' THEN 'good product'
            WHEN LOWER(commentaire) = 'average product, nothing special.' THEN 'average product'
            WHEN LOWER(commentaire) = 'below average quality.' THEN 'average quality'
            WHEN LOWER(commentaire) = 'customer service was unhelpful.' THEN 'service unhelpful'
            WHEN LOWER(commentaire) = 'delivery took too long.' THEN 'delivery long'
            WHEN LOWER(commentaire) = 'not satisfied with the service.' THEN 'bad service'
            WHEN LOWER(commentaire) = 'terrible experience, will not buy again.' THEN 'terrible experience'
            WHEN LOWER(commentaire) = 'great quality and service!' THEN 'great quality'
            WHEN LOWER(commentaire) = 'it was okay, not great.' THEN 'average product'
            WHEN LOWER(commentaire) = 'the product arrived damaged.' THEN 'product damaged'
            WHEN LOWER(commentaire) = 'perfect experience, very happy!' THEN 'perfect'
            WHEN LOWER(commentaire) = 'satisfied with the experience.' THEN 'satisfied'
            ELSE NULL
        END AS mots_cles
    FROM base
),

final AS (
    SELECT
        id_commande,
        note_client,
        sentiment,
        CASE
            WHEN note_client = 5 AND (mots_cles IS NULL OR mots_cles = '') THEN 'perfect'
            WHEN note_client = 3 AND (mots_cles IS NULL OR mots_cles = '') THEN 'be better'
            ELSE mots_cles
        END AS mots_cles
    FROM replaced
)

SELECT
    id_commande,
    note_client,
    sentiment,
    mots_cles
FROM final
ORDER BY sentiment
