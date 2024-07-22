WITH films_with_ratings AS (
    SELECT
    film_id,
    title,
    release_date,
    price,
    rating,
    user_rating,
    CASE
        WHEN user_rating >= 4.8 THEN 'Excelent'
        WHEN user_rating >= 4.5 THEN 'Good'
        WHEN user_rating >= 4.0 THEN 'Average'
        ELSE 'Poor'
    END AS rating_category
    FROM {{ref('films')}}
),

films_with_actors AS (
    SELECT 
        f.film_id, f.title, STRING_AGG(actor_name, ',') as actors
    FROM {{ ref('films') }} f
    JOIN {{ ref('film_actors') }} fa ON f.film_id = fa.film_id
    JOIN {{ ref('actors') }} a ON fa.actor_id = a.actor_id
    GROUP BY f.film_id, f.title
)

SELECT fwr.*, fwa.actors
FROM films_with_ratings fwr 
JOIN films_with_actors fwa 
ON fwr.film_id = fwa.film_id



-------------------------------------------------------------------


-- WITH films_with_ratings AS (
--     SELECT
--     film_id,
--     title,
--     release_date,
--     price,
--     rating,
--     user_rating,
--     CASE
--         WHEN user_rating >= 4.8 THEN 'Excelent'
--         WHEN user_rating >= 4.5 THEN 'Good'
--         WHEN user_rating >= 4.0 THEN 'Average'
--         ELSE 'Poor'
--     END AS rating_category
--     FROM films
-- ),

-- films_with_actors AS (
--     SELECT 
--         f.film_id, f.title, STRING_AGG(actor_name, ',') as actors
--     FROM films f
--     JOIN film_actors fa ON f.film_id = fa.film_id
--     JOIN actors a ON fa.actor_id = a.actor_id
--     GROUP BY f.film_id, f.title
-- )

-- SELECT fwr.*, fwa.actors
-- FROM films_with_ratings fwr 
-- JOIN films_with_actors fwa 
-- ON fwr.film_id = fwa.film_id



