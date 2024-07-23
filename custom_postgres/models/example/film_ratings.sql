WITH films_with_ratings AS (
    SELECT
    film_id,
    title,
    release_date,
    price,
    rating,
    user_rating,
    {{ generate_user_rating_categories() }}
    FROM {{ ref('films') }}
),

films_with_actors AS (
    SELECT 
        f.film_id,
        f.title,
        STRING_AGG(actor_name, ',') as actors
    FROM {{ ref('films') }} f
    LEFT JOIN {{ ref('film_actors') }} fa ON f.film_id = fa.film_id
    LEFT JOIN {{ ref('actors') }} a ON fa.actor_id = a.actor_id
    GROUP BY f.film_id, f.title
)

SELECT fwr.*, fwa.actors
FROM films_with_ratings fwr 
LEFT JOIN films_with_actors fwa
ON fwr.film_id = fwa.film_id