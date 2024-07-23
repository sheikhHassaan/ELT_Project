{% set movie_name = 'Dunkirk' %}

SELECT * FROM {{ 'films' }} WHERE title = '{{ movie_name }}'