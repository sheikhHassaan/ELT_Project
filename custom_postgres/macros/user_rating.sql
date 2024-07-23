{% macro generate_user_rating_categories() %}
    CASE
        WHEN user_rating >= 4.8 THEN 'Excelent'
        WHEN user_rating >= 4.5 THEN 'Good'
        WHEN user_rating >= 4.0 THEN 'Average'
        ELSE 'Poor'
    END AS rating_category
{% endmacro %}