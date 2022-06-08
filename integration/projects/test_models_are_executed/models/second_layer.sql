{{ config(alias="second_layer") }}

SELECT *, true as my_boolean_col
FROM {{ ref("first_layer") }}
LEFT JOIN {{ ref("my_seed") }} USING(a)