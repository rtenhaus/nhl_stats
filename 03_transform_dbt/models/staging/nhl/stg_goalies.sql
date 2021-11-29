-- Did not bring in player data, so let's extract and map from other source tables!

{{
    config(
        materialized='table'
    )
}}

with

plays as ( select * from {{ ref('stg_plays') }} ),

-- Could be in any slot except player_1
player_2 as (

    select
        player_2_id as player_id,
        player_2 as player_name
    from plays
    where player_2_type = 'Goalie'
    {{ dbt_utils.group_by(2) }}

),

player_3 as (

    select
        player_3_id as player_id,
        player_3 as player_name
    from plays
    where player_3_type = 'Goalie'
    {{ dbt_utils.group_by(2) }}

),

player_4 as (

    select
        player_4_id as player_id,
        player_4 as player_name
    from plays
    where player_4_type = 'Goalie'
    {{ dbt_utils.group_by(2) }}

),

unioned as (

    select * from player_2
    union distinct
    select * from player_3
    union distinct
    select * from player_4

)

select * from unioned