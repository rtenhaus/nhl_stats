-- Did not bring in team data, so let's extract and map from other source tables!

{{
    config(
        materialized='table'
    )
}}

with

games as ( select * from {{ ref('stg_games') }} ),

unioned as (

    select distinct home_team as team from games
    union distinct
    select distinct away_team as team from games

)

select * from unioned