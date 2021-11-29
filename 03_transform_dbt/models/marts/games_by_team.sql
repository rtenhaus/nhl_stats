-- This model shows results for each game and team, so a game between team A and team B will show up twice, once for each team.

{{
    config(
        materialized='table',
        partition_by={
            'field': 'game_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by='season, team, opponent'
    )
}}

with

staged as ( select * from {{ ref('stg_games') }} ),

arenas as ( select * from {{ ref('stg_venues') }} ),

home_games as (

    select
        game_date,
        game_id,
        season,

        venue,
        venue_type,
        
        home_team as team,
        home_score as score,
        true as is_home,
        false as is_away,

        away_team as opponent,
        away_score as opponent_score,

        home_score > away_score as is_win,
        home_score < away_score as is_loss

    from staged
    inner join arenas on staged.home_team = arenas.team

),

away_games as (

    select
        game_date,
        game_id,
        season,

        venue,
        venue_type,

        away_team as team,
        away_score as score,
        false as is_home,
        true as is_away,

        home_team as opponent,
        home_score as opponent_score,

        away_score > home_score as is_win,
        away_score < home_score as is_loss
    
    from staged
    inner join arenas on staged.home_team = arenas.team

),

unioned as (

    select * from home_games
    union all
    select * from away_games

),

-- Lets add the game number (1-82)
ranked as (

    select
        *,
        row_number() over (partition by team order by game_id asc) as game_number
    from unioned

)

select * from ranked

