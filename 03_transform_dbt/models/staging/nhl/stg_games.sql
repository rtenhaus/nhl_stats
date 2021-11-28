{{
    config(
        materialized='table',
        partition_by={
            'field': 'game_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by='season'
    )
}}

with

source as ( select * from {{ source('NHL', 'games') }} ),

extracted as (

    select
        game_date,
        game_id,
        json_extract_scalar(data, '$.home_team') as home_team,
        json_extract_scalar(data, '$.home_score') as home_score,
        json_extract_scalar(data, '$.away_team') as away_team,
        json_extract_scalar(data, '$.away_score') as away_score,
        json_extract_scalar(data, '$.season') as season,
        json_extract_scalar(data, '$.game_state') as game_state
    from source

),

casted as (

    select
        safe_cast(game_date as date) as game_date,
        safe_cast(game_id as int64) as game_id,
        safe_cast(home_team as string) as home_team,
        safe_cast(home_score as int64) as home_score,
        safe_cast(away_team as string) as away_team,
        safe_cast(away_score as int64) as away_score,
        safe_cast(season as int64) as season,
        safe_cast(game_state as string) as game_state
    from extracted

)

select * from casted



