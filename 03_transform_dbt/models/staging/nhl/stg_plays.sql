{{
    config(
        materialized='table',
        partition_by={
            'field': 'game_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by='event_type'
    )
}}

with

source as ( select * from {{ source('NHL', 'plays') }} ),

extracted as (

    select
        game_date,
        game_id,
        json_extract_scalar(data, '$.datetime') as datetime_utc,
        json_extract_scalar(data, '$.period') as period,
        json_extract_scalar(data, '$.period_time') as period_time,
        json_extract_scalar(data, '$.period_time_remaining') as period_time_remaining,
        json_extract_scalar(data, '$.period_type') as period_type,
        json_extract_scalar(data, '$.x') as x,
        json_extract_scalar(data, '$.y') as y,
        json_extract_scalar(data, '$.event_type') as event_type,
        json_extract_scalar(data, '$.event_secondary_type') as event_secondary_type,
        json_extract_scalar(data, '$.event_description') as event_description,
        json_extract_scalar(data, '$.is_shot') as is_shot,
        json_extract_scalar(data, '$.is_corsi') as is_corsi,
        json_extract_scalar(data, '$.is_fenwick') as is_fenwick,
        json_extract_scalar(data, '$.team_for') as team_for,
        json_extract_scalar(data, '$.shot_distance') as shot_distance,
        json_extract_scalar(data, '$.shot_angle') as shot_angle,
        json_extract_scalar(data, '$.player_1') as player_1,
        json_extract_scalar(data, '$.player_1_type') as player_1_type,
        json_extract_scalar(data, '$.player_1_id') as player_1_id,
        json_extract_scalar(data, '$.player_2') as player_2,
        json_extract_scalar(data, '$.player_2_type') as player_2_type,
        json_extract_scalar(data, '$.player_2_id') as player_2_id,
        json_extract_scalar(data, '$.player_3') as player_3,
        json_extract_scalar(data, '$.player_3_type') as player_3_type,
        json_extract_scalar(data, '$.player_3_id') as player_3_id,
        json_extract_scalar(data, '$.player_4') as player_4,
        json_extract_scalar(data, '$.player_4_type') as player_4_type,
        json_extract_scalar(data, '$.player_4_id') as player_4_id

    from source

),

casted as (

    select
        safe_cast(game_date as date) as game_date,
        safe_cast(game_id as int64) as game_id,
        safe_cast(datetime_utc as timestamp) as play_utc,
        safe_cast(period as int64) as period,
        safe_cast(period_time as time) as period_time,
        safe_cast(period_time_remaining as time) as period_time_remaining,
        safe_cast(period_type as string) as period_type,
        safe_cast(event_type as string) as event_type,
        safe_cast(event_secondary_type as string) as event_secondary_type,
        safe_cast(event_description as string) as event_description,
        safe_cast(team_for as string) as team_for,
        
        -- Play Details
        struct(
            safe_cast(x as int64) as x,
            safe_cast(y as int64) as y
        ) as play_location,
        safe_cast(is_shot as boolean) as is_shot,
        safe_cast(is_corsi as boolean) as is_corsi,
        safe_cast(is_fenwick as boolean) as is_fenwick,
        
        -- Player 1
        safe_cast(player_1 as string) as player_1,
        safe_cast(player_1_type as string) as player_1_type,
        safe_cast(player_1_id as int64) as player_1_id,

        -- Player 2
        safe_cast(player_2 as string) as player_2,
        safe_cast(player_2_type as string) as player_2_type,
        safe_cast(player_2_id as int64) as player_2_id,

        -- Player 3
        safe_cast(player_3 as string) as player_3,
        safe_cast(player_3_type as string) as player_3_type,
        safe_cast(player_3_id as int64) as player_3_id,

        -- Player 4
        safe_cast(player_4 as string) as player_4,
        safe_cast(player_4_type as string) as player_4_type,
        safe_cast(player_4_id as int64) as player_4_id
    from extracted

)

select * from casted



