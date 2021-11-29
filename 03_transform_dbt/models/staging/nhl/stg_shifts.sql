{{
    config(
        materialized='table',
        partition_by={
            'field': 'game_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by='game_id'
    )
}}

with

source as ( select * from {{ source('NHL', 'shifts') }} ),

extracted as (

    select
        game_date,
        game_id,
        json_extract_scalar(data, '$.id') as id,
        json_extract_scalar(data, '$.first_name') as first_name,
        json_extract_scalar(data, '$.last_name') as last_name,
        json_extract_scalar(data, '$.period') as period,
        json_extract_scalar(data, '$.shift_number') as shift_number,
        json_extract_scalar(data, '$.start_time') as start_time,
        json_extract_scalar(data, '$.end_time') as end_time,
        json_extract_scalar(data, '$.duration') as duration,
        json_extract_scalar(data, '$.team_abbreviation') as team_abbreviation,
        json_extract_scalar(data, '$.event_description') as event_description,
        json_extract_scalar(data, '$.event_details') as event_details,
        json_extract_scalar(data, '$.event_number') as event_number

    from source

),

casted as (

    select
        safe_cast(game_date as date) as game_date,
        safe_cast(game_id as int64) as game_id,
        safe_cast(id as int64) as id,
        safe_cast(first_name as string) as first_name,
        safe_cast(last_name as string) as last_name,
        safe.concat(safe_cast(first_name as string),' ', safe_cast(last_name as string)) as full_name,
        safe_cast(period as int64) as period,
        safe_cast(shift_number as int64) as shift_number,
        safe_cast(start_time as time) as start_time,
        safe_cast(end_time as time) as end_time,
        safe_cast(duration as time) as duration,
        safe_cast(team_abbreviation as string) as team_abbreviation,
        safe_cast(event_description as string) as event_description,
        safe_cast(event_details as string) as event_details,
        safe_cast(event_number as int64) as event_number
    from extracted

)

select * from casted



