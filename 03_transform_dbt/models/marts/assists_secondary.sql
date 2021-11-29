-- This model pulls secondary assists from the goals model

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

goals as ( select * from {{ ref('goals') }} where secondary_assist.id is not null),

final as (

    select
        game_date,
        game_id,
        
        team_for,

        -- When did the goal happen
        period,
        period_time,
        period_time_remaining,
        in_regulation,
        in_overtime,
        in_shootout,
        
        secondary_assist.name,
        secondary_assist.id,

        primary_assist,
        scorer
    
    from goals

)

select * from final