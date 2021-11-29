-- This model pulls primary assists from the goals model

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

goals as ( select * from {{ ref('goals') }} where primary_assist.id is not null),

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
        
        primary_assist.name,
        primary_assist.id,

        scorer,
        secondary_assist
    
    from goals

)

select * from final