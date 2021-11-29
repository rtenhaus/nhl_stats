-- This model shows stats about goals scored, including location, time, scorer, assists, and which goalie against.

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

staged as ( select * from {{ ref('stg_shots') }} where event_type = 'GOAL' ),

final as (

    select
        game_date,
        game_id,

        team_for,

        -- When did the goal happen
        case 
            when period = 1 then '01'
            when period = 2 then '02'
            when period = 3 then '03'
            when period = 4 then 'OT'
            when period = 5 then 'SO'
        end as period,
        period_time,
        period_time_remaining,
        period <= 3 as in_regulation,
        period = 4 as in_overtime,
        period = 5 as in_shootout,

        -- Where did the shot take place?
        shot_location,
        shot_distance,
        shot_angle,

        --Who Scored
        player_1 as name,
        player_1_id as id,
        
        struct(
            player_1 as name,
            player_1_id as id,
            event_secondary_type as shot_type
        ) as scorer,

        -- Primary Assist (if any)
        if(player_2_type = 'Assist', struct(player_2 as name, player_2_id as id), struct(null as name, null as id)) as primary_assist,
        
        -- Secondary Assist (if any)
        if(player_3_type = 'Assist', struct(player_3 as name, player_3_id as id), struct(null as name, null as id)) as secondary_assist,

        not(player_2_type = 'Assist' or player_3_type = 'Assist') as is_unassisted,

        -- Who did the scorer score against? Field position depends on if there was a primary and/or secondary assist
        if(player_2_type = 'Goalie', struct(player_2 as name, player_2_id as id),
            if(player_3_type = 'Goalie', struct(player_3 as name, player_3_id as id),
                if(player_4_type = 'Goalie', struct(player_4 as name, player_4_id as id),
                    struct(null as name, null as id)))) as goalie,
        
        not(player_2_type = 'Goalie' or player_3_type = 'Goalie' or player_4_type = 'Goalie') as is_empty_net,

        -- Some shot bools that will be useful later
        is_shot,
        is_corsi,
        is_fenwick

    from staged

    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        -- Let's rerun the last 3 days in case goal/assist attribution changes after review by league office
        where game_date >= date_sub(date(_dbt_max_partition), interval 3 days)

    {% endif %}

)

select * from final