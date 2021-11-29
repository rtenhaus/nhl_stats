-- This model models all assists

with

primary_assists as ( select * from {{ ref('assists_primary') }} ),

secondary_assists as ( select * from {{ ref('assists_secondary') }} ),

primary as (

    select

        game_date,
        game_id,

        team_for,

        period,
        period_time,
        period_time_remaining,
        in_regulation,
        in_overtime,
        in_shootout,

        true as is_primary,
        false as is_secondary,

        name,
        id,
        scorer

    from primary_assists

),

secondary as (

    select

        game_date,
        game_id,

        team_for,

        period,
        period_time,
        period_time_remaining,
        in_regulation,
        in_overtime,
        in_shootout,

        true as is_primary,
        false as is_secondary,

        name,
        id,
        scorer

    from secondary_assists

),

unioned as (

    select * from primary
    union all
    select * from secondary

)

select * from unioned