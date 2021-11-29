
with

games as ( select * from {{ ref('games_by_team') }} where is_away ),

agg as (

    select
        team,
        venue_type,
        count(distinct game_id) as game_count,
        count(distinct case when is_win then game_id else null end) as win_count,
        count(distinct case when is_loss then game_id else null end) as loss_count
    from games
    {{ dbt_utils.group_by(2) }}

),

final as (

    select
        team,
        venue_type,
        game_count,
        win_count,
        loss_count,
        safe_divide(win_count,game_count) as win_pct
    from agg
)

select * from final