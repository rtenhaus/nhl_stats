-- Did not bring in player data, so let's extract and map from other source tables!

{{
    config(
        materialized='table'
    )
}}

with

plays as ( select * from {{ ref('stg_plays') }} where player_1_id is not null ),

final as (

    select
        player_1_id as player_id,
        player_1 as player_name,
        team_for
    from plays
    {{ dbt_utils.group_by(3) }}

)

select * from final