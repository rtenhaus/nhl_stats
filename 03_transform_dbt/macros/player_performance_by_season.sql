{# Macro to run the same query for both home and away! #}

{% macro player_performance_by_season(home_or_away) %}

    with

        games as ( select * from {{ ref('games_by_team') }} 

                    where 
                        {% if home_or_away == 'home' %}
                            is_home
                        {% elif home_or_away == 'away' %}
                            is_away
                        {% else %}
                            1=0 
                        {% endif %}
        ),

        goals as ( select * from {{ ref('goals') }} ),

        assists as ( select * from {{ ref('assists_all') }} ),

        player_spine as (

            select distinct id, name, game_id from goals
            union distinct
            select distinct id, name, game_id from assists

        ),

        joined as (

            select
                id,
                name,
                season,
                sum(case when goals.id is not null then 1 else 0 end) as goals, -- too bad there is no goal_id
                sum(case when assists.id is not null then 1 else 0 end) as assists, -- same
                sum(case when assists.id is not null and is_primary then 1 else 0 end) as assists_primary,
                sum(case when assists.id is not null and is_secondary then 1 else 0 end) as assists_secondary
                -- Add shifts and shots later
            from player_spine
            left join games using(game_id)
            left join goals using(id,name)
            left join assists using(id,name)
            {{ dbt_utils.group_by(3) }}

        )

    select * from joined

{% endmacro %}