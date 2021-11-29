{{
    config(
        materialized='view'
    )
}}

with

seed as ( select * from {{ ref('csv_team_venues') }} ),

casted as (

    select 
        safe_cast(team as string) as team,
        safe_cast(venue as string) as venue,
        safe_cast(type as string) as venue_type
    from seed

)

select * from casted