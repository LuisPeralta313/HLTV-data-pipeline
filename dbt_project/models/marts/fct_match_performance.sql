/*
  fct_match_performance
  ----------------------
  Fact table joining match-level context with per-player stats.
  One row per player per match. Materialized as table for dashboard queries.

  Grain: match_id + player_name
*/

with

matches as (
    select
        match_id,
        match_date,
        team1_name,
        team2_name,
        match_winner,
        event_name,
        event_tier,
        match_format
    from {{ ref('stg_match_results') }}
),

players as (
    select
        match_id,
        player_name,
        team_name,
        kills,
        deaths,
        kd_ratio,
        adr,
        kast_pct,
        rating
    from {{ ref('stg_player_stats') }}
),

final as (
    select
        -- Match context
        m.match_id,
        m.match_date,
        m.team1_name,
        m.team2_name,
        m.match_winner,
        m.event_name,
        m.event_tier,
        m.match_format,

        -- Player stats
        p.player_name,
        p.team_name,
        p.kills,
        p.deaths,
        p.kd_ratio,
        p.adr,
        p.kast_pct,
        p.rating,

        -- Derived
        p.team_name = m.match_winner as is_winner

    from players p
    inner join matches m on p.match_id = m.match_id
)

select * from final
