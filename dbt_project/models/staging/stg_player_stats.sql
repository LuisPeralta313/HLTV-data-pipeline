/*
  stg_player_stats
  ----------------
  Reads all player stats Parquet files from raw_layer/player_stats/,
  deduplicates by (match_id, player_name), casts types, and adds kd_ratio.
*/

with

raw as (
    select *
    from read_parquet('../raw_layer/player_stats/*.parquet')
),

deduped as (
    select *
    from raw
    qualify row_number() over (
        partition by match_id, player_name
        order by scraped_at desc
    ) = 1
),

final as (
    select
        cast(match_id    as varchar)  as match_id,
        cast(player_name as varchar)  as player_name,
        cast(team_name   as varchar)  as team_name,

        cast(kills   as integer)      as kills,
        cast(deaths  as integer)      as deaths,
        cast(assists as integer)      as assists,

        cast(adr      as double)      as adr,
        cast(kast_pct as double)      as kast_pct,
        cast(rating   as double)      as rating,
        cast(hs_pct   as double)      as hs_pct,

        -- K/D ratio: null-safe division
        cast(kills as double) / nullif(cast(deaths as double), 0) as kd_ratio,

        cast(map_context as varchar)  as map_context,
        cast(scraped_at  as varchar)  as scraped_at

    from deduped
)

select * from final
