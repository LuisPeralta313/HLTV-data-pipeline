/*
  stg_match_results
  -----------------
  Reads all Parquet files from raw_layer/matches/ via DuckDB read_parquet(),
  deduplicates by match_id (latest scrape_at wins), applies type casts,
  and normalises event_tier and match_format.

  Transformations applied:
    - match_id        → VARCHAR
    - match_date      → DATE  (null filled from scraped_at)
    - team1/2_score   → INTEGER
    - event_tier      → 'Other' mapped to 'a-tier'; then MAX per event_name
    - match_format    → 'unknown' mapped to NULL
    - match_winner    → team1_name if team1_score > team2_score, else team2_name
*/

with

raw as (
    -- DuckDB reads all Parquet files matching the glob in one shot.
    -- Path is relative to the working directory where dbt is invoked (dbt_project/).
    select *
    from read_parquet('../raw_layer/matches/*.parquet')
),

deduped as (
    -- Keep only the most recent scrape for each match_id
    select *
    from raw
    qualify row_number() over (
        partition by match_id
        order by scraped_at desc
    ) = 1
),

typed as (
    select
        cast(match_id as varchar)                                   as match_id,
        cast(match_url as varchar)                                  as match_url,
        cast(team1_name as varchar)                                 as team1_name,
        cast(team2_name as varchar)                                 as team2_name,
        cast(team1_score as integer)                                as team1_score,
        cast(team2_score as integer)                                as team2_score,

        -- Fill null match_date (featured block) from scraped_at timestamp
        coalesce(
            cast(match_date as date),
            cast(scraped_at as date)
        )                                                           as match_date,

        cast(event_name as varchar)                                 as event_name,

        -- Normalise 'Other' → 'a-tier' before the window function
        lower(case
            when event_tier = 'Other' then 'a-tier'
            else event_tier
        end)                                                        as event_tier_raw,

        -- Normalise 'unknown' format → NULL
        case
            when match_format = 'unknown' then null
            else lower(match_format)
        end                                                         as match_format,

        cast(scraped_at as varchar)                                 as scraped_at

    from deduped
),

tier_normalised as (
    select
        *,
        -- Propagate the highest tier seen for each event across all its matches.
        -- Priority order (DuckDB MAX on strings): s-tier > major > a-tier
        -- We coalesce with event_tier_raw so isolated events keep their own value.
        coalesce(
            max(case
                when event_tier_raw in ('major', 's-tier', 'a-tier')
                then event_tier_raw
            end) over (partition by event_name),
            event_tier_raw
        )                                                           as event_tier

    from typed
),

final as (
    select
        match_id,
        match_url,
        match_date,
        team1_name,
        team2_name,
        team1_score,
        team2_score,
        event_name,
        event_tier,
        match_format,

        -- Winner: team with more maps won; null if scores are equal or missing
        case
            when team1_score > team2_score then team1_name
            when team2_score > team1_score then team2_name
            else null
        end                                                         as match_winner,

        scraped_at

    from tier_normalised
)

select * from final
