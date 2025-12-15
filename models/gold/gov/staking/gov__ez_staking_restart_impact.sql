{{ config (
    materialized = "view",
    tags = ['gold', 'gov', 'staking', 'curated_daily']
) }}

/*
Restart impact estimates per validator.
Calculates expected missed blocks and commission loss for different
restart durations (5 min, 15 min, 30 min, 1 hour) based on recent
performance metrics.

Useful for operational decision-making around maintenance windows.
*/

WITH validator_stats AS (
    SELECT
        validator_id,
        validator_address,
        AVG(actual_blocks_produced) AS avg_blocks_per_epoch,
        AVG(pct_of_epoch_blocks) AS avg_pct_share,
        COUNT(*) AS epochs_analyzed
    FROM {{ ref('gov__ez_staking_validator_epoch_performance') }}
    WHERE epoch >= (SELECT MAX(epoch) - 10 FROM {{ ref('gov__dim_staking_epochs') }})
    GROUP BY 1, 2
),

epoch_stats AS (
    SELECT
        AVG(epoch_block_count) AS avg_epoch_blocks,
        AVG(epoch_duration_seconds) AS avg_epoch_seconds
    FROM {{ ref('gov__dim_staking_epochs') }}
    WHERE epoch >= (SELECT MAX(epoch) - 10 FROM {{ ref('gov__dim_staking_epochs') }})
),

commission_stats AS (
    SELECT
        validator_id,
        AVG(total_earned) AS avg_daily_commission_mon
    FROM {{ ref('gov__ez_staking_validator_earnings') }}
    WHERE earning_date >= DATEADD('day', -7, CURRENT_DATE)
    GROUP BY 1
),

current_price AS (
    SELECT
        price AS mon_price_usd
    FROM {{ ref('price__ez_prices_hourly') }}
    WHERE is_native = TRUE
    ORDER BY hour DESC
    LIMIT 1
)

SELECT
    v.validator_id,
    v.validator_address,
    v.avg_blocks_per_epoch,
    v.avg_pct_share,
    v.epochs_analyzed,
    e.avg_epoch_blocks,
    e.avg_epoch_seconds,
    c.avg_daily_commission_mon,
    p.mon_price_usd,

    -- 5 minute restart estimates
    ROUND(300.0 / e.avg_epoch_seconds * v.avg_blocks_per_epoch) AS missed_blocks_5min,
    ROUND(c.avg_daily_commission_mon * (5.0 / 1440), 6) AS loss_mon_5min,
    ROUND(c.avg_daily_commission_mon * (5.0 / 1440) * p.mon_price_usd, 2) AS loss_usd_5min,

    -- 15 minute restart estimates
    ROUND(900.0 / e.avg_epoch_seconds * v.avg_blocks_per_epoch) AS missed_blocks_15min,
    ROUND(c.avg_daily_commission_mon * (15.0 / 1440), 6) AS loss_mon_15min,
    ROUND(c.avg_daily_commission_mon * (15.0 / 1440) * p.mon_price_usd, 2) AS loss_usd_15min,

    -- 30 minute restart estimates
    ROUND(1800.0 / e.avg_epoch_seconds * v.avg_blocks_per_epoch) AS missed_blocks_30min,
    ROUND(c.avg_daily_commission_mon * (30.0 / 1440), 6) AS loss_mon_30min,
    ROUND(c.avg_daily_commission_mon * (30.0 / 1440) * p.mon_price_usd, 2) AS loss_usd_30min,

    -- 1 hour restart estimates
    ROUND(3600.0 / e.avg_epoch_seconds * v.avg_blocks_per_epoch) AS missed_blocks_1hr,
    ROUND(c.avg_daily_commission_mon * (60.0 / 1440), 6) AS loss_mon_1hr,
    ROUND(c.avg_daily_commission_mon * (60.0 / 1440) * p.mon_price_usd, 2) AS loss_usd_1hr,

    {{ dbt_utils.generate_surrogate_key(['v.validator_id']) }} AS ez_staking_restart_impact_id

FROM validator_stats v
CROSS JOIN epoch_stats e
LEFT JOIN commission_stats c ON v.validator_id = c.validator_id
CROSS JOIN current_price p
