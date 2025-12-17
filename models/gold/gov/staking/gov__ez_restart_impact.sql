{{ config (
    materialized = "view",
    tags = ['gov', 'curated_daily']
) }}

/*
Restart impact estimates per validator.
Calculates expected missed blocks and commission loss for different
restart durations (5 min, 15 min, 30 min, 1 hour) based on recent
performance metrics.

Useful for operational decision-making around maintenance windows.
*/

WITH max_epoch AS (
    SELECT MAX(epoch) AS max_epoch
    FROM {{ ref('gov__ez_validator_epoch_performance') }}
),

validator_stats AS (
    SELECT
        p.validator_id,
        p.validator_address,
        AVG(p.actual_blocks_produced) AS avg_blocks_per_epoch,
        AVG(p.pct_of_epoch_blocks) AS avg_pct_share,
        AVG(p.total_block_rewards) AS avg_rewards_per_epoch,
        COUNT(*) AS epochs_analyzed
    FROM {{ ref('gov__ez_validator_epoch_performance') }} p
    CROSS JOIN max_epoch m
    WHERE p.epoch >= m.max_epoch - 10
    GROUP BY 1, 2
),

epoch_stats AS (
    SELECT
        AVG(e.epoch_block_count) AS avg_epoch_blocks,
        AVG(e.epoch_duration_seconds) AS avg_epoch_seconds
    FROM {{ ref('gov__dim_epochs') }} e
    INNER JOIN (
        SELECT DISTINCT epoch
        FROM {{ ref('gov__ez_validator_epoch_performance') }}
    ) p ON e.epoch = p.epoch
    CROSS JOIN max_epoch m
    WHERE e.epoch >= m.max_epoch - 10
      AND e.epoch_duration_seconds > 0
      AND e.epoch_block_count > 0
),

commission_stats AS (
    SELECT
        validator_id,
        AVG(total_earned) AS avg_daily_commission_mon
    FROM {{ ref('gov__ez_validator_earnings') }}
    WHERE earning_date >= DATEADD('day', -30, CURRENT_DATE)
    GROUP BY 1
),

current_price AS (
    SELECT
        COALESCE(
            (SELECT price
             FROM {{ ref('price__ez_prices_hourly') }}
             WHERE token_address = '0x0000000000000000000000000000000000000000'
             ORDER BY hour DESC
             LIMIT 1),
            0
        ) AS mon_price_usd
)

SELECT
    vs.validator_id,
    vd.validator_name,
    vd.consensus_address,
    vs.validator_address AS auth_address,
    vs.avg_blocks_per_epoch,
    vs.avg_pct_share,
    vs.avg_rewards_per_epoch,
    vs.epochs_analyzed,
    e.avg_epoch_blocks,
    e.avg_epoch_seconds,
    c.avg_daily_commission_mon,
    p.mon_price_usd,

    -- 5 minute restart estimates
    ROUND(300.0 / NULLIF(e.avg_epoch_seconds, 0) * vs.avg_blocks_per_epoch) AS missed_blocks_5min,
    ROUND(COALESCE(c.avg_daily_commission_mon, 0) * (5.0 / 1440), 6) AS loss_mon_5min,
    ROUND(COALESCE(c.avg_daily_commission_mon, 0) * (5.0 / 1440) * COALESCE(p.mon_price_usd, 0), 2) AS loss_usd_5min,

    -- 15 minute restart estimates
    ROUND(900.0 / NULLIF(e.avg_epoch_seconds, 0) * vs.avg_blocks_per_epoch) AS missed_blocks_15min,
    ROUND(COALESCE(c.avg_daily_commission_mon, 0) * (15.0 / 1440), 6) AS loss_mon_15min,
    ROUND(COALESCE(c.avg_daily_commission_mon, 0) * (15.0 / 1440) * COALESCE(p.mon_price_usd, 0), 2) AS loss_usd_15min,

    -- 30 minute restart estimates
    ROUND(1800.0 / NULLIF(e.avg_epoch_seconds, 0) * vs.avg_blocks_per_epoch) AS missed_blocks_30min,
    ROUND(COALESCE(c.avg_daily_commission_mon, 0) * (30.0 / 1440), 6) AS loss_mon_30min,
    ROUND(COALESCE(c.avg_daily_commission_mon, 0) * (30.0 / 1440) * COALESCE(p.mon_price_usd, 0), 2) AS loss_usd_30min,

    -- 1 hour restart estimates
    ROUND(3600.0 / NULLIF(e.avg_epoch_seconds, 0) * vs.avg_blocks_per_epoch) AS missed_blocks_1hr,
    ROUND(COALESCE(c.avg_daily_commission_mon, 0) * (60.0 / 1440), 6) AS loss_mon_1hr,
    ROUND(COALESCE(c.avg_daily_commission_mon, 0) * (60.0 / 1440) * COALESCE(p.mon_price_usd, 0), 2) AS loss_usd_1hr,

    {{ dbt_utils.generate_surrogate_key(['vs.validator_id']) }} AS ez_restart_impact_id

FROM validator_stats vs
LEFT JOIN {{ ref('gov__dim_validators') }} vd ON vs.validator_id = vd.validator_id
CROSS JOIN epoch_stats e
LEFT JOIN commission_stats c ON vs.validator_id = c.validator_id
CROSS JOIN current_price p
