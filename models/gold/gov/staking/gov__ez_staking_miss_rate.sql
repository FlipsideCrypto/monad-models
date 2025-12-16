{{ config (
    materialized = "view",
    tags = ['gov', 'curated_daily']
) }}

/*
Miss rate and estimated commission impact per validator per epoch.
Calculates missed blocks as expected minus actual, and estimates
the commission lost due to missed block production.

Uses average daily commission from fact_staking_validator_earnings
to estimate impact, since missed blocks reduce validator's share
of block rewards proportionally.
*/

WITH performance AS (
    SELECT
        epoch,
        validator_id,
        validator_address,
        validators_in_snapshot,
        total_epoch_blocks,
        expected_blocks_per_validator,
        actual_blocks_produced,
        blocks_vs_expected,
        pct_of_epoch_blocks,
        production_rank,
        epoch_start_timestamp,
        epoch_end_timestamp
    FROM {{ ref('gov__ez_staking_validator_epoch_performance') }}
),

avg_daily_commission AS (
    SELECT
        validator_id,
        AVG(total_earned) AS avg_daily_commission_mon
    FROM {{ ref('gov__ez_staking_validator_earnings') }}
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
    p.epoch,
    p.validator_id,
    v.validator_name,
    v.consensus_address,
    p.validator_address,
    p.validators_in_snapshot,
    p.total_epoch_blocks,
    p.expected_blocks_per_validator,
    p.actual_blocks_produced,
    p.blocks_vs_expected,
    p.pct_of_epoch_blocks,
    p.production_rank,

    -- Missed blocks (0 if overproduced)
    GREATEST(p.expected_blocks_per_validator - p.actual_blocks_produced, 0) AS missed_blocks,

    -- Miss rate percentage
    CASE
        WHEN p.expected_blocks_per_validator > 0
        THEN ROUND((GREATEST(p.expected_blocks_per_validator - p.actual_blocks_produced, 0))
                   / p.expected_blocks_per_validator * 100, 2)
        ELSE 0
    END AS miss_rate_pct,

    -- Uptime percentage (inverse of miss rate)
    CASE
        WHEN p.expected_blocks_per_validator > 0
        THEN ROUND(LEAST(p.actual_blocks_produced / p.expected_blocks_per_validator, 1) * 100, 2)
        ELSE 100
    END AS uptime_pct,

    -- Estimated commission impact
    c.avg_daily_commission_mon,
    CASE
        WHEN p.expected_blocks_per_validator > 0
        THEN ROUND((GREATEST(p.expected_blocks_per_validator - p.actual_blocks_produced, 0))
                   / p.expected_blocks_per_validator * COALESCE(c.avg_daily_commission_mon, 0), 4)
        ELSE 0
    END AS estimated_missed_commission_mon,

    CASE
        WHEN p.expected_blocks_per_validator > 0
        THEN ROUND((GREATEST(p.expected_blocks_per_validator - p.actual_blocks_produced, 0))
                   / p.expected_blocks_per_validator * COALESCE(c.avg_daily_commission_mon, 0) * pr.mon_price_usd, 2)
        ELSE 0
    END AS estimated_missed_commission_usd,

    pr.mon_price_usd,
    p.epoch_start_timestamp,
    p.epoch_end_timestamp,

    {{ dbt_utils.generate_surrogate_key(['p.epoch', 'p.validator_id']) }} AS ez_staking_miss_rate_id

FROM performance p
LEFT JOIN {{ ref('gov__dim_staking_validators') }} v ON p.validator_id = v.validator_id
LEFT JOIN avg_daily_commission c ON p.validator_id = c.validator_id
CROSS JOIN current_price pr
