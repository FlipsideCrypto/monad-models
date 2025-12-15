{{ config (
    materialized = "view",
    tags = ['gold', 'gov', 'staking', 'curated_daily']
) }}

/*
Compound vs sell decision support data per validator.
Aggregates unclaimed rewards, price trends, volatility, and recent
claim activity to provide data points for reward management decisions.

Includes a simple rule-based recommendation based on price trends
and volatility. Actual decisions should consider additional factors
like tax implications, cash flow needs, and market outlook.
*/

WITH latest_snapshot AS (
    SELECT
        validator_id,
        snapshot_date,
        unclaimed_rewards,
        unclaimed_rewards_usd,
        mon_price_usd
    FROM {{ ref('gov__fact_staking_validator_self_delegation_snapshots') }}
    WHERE snapshot_date = (
        SELECT MAX(snapshot_date)
        FROM {{ ref('gov__fact_staking_validator_self_delegation_snapshots') }}
    )
),

price_trend AS (
    SELECT
        AVG(CASE WHEN hour >= DATEADD('day', -1, CURRENT_TIMESTAMP) THEN price END) AS price_1d_avg,
        AVG(CASE WHEN hour >= DATEADD('day', -7, CURRENT_TIMESTAMP) THEN price END) AS price_7d_avg,
        AVG(CASE WHEN hour >= DATEADD('day', -30, CURRENT_TIMESTAMP) THEN price END) AS price_30d_avg,
        STDDEV(CASE WHEN hour >= DATEADD('day', -30, CURRENT_TIMESTAMP) THEN price END) AS price_volatility_30d,
        MIN(CASE WHEN hour >= DATEADD('day', -30, CURRENT_TIMESTAMP) THEN price END) AS price_30d_low,
        MAX(CASE WHEN hour >= DATEADD('day', -30, CURRENT_TIMESTAMP) THEN price END) AS price_30d_high
    FROM {{ ref('price__ez_prices_hourly') }}
    WHERE is_native = TRUE
      AND hour >= DATEADD('day', -30, CURRENT_TIMESTAMP)
),

recent_claims AS (
    SELECT
        rc.validator_id,
        COUNT(*) AS claims_last_30d,
        SUM(rc.amount) AS claimed_amount_30d,
        MAX(rc.block_timestamp) AS last_claim_timestamp
    FROM {{ ref('gov__fact_staking_rewards_claimed') }} rc
    INNER JOIN {{ ref('gov__fact_staking_validators_created') }} vc
        ON rc.validator_id = vc.validator_id
        AND rc.delegator_address = vc.auth_address
    WHERE rc.block_timestamp >= DATEADD('day', -30, CURRENT_DATE)
    GROUP BY 1
),

recent_earnings AS (
    SELECT
        validator_id,
        SUM(total_earned) AS earnings_30d,
        AVG(total_earned) AS avg_daily_earnings
    FROM {{ ref('gov__ez_staking_validator_earnings') }}
    WHERE earning_date >= DATEADD('day', -30, CURRENT_DATE)
    GROUP BY 1
)

SELECT
    s.validator_id,
    v.validator_name,
    s.snapshot_date,

    -- Current unclaimed balance
    s.unclaimed_rewards,
    s.unclaimed_rewards_usd,
    s.mon_price_usd AS current_price,

    -- Price trends
    p.price_1d_avg,
    p.price_7d_avg,
    p.price_30d_avg,
    p.price_30d_low,
    p.price_30d_high,
    p.price_volatility_30d,

    -- Price change percentages
    ROUND((s.mon_price_usd - p.price_7d_avg) / NULLIF(p.price_7d_avg, 0) * 100, 2) AS price_change_7d_pct,
    ROUND((s.mon_price_usd - p.price_30d_avg) / NULLIF(p.price_30d_avg, 0) * 100, 2) AS price_change_30d_pct,

    -- Recent claim activity
    COALESCE(c.claims_last_30d, 0) AS claims_last_30d,
    COALESCE(c.claimed_amount_30d, 0) AS claimed_amount_30d,
    c.last_claim_timestamp,
    DATEDIFF('day', c.last_claim_timestamp, CURRENT_TIMESTAMP) AS days_since_last_claim,

    -- Recent earnings
    COALESCE(e.earnings_30d, 0) AS earnings_30d,
    COALESCE(e.avg_daily_earnings, 0) AS avg_daily_earnings,

    -- Days of earnings accumulated (unclaimed / avg daily)
    CASE
        WHEN e.avg_daily_earnings > 0
        THEN ROUND(s.unclaimed_rewards / e.avg_daily_earnings, 1)
        ELSE NULL
    END AS days_of_earnings_accumulated,

    -- Simple recommendation logic
    CASE
        WHEN (s.mon_price_usd - p.price_30d_avg) / NULLIF(p.price_30d_avg, 0) > 0.2
            THEN 'Consider selling - price up 20%+ from 30d avg'
        WHEN (s.mon_price_usd - p.price_30d_avg) / NULLIF(p.price_30d_avg, 0) < -0.2
            THEN 'Consider compounding - price down 20%+ from 30d avg'
        WHEN p.price_volatility_30d > p.price_30d_avg * 0.1
            THEN 'High volatility - consider partial claim'
        WHEN s.unclaimed_rewards / NULLIF(e.avg_daily_earnings, 0) > 30
            THEN 'Large balance accumulated - consider claiming'
        ELSE 'Neutral - compound or hold based on cash needs'
    END AS recommendation,

    {{ dbt_utils.generate_surrogate_key(['s.validator_id', 's.snapshot_date']) }} AS ez_staking_compound_decision_id

FROM latest_snapshot s
CROSS JOIN price_trend p
LEFT JOIN recent_claims c ON s.validator_id = c.validator_id
LEFT JOIN recent_earnings e ON s.validator_id = e.validator_id
LEFT JOIN {{ ref('gov__dim_staking_validators') }} v ON s.validator_id = v.validator_id
