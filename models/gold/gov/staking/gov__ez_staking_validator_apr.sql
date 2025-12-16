{{ config (
    materialized = "view",
    tags = ['gov', 'curated_daily']
) }}

/*
Daily and rolling APR/APY calculations per validator.
APR = (daily_earnings / stake) * 365 * 100
APY = ((1 + daily_earnings / stake) ^ 365 - 1) * 100

Uses earnings from fact_staking_validator_earnings (commission + self-stake rewards)
and stake from fact_staking_validator_snapshots (execution_stake).
*/

WITH daily_data AS (
    SELECT
        e.validator_id,
        e.earning_date,
        e.total_earned,
        e.total_earned_usd,
        s.execution_stake,
        s.execution_stake_usd,
        e.mon_price_usd
    FROM {{ ref('gov__ez_staking_validator_earnings') }} e
    JOIN {{ ref('gov__fact_staking_validator_snapshots') }} s
        ON e.validator_id = s.validator_id
        AND e.earning_date = s.snapshot_date
    WHERE s.execution_stake > 0
)

SELECT
    d.validator_id,
    v.validator_name,
    v.consensus_address,
    d.earning_date,
    d.total_earned,
    d.total_earned_usd,
    d.execution_stake,
    d.execution_stake_usd,
    d.mon_price_usd,

    -- Daily return rate
    d.total_earned / d.execution_stake AS daily_return_rate,

    -- Annualized APR (simple)
    ROUND((d.total_earned / d.execution_stake) * 365 * 100, 4) AS apr_pct,

    -- Annualized APY (compound)
    ROUND((POWER(1 + (d.total_earned / d.execution_stake), 365) - 1) * 100, 4) AS apy_pct,

    -- 7-day rolling APR
    ROUND(AVG(d.total_earned / d.execution_stake) OVER (
        PARTITION BY d.validator_id
        ORDER BY d.earning_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) * 365 * 100, 4) AS apr_7d_rolling_pct,

    -- 30-day rolling APR
    ROUND(AVG(d.total_earned / d.execution_stake) OVER (
        PARTITION BY d.validator_id
        ORDER BY d.earning_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) * 365 * 100, 4) AS apr_30d_rolling_pct,

    {{ dbt_utils.generate_surrogate_key(['d.validator_id', 'd.earning_date']) }} AS ez_staking_validator_apr_id

FROM daily_data d
LEFT JOIN {{ ref('gov__dim_staking_validators') }} v
    ON d.validator_id = v.validator_id
