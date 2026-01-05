{{ config (
    materialized = "view",
    tags = ['gov', 'curated_daily']
) }}

/*
Daily staking totals rolled up to the validator level.
Aggregates all delegator balances per validator per day.

Staking balance states:
1. TOTAL_ACTIVE_STAKE: Sum of all delegators' active stake for this validator
2. TOTAL_PENDING_WITHDRAWAL: Sum of all pending withdrawals across delegators
3. TOTAL_STAKE_AT_RISK: Active + pending (both subject to slashing until withdrawn)

Includes delegator counts and daily change metrics.
*/

WITH validator_totals AS (
    SELECT
        balance_date,
        validator_id,
        SUM(active_balance) AS total_active_stake,
        SUM(pending_withdrawal_balance) AS total_pending_withdrawal,
        SUM(active_balance + pending_withdrawal_balance) AS total_stake_at_risk,
        SUM(daily_active_change) AS daily_active_change,
        SUM(daily_pending_change) AS daily_pending_change,
        COUNT(DISTINCT delegator_address) AS delegator_count,
        COUNT(DISTINCT CASE WHEN active_balance > 0 THEN delegator_address END) AS active_delegator_count
    FROM
        {{ ref('gov__ez_staking_balances_daily') }}
    GROUP BY
        balance_date,
        validator_id
),

-- Get last price of each day
prices AS (
    SELECT
        hour::DATE AS price_date,
        price AS mon_price_usd
    FROM
        {{ ref('price__ez_prices_hourly') }}
    WHERE
        is_native
    QUALIFY ROW_NUMBER() OVER (PARTITION BY hour::DATE ORDER BY hour DESC) = 1
)

SELECT
    vt.balance_date,
    vt.validator_id,
    v.validator_name,
    v.consensus_address,
    vt.total_active_stake,
    ROUND(vt.total_active_stake * p.mon_price_usd, 2) AS total_active_stake_usd,
    vt.total_pending_withdrawal,
    ROUND(vt.total_pending_withdrawal * p.mon_price_usd, 2) AS total_pending_withdrawal_usd,
    vt.total_stake_at_risk,
    ROUND(vt.total_stake_at_risk * p.mon_price_usd, 2) AS total_stake_at_risk_usd,
    vt.daily_active_change,
    vt.daily_pending_change,
    vt.delegator_count,
    vt.active_delegator_count,
    p.mon_price_usd,
    {{ dbt_utils.generate_surrogate_key(['vt.balance_date', 'vt.validator_id']) }} AS ez_validator_totals_daily_id
FROM
    validator_totals vt
LEFT JOIN
    {{ ref('gov__dim_validators') }} v
    ON vt.validator_id = v.validator_id
LEFT JOIN
    prices p
    ON vt.balance_date = p.price_date
