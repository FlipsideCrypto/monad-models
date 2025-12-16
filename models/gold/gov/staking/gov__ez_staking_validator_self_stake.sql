{{ config (
    materialized = 'view',
    tags = ['gov', 'curated_daily']
) }}

/*
Shows the percentage of each validator's total stake that is self-staked
(where the validator's auth_address equals the delegator_address).
Useful for analyzing validator skin-in-the-game.
*/

WITH validator_auth_addresses AS (
    SELECT
        validator_id,
        auth_address
    FROM
        {{ ref('gov__fact_staking_validators_created') }}
),

daily_balances AS (
    SELECT
        balance_date,
        validator_id,
        delegator_address,
        active_balance,
        pending_withdrawal_balance,
        total_balance_at_risk
    FROM
        {{ ref('gov__ez_staking_balances_daily') }}
),

validator_totals AS (
    SELECT
        balance_date,
        validator_id,
        SUM(active_balance) AS total_active_stake,
        SUM(pending_withdrawal_balance) AS total_pending_stake,
        SUM(total_balance_at_risk) AS total_stake_at_risk
    FROM
        daily_balances
    GROUP BY
        balance_date,
        validator_id
),

self_stake AS (
    SELECT
        db.balance_date,
        db.validator_id,
        db.active_balance AS self_active_stake,
        db.pending_withdrawal_balance AS self_pending_stake,
        db.total_balance_at_risk AS self_stake_at_risk
    FROM
        daily_balances db
    INNER JOIN
        validator_auth_addresses va
        ON db.validator_id = va.validator_id
        AND LOWER(db.delegator_address) = LOWER(va.auth_address)
)

SELECT
    vt.balance_date,
    vt.validator_id,
    v.validator_name,
    v.consensus_address,
    va.auth_address AS validator_address,
    COALESCE(ss.self_active_stake, 0) AS self_active_stake,
    vt.total_active_stake,
    COALESCE(ss.self_pending_stake, 0) AS self_pending_stake,
    vt.total_pending_stake,
    COALESCE(ss.self_stake_at_risk, 0) AS self_stake_at_risk,
    vt.total_stake_at_risk,
    CASE
        WHEN vt.total_active_stake > 0
        THEN ROUND(COALESCE(ss.self_active_stake, 0) / vt.total_active_stake * 100, 4)
        ELSE 0
    END AS self_stake_pct_of_active,
    CASE
        WHEN vt.total_stake_at_risk > 0
        THEN ROUND(COALESCE(ss.self_stake_at_risk, 0) / vt.total_stake_at_risk * 100, 4)
        ELSE 0
    END AS self_stake_pct_of_total
FROM
    validator_totals vt
INNER JOIN
    validator_auth_addresses va
    ON vt.validator_id = va.validator_id
LEFT JOIN
    {{ ref('gov__dim_staking_validators') }} v
    ON vt.validator_id = v.validator_id
LEFT JOIN
    self_stake ss
    ON vt.balance_date = ss.balance_date
    AND vt.validator_id = ss.validator_id
