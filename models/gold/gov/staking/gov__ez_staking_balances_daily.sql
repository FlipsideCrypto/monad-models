{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "ez_staking_balances_daily_id",
    cluster_by = ['balance_date'],
    tags = ['gov', 'curated_daily']
) }}

/*
Staking balance states:
1. ACTIVE_BALANCE: Currently delegated stake (from Delegate events, reduced by Undelegate)
2. PENDING_WITHDRAWAL_BALANCE: Undelegated but not yet withdrawn (between Undelegate and Withdraw)
3. Total stake at risk = active_balance + pending_withdrawal_balance (both subject to slashing until withdrawn)

Flow:
- Delegate: increases active_balance
- Undelegate: decreases active_balance, increases pending_withdrawal_balance
- Withdraw: decreases pending_withdrawal_balance by the UNDELEGATION amount (not withdrawal amount,
  which includes rewards earned during the withdrawal delay period)

IMPORTANT: This model calculates balances from event history. If events are missing
(e.g., delegations before data collection started), balances will be incorrect.
For accurate point-in-time balances, use fact_validator_snapshots instead.

A full refresh is required after fixing the incremental logic to rebuild correct balances.
*/

WITH
{% if is_incremental() %}
-- Get the last known balances before our incremental window as starting point
previous_balances AS (
    SELECT
        validator_id,
        delegator_address,
        active_balance AS prev_active_balance,
        pending_withdrawal_balance AS prev_pending_balance
    FROM
        {{ this }}
    WHERE
        balance_date = (SELECT MAX(balance_date) - INTERVAL '3 days' FROM {{ this }})
),
{% endif %}

date_spine AS (
    SELECT
        date_day AS balance_date
    FROM
        {{ source('crosschain_gold', 'dim_dates') }}
    WHERE
        date_day <= CURRENT_DATE
{% if is_incremental() %}
        AND date_day > (SELECT MAX(balance_date) - INTERVAL '3 days' FROM {{ this }})
{% endif %}
),

-- Delegations increase active balance
delegations AS (
    SELECT
        block_timestamp::DATE AS action_date,
        validator_id,
        delegator_address,
        amount AS active_change,
        0 AS pending_change
    FROM
        {{ ref('gov__fact_delegations') }}
{% if is_incremental() %}
    WHERE
        block_timestamp::DATE > (SELECT MAX(balance_date) - INTERVAL '3 days' FROM {{ this }})
{% endif %}
),

-- Undelegations move from active to pending withdrawal
undelegations AS (
    SELECT
        block_timestamp::DATE AS action_date,
        validator_id,
        delegator_address,
        -amount AS active_change,
        amount AS pending_change
    FROM
        {{ ref('gov__fact_undelegations') }}
{% if is_incremental() %}
    WHERE
        block_timestamp::DATE > (SELECT MAX(balance_date) - INTERVAL '3 days' FROM {{ this }})
{% endif %}
),

-- Withdrawals remove from pending (funds returned to delegator)
-- Use the UNDELEGATION amount (not withdrawal amount) since withdrawal includes rewards earned during delay
-- withdraw_id can be reused after withdrawal, so match to most recent undelegation BEFORE the withdrawal
withdrawals AS (
    SELECT
        w.block_timestamp::DATE AS action_date,
        w.validator_id,
        w.delegator_address,
        0 AS active_change,
        -COALESCE(u.amount, w.amount) AS pending_change  -- Use undelegation amount if available
    FROM
        {{ ref('gov__fact_withdrawals') }} w
    LEFT JOIN
        {{ ref('gov__fact_undelegations') }} u
        ON w.validator_id = u.validator_id
        AND w.delegator_address = u.delegator_address
        AND w.withdraw_id = u.withdraw_id
        AND u.block_timestamp < w.block_timestamp  -- Undelegation must be before withdrawal
{% if is_incremental() %}
    WHERE
        w.block_timestamp::DATE > (SELECT MAX(balance_date) - INTERVAL '3 days' FROM {{ this }})
{% endif %}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY w.validator_id, w.delegator_address, w.withdraw_id, w.block_timestamp
        ORDER BY u.block_timestamp DESC NULLS LAST  -- Get most recent undelegation before this withdrawal
    ) = 1
),

all_actions AS (
    SELECT * FROM delegations
    UNION ALL
    SELECT * FROM undelegations
    UNION ALL
    SELECT * FROM withdrawals
),

-- Get all unique validator-delegator pairs
-- In incremental mode, include pairs from previous balances + new actions
validator_delegator_pairs AS (
    SELECT DISTINCT
        validator_id,
        delegator_address
    FROM
        all_actions
{% if is_incremental() %}
    UNION
    SELECT DISTINCT
        validator_id,
        delegator_address
    FROM
        previous_balances
{% endif %}
),

-- Cross join pairs with dates to get all possible combinations
pairs_with_dates AS (
    SELECT
        d.balance_date,
        p.validator_id,
        p.delegator_address
    FROM
        date_spine d
    CROSS JOIN
        validator_delegator_pairs p
),

-- Calculate daily net changes
-- Round to 6 decimals to eliminate floating-point precision artifacts early
daily_changes AS (
    SELECT
        action_date,
        validator_id,
        delegator_address,
        ROUND(SUM(active_change), 6) AS daily_active_change,
        ROUND(SUM(pending_change), 6) AS daily_pending_change
    FROM
        all_actions
    GROUP BY
        action_date,
        validator_id,
        delegator_address
),

-- Join and calculate running balances
-- Zero out values smaller than threshold (0.0001) to eliminate floating-point artifacts
balances AS (
    SELECT
        pwd.balance_date,
        pwd.validator_id,
        pwd.delegator_address,
        COALESCE(dc.daily_active_change, 0) AS daily_active_change,
        COALESCE(dc.daily_pending_change, 0) AS daily_pending_change,
{% if is_incremental() %}
        -- In incremental mode, add previous balance as starting point
        CASE
            WHEN ABS(COALESCE(pb.prev_active_balance, 0) + SUM(COALESCE(dc.daily_active_change, 0)) OVER (
                PARTITION BY pwd.validator_id, pwd.delegator_address
                ORDER BY pwd.balance_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )) < 0.0001 THEN 0
            ELSE ROUND(COALESCE(pb.prev_active_balance, 0) + SUM(COALESCE(dc.daily_active_change, 0)) OVER (
                PARTITION BY pwd.validator_id, pwd.delegator_address
                ORDER BY pwd.balance_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ), 6)
        END AS active_balance,
        CASE
            WHEN ABS(COALESCE(pb.prev_pending_balance, 0) + SUM(COALESCE(dc.daily_pending_change, 0)) OVER (
                PARTITION BY pwd.validator_id, pwd.delegator_address
                ORDER BY pwd.balance_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )) < 0.0001 THEN 0
            ELSE ROUND(COALESCE(pb.prev_pending_balance, 0) + SUM(COALESCE(dc.daily_pending_change, 0)) OVER (
                PARTITION BY pwd.validator_id, pwd.delegator_address
                ORDER BY pwd.balance_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ), 6)
        END AS pending_withdrawal_balance
{% else %}
        -- Full refresh: sum from beginning
        CASE
            WHEN ABS(SUM(COALESCE(dc.daily_active_change, 0)) OVER (
                PARTITION BY pwd.validator_id, pwd.delegator_address
                ORDER BY pwd.balance_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )) < 0.0001 THEN 0
            ELSE ROUND(SUM(COALESCE(dc.daily_active_change, 0)) OVER (
                PARTITION BY pwd.validator_id, pwd.delegator_address
                ORDER BY pwd.balance_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ), 6)
        END AS active_balance,
        CASE
            WHEN ABS(SUM(COALESCE(dc.daily_pending_change, 0)) OVER (
                PARTITION BY pwd.validator_id, pwd.delegator_address
                ORDER BY pwd.balance_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )) < 0.0001 THEN 0
            ELSE ROUND(SUM(COALESCE(dc.daily_pending_change, 0)) OVER (
                PARTITION BY pwd.validator_id, pwd.delegator_address
                ORDER BY pwd.balance_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ), 6)
        END AS pending_withdrawal_balance
{% endif %}
    FROM
        pairs_with_dates pwd
    LEFT JOIN
        daily_changes dc
        ON pwd.balance_date = dc.action_date
        AND pwd.validator_id = dc.validator_id
        AND pwd.delegator_address = dc.delegator_address
{% if is_incremental() %}
    LEFT JOIN
        previous_balances pb
        ON pwd.validator_id = pb.validator_id
        AND pwd.delegator_address = pb.delegator_address
{% endif %}
)

SELECT
    b.balance_date,
    b.validator_id,
    v.validator_name,
    v.consensus_address,
    b.delegator_address,
    b.active_balance,
    b.pending_withdrawal_balance,
    b.active_balance + b.pending_withdrawal_balance AS total_balance_at_risk,
    b.daily_active_change,
    b.daily_pending_change,
    {{ dbt_utils.generate_surrogate_key(['b.balance_date', 'b.validator_id', 'b.delegator_address']) }} AS ez_staking_balances_daily_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    balances b
LEFT JOIN
    {{ ref('gov__dim_validators') }} v
    ON b.validator_id = v.validator_id
WHERE
    b.active_balance != 0
    OR b.pending_withdrawal_balance != 0
    OR b.daily_active_change != 0
    OR b.daily_pending_change != 0
