{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "ez_staking_balances_daily_id",
    cluster_by = ['balance_date'],
    tags = ['gold', 'gov', 'staking', 'curated_daily']
) }}

/*
Staking balance states:
1. ACTIVE_BALANCE: Currently delegated stake (from Delegate events, reduced by Undelegate)
2. PENDING_WITHDRAWAL_BALANCE: Undelegated but not yet withdrawn (between Undelegate and Withdraw)
3. Total stake at risk = active_balance + pending_withdrawal_balance (both subject to slashing until withdrawn)

Flow:
- Delegate: increases active_balance
- Undelegate: decreases active_balance, increases pending_withdrawal_balance
- Withdraw: decreases pending_withdrawal_balance (funds returned to delegator)
*/

WITH date_spine AS (
    SELECT
        date_day AS balance_date
    FROM
        {{ source('crosschain_gold', 'dim_dates') }}
    WHERE
        date_day <= CURRENT_DATE
{% if is_incremental() %}
        AND date_day >= (SELECT MAX(balance_date) - INTERVAL '3 days' FROM {{ this }})
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
        {{ ref('gov__fact_staking_delegations') }}
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
        {{ ref('gov__fact_staking_undelegations') }}
),

-- Withdrawals remove from pending (funds returned to delegator)
withdrawals AS (
    SELECT
        block_timestamp::DATE AS action_date,
        validator_id,
        delegator_address,
        0 AS active_change,
        -amount AS pending_change
    FROM
        {{ ref('gov__fact_staking_withdrawals') }}
),

all_actions AS (
    SELECT * FROM delegations
    UNION ALL
    SELECT * FROM undelegations
    UNION ALL
    SELECT * FROM withdrawals
),

-- Get all unique validator-delegator pairs
validator_delegator_pairs AS (
    SELECT DISTINCT
        validator_id,
        delegator_address
    FROM
        all_actions
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
daily_changes AS (
    SELECT
        action_date,
        validator_id,
        delegator_address,
        SUM(active_change) AS daily_active_change,
        SUM(pending_change) AS daily_pending_change
    FROM
        all_actions
    GROUP BY
        action_date,
        validator_id,
        delegator_address
),

-- Join and calculate running balances
balances AS (
    SELECT
        pwd.balance_date,
        pwd.validator_id,
        pwd.delegator_address,
        COALESCE(dc.daily_active_change, 0) AS daily_active_change,
        COALESCE(dc.daily_pending_change, 0) AS daily_pending_change,
        SUM(COALESCE(dc.daily_active_change, 0)) OVER (
            PARTITION BY pwd.validator_id, pwd.delegator_address
            ORDER BY pwd.balance_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS active_balance,
        SUM(COALESCE(dc.daily_pending_change, 0)) OVER (
            PARTITION BY pwd.validator_id, pwd.delegator_address
            ORDER BY pwd.balance_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS pending_withdrawal_balance
    FROM
        pairs_with_dates pwd
    LEFT JOIN
        daily_changes dc
        ON pwd.balance_date = dc.action_date
        AND pwd.validator_id = dc.validator_id
        AND pwd.delegator_address = dc.delegator_address
)

SELECT
    balance_date,
    validator_id,
    delegator_address,
    active_balance,
    pending_withdrawal_balance,
    active_balance + pending_withdrawal_balance AS total_balance_at_risk,
    daily_active_change,
    daily_pending_change,
    {{ dbt_utils.generate_surrogate_key(['balance_date', 'validator_id', 'delegator_address']) }} AS ez_staking_balances_daily_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    balances
WHERE
    active_balance != 0
    OR pending_withdrawal_balance != 0
    OR daily_active_change != 0
    OR daily_pending_change != 0
