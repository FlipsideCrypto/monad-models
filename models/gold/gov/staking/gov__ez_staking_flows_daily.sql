{{ config (
    materialized = "view",
    tags = ['gov', 'curated_daily']
) }}

/*
Daily staking flows aggregated by validator and delegator.
Combines delegations, undelegations, withdrawals, and claims into a single view.
Useful for tracking inflows/outflows and net position changes.
*/

WITH delegations AS (
    SELECT
        validator_id,
        delegator_address,
        block_timestamp::DATE AS flow_date,
        'delegation' AS flow_type,
        SUM(amount) AS amount,
        SUM(amount_raw) AS amount_raw,
        COUNT(*) AS tx_count
    FROM {{ ref('gov__fact_delegations') }}
    GROUP BY 1, 2, 3, 4
),

undelegations AS (
    SELECT
        validator_id,
        delegator_address,
        block_timestamp::DATE AS flow_date,
        'undelegation' AS flow_type,
        SUM(amount) AS amount,
        SUM(amount_raw) AS amount_raw,
        COUNT(*) AS tx_count
    FROM {{ ref('gov__fact_undelegations') }}
    GROUP BY 1, 2, 3, 4
),

withdrawals AS (
    SELECT
        validator_id,
        delegator_address,
        block_timestamp::DATE AS flow_date,
        'withdrawal' AS flow_type,
        SUM(amount) AS amount,
        SUM(amount_raw) AS amount_raw,
        COUNT(*) AS tx_count
    FROM {{ ref('gov__fact_withdrawals') }}
    GROUP BY 1, 2, 3, 4
),

claims AS (
    SELECT
        validator_id,
        delegator_address,
        block_timestamp::DATE AS flow_date,
        'claim' AS flow_type,
        SUM(amount) AS amount,
        SUM(amount_raw) AS amount_raw,
        COUNT(*) AS tx_count
    FROM {{ ref('gov__fact_rewards_claimed') }}
    GROUP BY 1, 2, 3, 4
),

all_flows AS (
    SELECT * FROM delegations
    UNION ALL
    SELECT * FROM undelegations
    UNION ALL
    SELECT * FROM withdrawals
    UNION ALL
    SELECT * FROM claims
)

SELECT
    f.flow_date,
    f.validator_id,
    v.validator_name,
    v.consensus_address,
    f.delegator_address,
    f.flow_type,
    f.amount,
    f.amount_raw,
    f.tx_count,
    {{ dbt_utils.generate_surrogate_key(['f.flow_date', 'f.validator_id', 'f.delegator_address', 'f.flow_type']) }} AS ez_staking_flows_daily_id
FROM all_flows f
LEFT JOIN {{ ref('gov__dim_validators') }} v
    ON f.validator_id = v.validator_id
