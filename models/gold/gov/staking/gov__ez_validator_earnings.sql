{{ config (
    materialized = "view",
    tags = ['gov', 'curated_daily']
) }}

/*
Validator total earnings calculated as:
- Daily earned = claimed rewards that day + change in unclaimed balance from prior day + priority fees

This uses the getDelegator snapshot (self-delegation) which captures the validator's
TOTAL earnings including:
1. Commission earned from delegators' rewards
2. Proportional rewards earned on the validator's own self-stake
3. Priority fees from transactions in blocks they produced

The claimRewards function claims from the delegator struct, which accumulates both
commission (deposited by syscallReward) and self-stake rewards. So getDelegator's
unclaimed_rewards represents the complete picture of validator earnings.

Priority fees are added separately as they go directly to the block producer.

Note: getValidator.unclaimed_rewards is the reward POOL for all delegators, not
the validator's personal earnings - that's why we use getDelegator instead.
*/

WITH daily_self_delegation AS (
    SELECT
        validator_id,
        snapshot_date,
        stake AS self_stake,
        unclaimed_rewards,
        unclaimed_rewards_usd,
        mon_price_usd,
        LAG(unclaimed_rewards) OVER (PARTITION BY validator_id ORDER BY snapshot_date) AS prev_unclaimed,
        LAG(snapshot_date) OVER (PARTITION BY validator_id ORDER BY snapshot_date) AS prev_snapshot_date
    FROM {{ ref('gov__fact_validator_self_delegation_snapshots') }}
),

daily_claims AS (
    -- Claims by the validator's auth_address for their own validator_id
    SELECT
        rc.validator_id,
        rc.block_timestamp::DATE AS claim_date,
        SUM(rc.amount) AS claimed_amount,
        SUM(rc.amount_raw) AS claimed_amount_raw,
        COUNT(*) AS claim_count
    FROM {{ ref('gov__fact_rewards_claimed') }} rc
    INNER JOIN {{ ref('gov__fact_validators_created') }} vc
        ON rc.validator_id = vc.validator_id
        AND rc.delegator_address = vc.auth_address
    GROUP BY 1, 2
),

daily_priority_fees AS (
    -- Priority fees earned by validator per day
    SELECT
        validator_id,
        block_timestamp::DATE AS fee_date,
        SUM(total_priority_fee) AS total_priority_fees,
        COUNT(*) AS blocks_produced
    FROM {{ ref('gov__fact_block_priority_fees') }}
    WHERE validator_id IS NOT NULL
    GROUP BY 1, 2
)

SELECT
    s.validator_id,
    v.validator_name,
    v.consensus_address,
    s.snapshot_date AS earning_date,

    -- Validator's self-stake (for context)
    s.self_stake,

    -- Claimed rewards that day (by auth_address from their own validator)
    COALESCE(c.claimed_amount, 0) AS claimed_rewards,
    COALESCE(c.claimed_amount_raw, 0) AS claimed_rewards_raw,
    COALESCE(c.claim_count, 0) AS claim_count,

    -- Change in unclaimed balance (new rewards earned minus any claims)
    s.unclaimed_rewards - COALESCE(s.prev_unclaimed, 0) AS unclaimed_change,

    -- Priority fees earned from block production
    COALESCE(pf.total_priority_fees, 0) AS priority_fees,
    COALESCE(pf.blocks_produced, 0) AS blocks_produced,

    -- Total earned = claims + unclaimed change + priority fees
    -- This includes commission, self-stake rewards, AND priority fees
    COALESCE(c.claimed_amount, 0) + (s.unclaimed_rewards - COALESCE(s.prev_unclaimed, 0)) + COALESCE(pf.total_priority_fees, 0) AS total_earned,

    -- Current unclaimed balance (commission + self-stake rewards)
    s.unclaimed_rewards AS unclaimed_balance,
    s.unclaimed_rewards_usd AS unclaimed_balance_usd,

    -- Price at snapshot for USD calculations
    s.mon_price_usd,

    -- USD value of earnings (using snapshot price)
    (COALESCE(c.claimed_amount, 0) + (s.unclaimed_rewards - COALESCE(s.prev_unclaimed, 0)) + COALESCE(pf.total_priority_fees, 0)) * s.mon_price_usd AS total_earned_usd,
    COALESCE(pf.total_priority_fees, 0) * s.mon_price_usd AS priority_fees_usd,

    -- Metadata
    s.prev_snapshot_date,
    {{ dbt_utils.generate_surrogate_key(['s.validator_id', 's.snapshot_date']) }} AS ez_validator_earnings_id

FROM daily_self_delegation s
LEFT JOIN daily_claims c
    ON s.validator_id = c.validator_id
    AND s.snapshot_date = c.claim_date
LEFT JOIN daily_priority_fees pf
    ON s.validator_id = pf.validator_id
    AND s.snapshot_date = pf.fee_date
LEFT JOIN {{ ref('gov__dim_validators') }} v
    ON s.validator_id = v.validator_id
WHERE s.prev_unclaimed IS NOT NULL  -- Exclude first day (no prior comparison)
