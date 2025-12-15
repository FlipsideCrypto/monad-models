{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "fact_staking_validator_self_delegation_snapshots_id",
    cluster_by = ['snapshot_date'],
    tags = ['gold', 'gov', 'staking', 'curated_daily']
) }}

/*
Parsed daily validator self-delegation snapshots from getDelegator(validatorId, auth_address) LiveQuery calls.
Captures the validator's own delegation (self-stake) and their TOTAL unclaimed earnings.

Key insight: For the auth_address, unclaimed_rewards includes BOTH:
1. Commission - deposited by syscallReward into auth_address's DelInfo.rewards
2. Self-stake rewards - proportional share of rewards earned on their own stake

This is the SOURCE OF TRUTH for validator earnings. The getValidator.unclaimed_rewards is
the DELEGATOR POOL (total rewards for all delegators), not validator personal earnings.

getDelegator returns (DelInfo struct):
  - stake: Current active self-stake (DelInfo.stake)
  - accRewardPerToken: Last checked accumulator (DelInfo.acc)
  - unclaimedRewards: Total pending earnings = commission + self-stake rewards (DelInfo.rewards)
  - deltaStake: Stake to be activated in delta_epoch (DelInfo.delta_stake)
  - nextDeltaStake: Stake to be activated in next_delta_epoch (DelInfo.next_delta_stake)
  - deltaEpoch: Epoch when deltaStake becomes active (DelInfo.delta_epoch)
  - nextDeltaEpoch: Epoch when nextDeltaStake becomes active (DelInfo.next_delta_epoch)
*/

WITH prices AS (
    SELECT
        hour::DATE AS price_date,
        price AS mon_price_usd
    FROM
        {{ ref('price__ez_prices_hourly') }}
    WHERE
        is_native = TRUE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY hour::DATE ORDER BY hour DESC) = 1
),

raw_snapshots AS (
    SELECT
        snapshot_date,
        validator_id,
        auth_address,
        snapshot_block,
        response
    FROM
        {{ ref('silver__staking_snapshot_get_delegator') }}
{% if is_incremental() %}
    WHERE
        modified_timestamp > (SELECT MAX(modified_timestamp) FROM {{ this }})
{% endif %}
),

parsed AS (
    SELECT
        snapshot_date,
        validator_id,
        auth_address,
        snapshot_block,
        response:data:result::STRING AS delegator_data_hex,
        -- Split into 64-char (32-byte) segments
        REGEXP_SUBSTR_ALL(SUBSTR(delegator_data_hex, 3, LEN(delegator_data_hex)), '.{64}') AS segmented_data,
        -- Parse the 7 return values
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data[0]::STRING)) AS stake_raw,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data[1]::STRING)) AS acc_reward_per_token_raw,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data[2]::STRING)) AS unclaimed_rewards_raw,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data[3]::STRING)) AS delta_stake_raw,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data[4]::STRING)) AS next_delta_stake_raw,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data[5]::STRING)) AS delta_epoch,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data[6]::STRING)) AS next_delta_epoch
    FROM
        raw_snapshots
    WHERE
        delegator_data_hex IS NOT NULL
        AND delegator_data_hex != '0x'
)

SELECT
    p.snapshot_date,
    p.validator_id,
    p.auth_address,
    p.snapshot_block,

    -- Active self-stake
    p.stake_raw,
    p.stake_raw / POW(10, 18) AS stake,
    ROUND((p.stake_raw / POW(10, 18)) * pr.mon_price_usd, 2) AS stake_usd,

    -- Accumulator checkpoint
    p.acc_reward_per_token_raw,

    -- Unclaimed rewards: commission + self-stake rewards (TOTAL validator earnings)
    p.unclaimed_rewards_raw,
    p.unclaimed_rewards_raw / POW(10, 18) AS unclaimed_rewards,
    ROUND((p.unclaimed_rewards_raw / POW(10, 18)) * pr.mon_price_usd, 2) AS unclaimed_rewards_usd,

    -- Pending stake changes
    p.delta_stake_raw,
    p.delta_stake_raw / POW(10, 18) AS delta_stake,
    p.next_delta_stake_raw,
    p.next_delta_stake_raw / POW(10, 18) AS next_delta_stake,

    -- Epochs when pending stakes activate
    p.delta_epoch,
    p.next_delta_epoch,

    -- Total pending stake
    (p.delta_stake_raw + p.next_delta_stake_raw) / POW(10, 18) AS total_pending_stake,

    -- Price at snapshot
    pr.mon_price_usd,

    {{ dbt_utils.generate_surrogate_key(['p.snapshot_date', 'p.validator_id']) }} AS fact_staking_validator_self_delegation_snapshots_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    parsed p
LEFT JOIN
    prices pr
    ON p.snapshot_date = pr.price_date
