{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "fact_staking_rewards_id",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['gold', 'gov', 'staking']
) }}

-- This model combines two reward-related events:
-- 1. ValidatorRewarded: validatorId (indexed), from (indexed), amount, epoch
--    Emitted when block reward allocated via syscallReward
-- 2. ClaimRewards: validatorId (indexed), delegator (indexed), amount, epoch
--    Emitted when delegator claims rewards via claimRewards

WITH validator_rewards AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        event_name,
        decoded_log:validatorId::INTEGER AS validator_id,
        decoded_log:from::STRING AS reward_source,
        NULL AS delegator_address,
        decoded_log:amount::INTEGER AS amount_raw,
        decoded_log:amount::INTEGER / POW(10, 18) AS amount,
        decoded_log:epoch::INTEGER AS epoch,
        'block_reward' AS reward_type,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        modified_timestamp
    FROM
        {{ ref('silver__staking_events') }}
    WHERE
        event_name = 'ValidatorRewarded'
{% if is_incremental() %}
        AND modified_timestamp > (
            SELECT COALESCE(MAX(modified_timestamp), '1970-01-01'::TIMESTAMP)
            FROM {{ this }}
        )
{% endif %}
),

claim_rewards AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        event_name,
        decoded_log:validatorId::INTEGER AS validator_id,
        NULL AS reward_source,
        decoded_log:delegator::STRING AS delegator_address,
        decoded_log:amount::INTEGER AS amount_raw,
        decoded_log:amount::INTEGER / POW(10, 18) AS amount,
        decoded_log:epoch::INTEGER AS epoch,
        'claimed' AS reward_type,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        modified_timestamp
    FROM
        {{ ref('silver__staking_events') }}
    WHERE
        event_name = 'ClaimRewards'
{% if is_incremental() %}
        AND modified_timestamp > (
            SELECT COALESCE(MAX(modified_timestamp), '1970-01-01'::TIMESTAMP)
            FROM {{ this }}
        )
{% endif %}
),

combined AS (
    SELECT * FROM validator_rewards
    UNION ALL
    SELECT * FROM claim_rewards
)

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    contract_address,
    event_name,
    validator_id,
    reward_source,
    delegator_address,
    amount_raw,
    amount,
    epoch,
    reward_type,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    {{ dbt_utils.generate_surrogate_key(['tx_hash', 'event_index']) }} AS fact_staking_rewards_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    combined
