{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "fact_staking_rewards_claimed_id",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['gold', 'gov', 'staking', 'curated_daily']
) }}

/*
ClaimRewards events - when delegators claim their accumulated rewards.
Emitted when a delegator calls claimRewards to withdraw earned rewards.

ClaimRewards: validatorId (indexed), delegator (indexed), amount, epoch
*/

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    contract_address,
    event_name,
    decoded_log:validatorId::INTEGER AS validator_id,
    decoded_log:delegator::STRING AS delegator_address,
    decoded_log:amount::INTEGER AS amount_raw,
    decoded_log:amount::INTEGER / POW(10, 18) AS amount,
    decoded_log:epoch::INTEGER AS epoch,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    {{ dbt_utils.generate_surrogate_key(['tx_hash', 'event_index']) }} AS fact_staking_rewards_claimed_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
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
