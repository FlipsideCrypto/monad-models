{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "fact_validator_rewards_id",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['gov', 'curated_daily']
) }}

/*
ValidatorRewarded events - block rewards allocated to validators via syscallReward.
Emitted each block when rewards are distributed to the block producer.

ValidatorRewarded: validatorId (indexed), from (indexed), amount, epoch

Note: The amount represents rewards distributed to delegators after the validator's
commission has already been deducted.
*/

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    contract_address,
    event_name,
    decoded_log:validatorId::INTEGER AS validator_id,
    decoded_log:from::STRING AS reward_source,
    decoded_log:amount::INTEGER AS amount_raw,
    decoded_log:amount::INTEGER / POW(10, 18) AS amount,
    decoded_log:epoch::INTEGER AS epoch,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    {{ dbt_utils.generate_surrogate_key(['tx_hash', 'event_index']) }} AS fact_validator_rewards_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
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
