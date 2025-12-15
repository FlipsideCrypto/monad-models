{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "fact_staking_undelegations_id",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['gold', 'gov', 'staking', 'curated_daily']
) }}

-- Undelegate: validatorId (indexed), delegator (indexed), withdrawId, amount, activationEpoch
-- Emitted when delegator initiates undelegation

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    contract_address,
    event_name,
    decoded_log:validatorId::INTEGER AS validator_id,
    decoded_log:delegator::STRING AS delegator_address,
    decoded_log:withdrawId::INTEGER AS withdraw_id,
    decoded_log:amount::INTEGER AS amount_raw,
    decoded_log:amount::INTEGER / POW(10, 18) AS amount,
    decoded_log:activationEpoch::INTEGER AS activation_epoch,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    {{ dbt_utils.generate_surrogate_key(['tx_hash', 'event_index']) }} AS fact_staking_undelegations_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref('silver__staking_events') }}
WHERE
    event_name = 'Undelegate'
{% if is_incremental() %}
    AND modified_timestamp > (
        SELECT COALESCE(MAX(modified_timestamp), '1970-01-01'::TIMESTAMP)
        FROM {{ this }}
    )
{% endif %}
