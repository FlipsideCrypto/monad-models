{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "fact_withdrawals_id",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['gov', 'curated_daily']
) }}

-- Withdraw: validatorId (indexed), delegator (indexed), withdrawId, amount, withdrawEpoch
-- Emitted upon successful withdraw execution

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
    decoded_log:withdrawEpoch::INTEGER AS withdraw_epoch,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    {{ dbt_utils.generate_surrogate_key(['tx_hash', 'event_index']) }} AS fact_withdrawals_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref('silver__staking_events') }}
WHERE
    event_name = 'Withdraw'
{% if is_incremental() %}
    AND modified_timestamp > (
        SELECT COALESCE(MAX(modified_timestamp), '1970-01-01'::TIMESTAMP)
        FROM {{ this }}
    )
{% endif %}
