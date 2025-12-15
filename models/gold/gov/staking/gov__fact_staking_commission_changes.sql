{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "fact_staking_commission_changes_id",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['gold', 'gov', 'staking', 'curated_daily']
) }}

-- CommissionChanged: validatorId (indexed), oldCommission, newCommission
-- Emitted when validator modifies commission via changeCommission

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    contract_address,
    event_name,
    decoded_log:validatorId::INTEGER AS validator_id,
    decoded_log:oldCommission::INTEGER AS old_commission,
    decoded_log:newCommission::INTEGER AS new_commission,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    {{ dbt_utils.generate_surrogate_key(['tx_hash', 'event_index']) }} AS fact_staking_commission_changes_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref('silver__staking_events') }}
WHERE
    event_name = 'CommissionChanged'
{% if is_incremental() %}
    AND modified_timestamp > (
        SELECT COALESCE(MAX(modified_timestamp), '1970-01-01'::TIMESTAMP)
        FROM {{ this }}
    )
{% endif %}
