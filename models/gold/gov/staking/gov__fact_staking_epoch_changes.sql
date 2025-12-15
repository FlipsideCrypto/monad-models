{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "fact_staking_epoch_changes_id",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['gold', 'gov', 'staking', 'curated_daily']
) }}

-- EpochChanged: oldEpoch, newEpoch
-- Emitted when epoch transitions via syscallOnEpochChange

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    contract_address,
    event_name,
    decoded_log:oldEpoch::INTEGER AS old_epoch,
    decoded_log:newEpoch::INTEGER AS new_epoch,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    {{ dbt_utils.generate_surrogate_key(['tx_hash', 'event_index']) }} AS fact_staking_epoch_changes_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref('silver__staking_events') }}
WHERE
    event_name = 'EpochChanged'
{% if is_incremental() %}
    AND modified_timestamp > (
        SELECT COALESCE(MAX(modified_timestamp), '1970-01-01'::TIMESTAMP)
        FROM {{ this }}
    )
{% endif %}
