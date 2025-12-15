{{ config (
    materialized = 'view',
    tags = ['silver', 'gov', 'staking', 'curated_daily']
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    contract_address,
    event_name,
    decoded_log,
    full_decoded_log,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    tx_succeeded,
    ez_decoded_event_logs_id AS staking_events_id,
    inserted_timestamp,
    modified_timestamp
FROM
   monad.core.ez_decoded_event_logs-- {{ ref('core__ez_decoded_event_logs') }}
WHERE
    contract_address = '0x0000000000000000000000000000000000001000'
    AND tx_succeeded
