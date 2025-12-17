{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "snapshot_validator_set_id",
    cluster_by = ['epoch'],
    tags = ['gov', 'curated_daily']
) }}

/*
Fetches the snapshot validator set for each epoch via LiveQuery.
The snapshot validator set determines which validators are eligible
for producing blocks in that epoch.

Uses getSnapshotValidatorSet(uint32 startIndex) which returns:
- isDone: boolean indicating if all validators have been returned
- nextIndex: next index to query if not done
- valIds[]: array of validator IDs in the snapshot

Queries at epoch_start_block to get the validator set that was active for that epoch.
*/

WITH new_epochs AS (
    SELECT
        epoch,
        epoch_start_block,
        utils.udf_int_to_hex(epoch_start_block) AS block_hex
    FROM
        {{ ref('gov__dim_epochs') }}
    WHERE
    1=1
       AND epoch_start_timestamp >= DATEADD('day', -16, CURRENT_DATE)
{% if is_incremental() %}
        AND epoch NOT IN (SELECT DISTINCT epoch FROM {{ this }})
{% endif %}
order by 1 limit 10
),

pages AS (
    SELECT
        e.epoch,
        e.epoch_start_block,
        e.block_hex,
        p.start_index,
        CONCAT(
            '0xde66a368',
            LPAD(TRIM(TO_CHAR(p.start_index, 'FMXXXXXXXX')), 64, '0')
        ) AS calldata
    FROM
        new_epochs e
    CROSS JOIN (
        SELECT 0 AS start_index UNION ALL
        SELECT 100 UNION ALL
        SELECT 200 
    ) p
),

base AS (
    SELECT
        epoch,
        epoch_start_block,
        start_index,
        live.udf_api(
            'POST',
            '{URL}',
            OBJECT_CONSTRUCT(
                'Content-Type', 'application/json',
                'fsc-quantum-state', 'livequery'
            ),
            OBJECT_CONSTRUCT(
                'method', 'eth_call',
                'jsonrpc', '2.0',
                'params', [
                    OBJECT_CONSTRUCT(
                        'to', '0x0000000000000000000000000000000000001000',
                        'data', calldata
                    ),
                    block_hex
                ],
                'id', CONCAT('getSnapshotValidatorSet-', epoch::STRING, '-', start_index::STRING)
            ),
            'Vault/prod/evm/quicknode/monad/mainnet'
        ) AS response
    FROM
        pages
),

parsed AS (
    SELECT
        epoch,
        epoch_start_block,
        start_index,
        response:data:result::STRING AS validator_data_hex,
        REGEXP_SUBSTR_ALL(SUBSTR(validator_data_hex, 3, LEN(validator_data_hex)), '.{64}') AS segmented_data,
        TRY_TO_BOOLEAN(utils.udf_hex_to_int(segmented_data[0]::STRING)) AS is_done,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data[1]::STRING)) AS next_index,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data[2]::STRING)) AS array_offset,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data[3]::STRING)) AS array_length
    FROM
        base
    WHERE
        validator_data_hex IS NOT NULL
),

flattened AS (
    SELECT
        p.epoch,
        p.epoch_start_block,
        p.start_index,
        p.is_done,
        f.index AS array_index,
        p.start_index + f.index AS validator_position,
        TRY_TO_NUMBER(utils.udf_hex_to_int(f.value::STRING)) AS validator_id
    FROM
        parsed p,
        LATERAL FLATTEN(input => ARRAY_SLICE(p.segmented_data, 4, 4 + p.array_length)) f
    WHERE
        p.array_length > 0
)

SELECT
    epoch,
    epoch_start_block,
    validator_id,
    validator_position,
    {{ dbt_utils.generate_surrogate_key(['epoch', 'validator_id']) }} AS snapshot_validator_set_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    flattened
WHERE
    validator_id IS NOT NULL
