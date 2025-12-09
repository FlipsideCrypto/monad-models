{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "staking_snapshot_get_validator_id",
    cluster_by = ['snapshot_date'],
    tags = ['silver', 'gov', 'staking', 'livequery']
) }}

/*
Daily snapshot of validator state via LiveQuery.
Calls getValidator(uint256 validatorId) at the last block of each day.
Limited to last 16 days, one day per run to control API usage.

Function signature: 0x2b6d639a (getValidator)
Returns: Validator struct with state info
*/

WITH validators AS (
    SELECT DISTINCT
        validator_id
    FROM
        {{ ref('gov__fact_staking_validators_created') }}
),

-- Get last block for one day (oldest missing day in last 16 days)
last_blocks AS (
    SELECT
        block_timestamp::DATE AS snapshot_date,
        MAX(block_number) AS last_block_number
    FROM
        {{ ref('core__fact_blocks') }}
    WHERE
        block_timestamp::DATE >= DATEADD('day', -16, CURRENT_DATE)
        AND block_timestamp::DATE < CURRENT_DATE
{% if is_incremental() %}
        AND block_timestamp::DATE NOT IN (SELECT DISTINCT snapshot_date FROM {{ this }})
{% endif %}
    GROUP BY 1
    ORDER BY 1
    LIMIT 1
),

-- Cross join validators with dates
validator_dates AS (
    SELECT
        v.validator_id,
        lb.snapshot_date,
        lb.last_block_number,
        utils.udf_int_to_hex(lb.last_block_number) AS block_hex,
        CONCAT(
            '0x2b6d639a',
            LPAD(LTRIM(utils.udf_int_to_hex(v.validator_id), '0x'), 64, '0')
        ) AS calldata
    FROM
        validators v
    CROSS JOIN
        last_blocks lb
)

SELECT
    snapshot_date,
    validator_id,
    last_block_number AS snapshot_block,
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
            'id', CONCAT('getValidator-', validator_id::STRING, '-', snapshot_date::STRING)
        ),
        'Vault/prod/evm/quicknode/monad/mainnet'
    ) AS response,
    {{ dbt_utils.generate_surrogate_key(['snapshot_date', 'validator_id']) }} AS staking_snapshot_get_validator_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    validator_dates
