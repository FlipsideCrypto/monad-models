{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "snapshot_get_delegator_id",
    cluster_by = ['snapshot_date'],
    tags = ['gov', 'curated_daily']
) }}

/*
Daily snapshot of validator self-delegation state via LiveQuery.
Calls getDelegator(uint64 validatorId, address delegator) at the last block of each day,
using the validator's auth_address as the delegator.

This captures the validator's own delegation to themselves (self-stake) and their
unclaimed delegator rewards, which may differ from the validator-level unclaimed_rewards
(which is commission from delegators).

Function signature: 0x573c1ce0 (getDelegator)
Parameters: validatorId (uint64), delegator (address)
Returns:
  - stake (uint256): Current active stake
  - accRewardPerToken (uint256): Last checked accumulator
  - unclaimedRewards (uint256): Unclaimed delegator rewards
  - deltaStake (uint256): Stake to be activated next epoch
  - nextDeltaStake (uint256): Stake to be activated in 2 epochs
  - deltaEpoch (uint64): Epoch when deltaStake becomes active
  - nextDeltaEpoch (uint64): Epoch when nextDeltaStake becomes active

Limited to last 16 days, one day per run to control API usage.
*/

WITH validators AS (
    SELECT DISTINCT
        validator_id,
        auth_address
    FROM
        {{ ref('gov__fact_validators_created') }}
),

-- Get last block for one day (oldest missing day in last 16 days)
last_blocks AS (
    SELECT
        block_date AS snapshot_date,
        MAX(block_number) AS last_block_number
    FROM
       {{ ref('_max_block_by_date') }}
    WHERE
        block_date >= DATEADD('day', -7, CURRENT_DATE)
        AND block_date < CURRENT_DATE
{% if is_incremental() %}
        AND block_date NOT IN (SELECT DISTINCT snapshot_date FROM {{ this }})
{% endif %}
    GROUP BY 1
    ORDER BY 1
    LIMIT 3
),

-- Cross join validators with dates and build calldata
validator_dates AS (
    SELECT
        v.validator_id,
        v.auth_address,
        lb.snapshot_date,
        lb.last_block_number,
        utils.udf_int_to_hex(lb.last_block_number) AS block_hex,
        -- getDelegator(uint64 validatorId, address delegator)
        -- Selector: 0x573c1ce0
        -- validatorId: uint64 padded to 32 bytes
        -- delegator: address padded to 32 bytes
        CONCAT(
            '0x573c1ce0',
            LPAD(LTRIM(utils.udf_int_to_hex(v.validator_id), '0x'), 64, '0'),
            LPAD(LTRIM(v.auth_address, '0x'), 64, '0')
        ) AS calldata
    FROM
        validators v
    CROSS JOIN
        last_blocks lb
)

SELECT
    snapshot_date,
    validator_id,
    auth_address,
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
            'id', CONCAT('getDelegator-', validator_id::STRING, '-', snapshot_date::STRING)
        ),
        'Vault/prod/evm/quicknode/monad/mainnet'
    ) AS response,
    {{ dbt_utils.generate_surrogate_key(['snapshot_date', 'validator_id']) }} AS snapshot_get_delegator_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    validator_dates
