{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "snapshot_get_validator_id",
    cluster_by = ['snapshot_date'],
    tags = ['gov', 'curated_daily']
) }}

/*
Daily snapshot of validator state via LiveQuery.
Calls getValidator(uint256 validatorId) at the last block of each day.
Limited to last 16 days, one day per run to control API usage.

Function signature: 0x2b6d639a (getValidator)
Returns: Validator struct with state info parsed into columns

getValidator returns:
  - authAddress, flags, stake (execution), accRewardPerToken, commission (execution)
  - unclaimedRewards (DELEGATOR POOL - increased by syscallReward, decreased by claims)
  - consensusStake, consensusCommission (active for current epoch)
  - snapshotStake, snapshotCommission (previous epoch, used during delay rounds)
  - secpPubkey, blsPubkey (dynamic bytes - parsed from offsets)

ABI encoding for dynamic bytes:
  - Slots 0-9: static fields (address, uint64, uint256s)
  - Slots 10-11: offsets pointing to dynamic data
  - After offsets: length + data for each dynamic bytes field
*/

WITH validators AS (
    SELECT DISTINCT
        validator_id
    FROM
        {{ ref('gov__fact_validators_created') }}
),

-- Get last block for each day, limited to recent dates not already in table
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
    LIMIT 3  -- Process one day at a time to control API usage
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
),

-- Make API calls
raw_responses AS (
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
        ) AS response
    FROM
        validator_dates
),

-- Parse the response
parsed AS (
    SELECT
        snapshot_date,
        validator_id,
        snapshot_block,
        response,
        response:data:result::STRING AS validator_data_hex,
        -- Remove 0x prefix for easier parsing
        SUBSTR(validator_data_hex, 3) AS hex_data,
        REGEXP_SUBSTR_ALL(SUBSTR(validator_data_hex, 3, LEN(validator_data_hex)), '.{64}') AS segmented_data,
        -- Parse static fields (slots 0-9)
        CONCAT('0x', SUBSTR(segmented_data[0]::STRING, 25, 40)) AS auth_address,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data[1]::STRING)) AS flags,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data[2]::STRING)) AS execution_stake_raw,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data[3]::STRING)) AS accumulated_rewards_per_token_raw,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data[4]::STRING)) AS execution_commission_bps,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data[5]::STRING)) AS unclaimed_rewards_raw,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data[6]::STRING)) AS consensus_stake_raw,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data[7]::STRING)) AS consensus_commission_bps,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data[8]::STRING)) AS snapshot_stake_raw,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data[9]::STRING)) AS snapshot_commission_bps,
        -- Slots 10-11 are offsets to dynamic data (in bytes from start of data)
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data[10]::STRING)) AS secp_offset,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data[11]::STRING)) AS bls_offset
    FROM
        raw_responses
    WHERE
        validator_data_hex IS NOT NULL
        AND validator_data_hex != '0x'
),

-- Parse dynamic bytes fields using offsets
parsed_with_keys AS (
    SELECT
        p.*,
        -- secpPubkey: offset points to length slot, data follows
        -- Offset is in bytes, multiply by 2 for hex chars, add 1 for 1-based substr
        TRY_TO_NUMBER(utils.udf_hex_to_int(SUBSTR(p.hex_data, p.secp_offset * 2 + 1, 64))) AS secp_length,
        -- blsPubkey: same pattern
        TRY_TO_NUMBER(utils.udf_hex_to_int(SUBSTR(p.hex_data, p.bls_offset * 2 + 1, 64))) AS bls_length
    FROM parsed p
),

parsed_final AS (
    SELECT
        p.*,
        -- Extract secpPubkey bytes (after length slot)
        CASE
            WHEN p.secp_length > 0
            THEN CONCAT('0x', SUBSTR(p.hex_data, p.secp_offset * 2 + 65, p.secp_length * 2))
            ELSE NULL
        END AS secp_pubkey,
        -- Extract blsPubkey bytes (after length slot)
        CASE
            WHEN p.bls_length > 0
            THEN CONCAT('0x', SUBSTR(p.hex_data, p.bls_offset * 2 + 65, p.bls_length * 2))
            ELSE NULL
        END AS bls_pubkey
    FROM parsed_with_keys p
)

SELECT
    snapshot_date,
    validator_id,
    snapshot_block,
    auth_address,
    flags,
    execution_stake_raw,
    execution_stake_raw / POW(10, 18) AS execution_stake,
    accumulated_rewards_per_token_raw,
    execution_commission_bps,
    execution_commission_bps / POW(10, 16) AS execution_commission_pct,
    unclaimed_rewards_raw,
    unclaimed_rewards_raw / POW(10, 18) AS unclaimed_rewards,
    consensus_stake_raw,
    consensus_stake_raw / POW(10, 18) AS consensus_stake,
    consensus_commission_bps,
    consensus_commission_bps / POW(10, 16) AS consensus_commission_pct,
    snapshot_stake_raw,
    snapshot_stake_raw / POW(10, 18) AS snapshot_stake,
    snapshot_commission_bps,
    snapshot_commission_bps / POW(10, 16) AS snapshot_commission_pct,
    secp_pubkey,
    bls_pubkey,
    response AS raw_response,
    {{ dbt_utils.generate_surrogate_key(['snapshot_date', 'validator_id']) }} AS snapshot_get_validator_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    parsed_final
