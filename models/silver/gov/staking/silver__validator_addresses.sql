{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "validator_addresses_id",
    cluster_by = ['validator_id'],
    tags = ['gov', 'curated_daily']
) }}

/*
Maps validator_id to consensus_address.

The consensus address is derived from the validator's secp256k1 public key
from snapshot data (silver__snapshot_get_validator).

Note: The block miner address on a block is NOT necessarily the validator who received
rewards on that block. ValidatorRewarded events are emitted to MANY validators per block
(staking rewards distribution), not just the block producer.
*/

WITH validators AS (
    SELECT
        validator_id,
        auth_address,
        block_timestamp AS created_at
    FROM
        {{ ref('gov__fact_validators_created') }}
),

-- Get consensus address from secp_pubkey (latest snapshot per validator)
latest_snapshots AS (
    SELECT
        validator_id,
        secp_pubkey,
        snapshot_date,
        ROW_NUMBER() OVER (PARTITION BY validator_id ORDER BY snapshot_date DESC) AS rn
    FROM
        {{ ref('silver__snapshot_get_validator') }}
    WHERE
        secp_pubkey IS NOT NULL
),

snapshot_derived AS (
    SELECT
        validator_id,
        secp_pubkey,
        LOWER(utils.udf_pubkey_to_address(secp_pubkey)) AS consensus_address,
        snapshot_date
    FROM
        latest_snapshots
    WHERE
        rn = 1
        AND utils.udf_pubkey_to_address(secp_pubkey) IS NOT NULL
)

SELECT
    v.validator_id,
    v.auth_address,
    sd.consensus_address,
    sd.secp_pubkey,
    sd.snapshot_date AS address_snapshot_date,
    v.created_at AS validator_created_at,
    {{ dbt_utils.generate_surrogate_key(['v.validator_id']) }} AS validator_addresses_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    validators v
LEFT JOIN
    snapshot_derived sd
    ON v.validator_id = sd.validator_id
