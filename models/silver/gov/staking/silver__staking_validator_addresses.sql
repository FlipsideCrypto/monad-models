{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "staking_validator_addresses_id",
    cluster_by = ['validator_id'],
    tags = ['gov', 'curated_daily']
) }}

/*
Maps validator_id to consensus_address.

The consensus address is derived from the validator's secp256k1 public key.
Sources (in priority order):
1. Snapshot data (silver__staking_snapshot_get_validator)
2. Seed metadata (silver__staking_validator_metadata) - fallback for new validators

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
        {{ ref('gov__fact_staking_validators_created') }}
),

-- Get consensus address from secp_pubkey (latest snapshot per validator)
latest_snapshots AS (
    SELECT
        validator_id,
        secp_pubkey,
        snapshot_date,
        ROW_NUMBER() OVER (PARTITION BY validator_id ORDER BY snapshot_date DESC) AS rn
    FROM
        {{ ref('silver__staking_snapshot_get_validator') }}
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
),

-- Fallback: get from seed metadata for validators not in snapshots
seed_derived AS (
    SELECT
        m.validator_id,
        m.pub_key AS secp_pubkey,
        LOWER(utils.udf_pubkey_to_address(m.pub_key)) AS consensus_address
    FROM
        {{ ref('silver__staking_validator_metadata') }} m
    WHERE
        m.pub_key IS NOT NULL
        AND m.pub_key != ''
        AND NOT EXISTS (
            SELECT 1 FROM snapshot_derived sd
            WHERE sd.validator_id = m.validator_id
        )
)

SELECT
    v.validator_id,
    v.auth_address,
    COALESCE(sd.consensus_address, seed.consensus_address) AS consensus_address,
    COALESCE(sd.secp_pubkey, seed.secp_pubkey) AS secp_pubkey,
    sd.snapshot_date AS address_snapshot_date,
    v.created_at AS validator_created_at,
    {{ dbt_utils.generate_surrogate_key(['v.validator_id']) }} AS staking_validator_addresses_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    validators v
LEFT JOIN
    snapshot_derived sd
    ON v.validator_id = sd.validator_id
LEFT JOIN
    seed_derived seed
    ON v.validator_id = seed.validator_id
