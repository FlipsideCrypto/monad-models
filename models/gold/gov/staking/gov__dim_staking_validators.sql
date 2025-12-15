{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "dim_staking_validators_id",
    cluster_by = ['validator_id'],
    tags = ['gold', 'gov', 'staking', 'curated_daily']
) }}

/*
Dimension table for Monad validators.
Combines validator creation data, seed metadata, latest snapshot state, and labels.

Priority for validator name/metadata:
1. Seed data (staking_validator_metadata) - primary source
2. Labels table - fallback

auth_address: Administrative address from validator creation event, used for
withdrawals and receiving commission.
*/

WITH validators_created AS (
    SELECT
        validator_id,
        auth_address,
        commission AS initial_commission_raw,
        commission / POW(10, 18) * 100 AS initial_commission_pct,
        block_number AS created_block,
        block_timestamp AS created_at,
        tx_hash AS creation_tx_hash
    FROM
        {{ ref('gov__fact_staking_validators_created') }}
),

-- Get validator metadata from seed (primary source)
validator_metadata AS (
    SELECT
        validator_id,
        name AS validator_name,
        city,
        country,
        region
    FROM
        {{ ref('silver__staking_validator_metadata') }}
),

-- Get consensus address for each validator (derived from secp_pubkey)
addresses AS (
    SELECT
        validator_id,
        consensus_address
    FROM
        {{ ref('silver__staking_validator_addresses') }}
),

-- Get latest snapshot data for each validator
latest_snapshots AS (
    SELECT
        validator_id,
        snapshot_date,
        flags,
        execution_commission_pct,
        consensus_commission_pct,
        secp_pubkey,
        bls_pubkey,
        ROW_NUMBER() OVER (PARTITION BY validator_id ORDER BY snapshot_date DESC) AS rn
    FROM
        {{ ref('gov__fact_staking_validator_snapshots') }}
),

-- Get validator name from labels as fallback (join on auth_address)
validator_labels AS (
    SELECT
        address,
        address_name AS validator_name,
        label AS project_name
    FROM
        {{ ref('core__dim_labels') }}
    WHERE
        label_type = 'operator'
        OR label_subtype = 'validator'
        OR address_name IS NOT NULL
)

SELECT
    vc.validator_id,
    COALESCE(vm.validator_name, vl.validator_name) AS validator_name,
    vm.city,
    vm.country,
    vm.region,
    vc.auth_address,
    a.consensus_address,
    vc.created_at,
    vc.created_block,
    vc.creation_tx_hash,
    vc.initial_commission_pct,
    ls.execution_commission_pct AS current_commission_pct,
    ls.consensus_commission_pct,
    ls.flags,
    CASE
        WHEN ls.flags = 0 THEN 'active'
        WHEN ls.flags = 1 THEN 'stake_too_low'
        ELSE 'unknown'
    END AS status,
    ls.secp_pubkey,
    ls.bls_pubkey,
    ls.snapshot_date AS latest_snapshot_date,
    {{ dbt_utils.generate_surrogate_key(['vc.validator_id']) }} AS dim_staking_validators_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    validators_created vc
LEFT JOIN
    validator_metadata vm
    ON vc.validator_id = vm.validator_id
LEFT JOIN
    addresses a
    ON vc.validator_id = a.validator_id
LEFT JOIN
    latest_snapshots ls
    ON vc.validator_id = ls.validator_id
    AND ls.rn = 1
LEFT JOIN
    validator_labels vl
    ON LOWER(vc.auth_address) = LOWER(vl.address)
