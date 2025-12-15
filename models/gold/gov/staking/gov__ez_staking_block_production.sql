{{ config (
    materialized = "view",
    tags = ['gold', 'gov', 'staking', 'curated_daily']
) }}

/*
Block production metrics per validator per epoch.
Tracks how many blocks each validator produced along with gas and transaction metrics.

Links blocks to validators by matching the block's miner address to the validator's
consensus_address (derived from their secp256k1 public key).

Note: ValidatorRewarded events are NOT a reliable source for block production attribution.
Those events represent staking rewards distributed to many validators per block, not the
block producer.
*/

WITH blocks_with_producer AS (
    SELECT
        b.block_number,
        b.block_timestamp,
        LOWER(b.miner) AS miner_address,
        b.gas_used,
        b.tx_count,
        e.epoch,
        v.validator_id,
        v.validator_name
    FROM
        {{ ref('core__fact_blocks') }} b
    INNER JOIN
        {{ ref('gov__dim_staking_epochs') }} e
        ON b.block_number BETWEEN e.epoch_start_block AND e.epoch_end_block
    LEFT JOIN
        {{ ref('gov__dim_staking_validators') }} v
        ON LOWER(b.miner) = LOWER(v.consensus_address)
),

aggregated AS (
    SELECT
        epoch,
        validator_id,
        validator_name,
        miner_address AS consensus_address,
        COUNT(*) AS blocks_produced,
        SUM(gas_used) AS total_gas_used,
        AVG(gas_used) AS avg_gas_per_block,
        SUM(tx_count) AS total_transactions,
        AVG(tx_count) AS avg_tx_per_block,
        MIN(block_number) AS first_block_produced,
        MAX(block_number) AS last_block_produced,
        MIN(block_timestamp) AS first_block_timestamp,
        MAX(block_timestamp) AS last_block_timestamp
    FROM
        blocks_with_producer
    GROUP BY
        epoch,
        validator_id,
        validator_name,
        miner_address
)

SELECT
    epoch,
    validator_id,
    validator_name,
    consensus_address,
    blocks_produced,
    total_gas_used,
    avg_gas_per_block,
    total_transactions,
    avg_tx_per_block,
    first_block_produced,
    last_block_produced,
    first_block_timestamp,
    last_block_timestamp,
    {{ dbt_utils.generate_surrogate_key(['epoch', 'validator_id', 'consensus_address']) }} AS ez_staking_block_production_id
FROM
    aggregated
