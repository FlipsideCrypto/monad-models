{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "fact_staking_block_production_id",
    cluster_by = ['epoch'],
    tags = ['gold', 'gov', 'staking']
) }}

/*
Block production metrics per validator per epoch.
Tracks how many blocks each validator produced and gas metrics.
*/

WITH epochs_to_process AS (
    SELECT
        epoch,
        epoch_start_block,
        epoch_end_block
    FROM
        {{ ref('gov__dim_staking_epochs') }}
{% if is_incremental() %}
    WHERE
        epoch >= (SELECT MAX(epoch) FROM {{ this }})
{% endif %}
),

blocks AS (
    SELECT
        b.block_number,
        b.block_timestamp,
        b.miner AS validator_address,
        b.gas_used,
        b.tx_count,
        e.epoch
    FROM
        {{ ref('core__fact_blocks') }} b
    INNER JOIN
        epochs_to_process e
        ON b.block_number BETWEEN e.epoch_start_block AND e.epoch_end_block
)

SELECT
    epoch,
    validator_address,
    COUNT(*) AS blocks_produced,
    SUM(gas_used) AS total_gas_used,
    AVG(gas_used) AS avg_gas_per_block,
    SUM(tx_count) AS total_transactions,
    AVG(tx_count) AS avg_tx_per_block,
    MIN(block_number) AS first_block_produced,
    MAX(block_number) AS last_block_produced,
    MIN(block_timestamp) AS first_block_timestamp,
    MAX(block_timestamp) AS last_block_timestamp,
    {{ dbt_utils.generate_surrogate_key(['epoch', 'validator_address']) }} AS fact_staking_block_production_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    blocks
GROUP BY
    epoch,
    validator_address
