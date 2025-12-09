{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "fact_staking_validator_epoch_performance_id",
    cluster_by = ['epoch'],
    tags = ['gold', 'gov', 'staking']
) }}

/*
Validator performance per epoch comparing expected vs actual block production.
Shows all validators in the snapshot set with their actual blocks produced,
rank within epoch, and percentage of total blocks mined.
*/

WITH snapshot_validators AS (
    SELECT
        s.epoch,
        s.validator_id,
        s.validator_position,
        v.auth_address AS validator_address
    FROM
        {{ ref('silver__staking_snapshot_validator_set') }} s
    LEFT JOIN
        {{ ref('gov__fact_staking_validators_created') }} v
        ON s.validator_id = v.validator_id
{% if is_incremental() %}
    WHERE
        s.epoch > (SELECT MAX(epoch) - 5 FROM {{ this }})
{% endif %}
),

epoch_info AS (
    SELECT
        epoch,
        epoch_start_block,
        epoch_end_block,
        epoch_block_count,
        epoch_start_timestamp,
        epoch_end_timestamp
    FROM
        {{ ref('gov__dim_staking_epochs') }}
{% if is_incremental() %}
    WHERE
        epoch > (SELECT MAX(epoch) - 5 FROM {{ this }})
{% endif %}
),

block_production AS (
    SELECT
        epoch,
        LOWER(validator_address) AS validator_address,
        blocks_produced,
        total_gas_used,
        avg_gas_per_block,
        total_transactions,
        avg_tx_per_block
    FROM
        {{ ref('gov__fact_staking_block_production') }}
{% if is_incremental() %}
    WHERE
        epoch > (SELECT MAX(epoch) - 5 FROM {{ this }})
{% endif %}
),

-- Count validators in snapshot per epoch to calculate expected blocks
validators_per_epoch AS (
    SELECT
        epoch,
        COUNT(DISTINCT validator_id) AS validator_count
    FROM
        snapshot_validators
    GROUP BY
        epoch
),

-- Total blocks per epoch
blocks_per_epoch AS (
    SELECT
        epoch,
        SUM(blocks_produced) AS total_blocks_in_epoch
    FROM
        block_production
    GROUP BY
        epoch
),

-- Join snapshot validators with block production
validator_performance AS (
    SELECT
        sv.epoch,
        sv.validator_id,
        sv.validator_position,
        sv.validator_address,
        COALESCE(bp.blocks_produced, 0) AS blocks_produced,
        COALESCE(bp.total_gas_used, 0) AS total_gas_used,
        COALESCE(bp.avg_gas_per_block, 0) AS avg_gas_per_block,
        COALESCE(bp.total_transactions, 0) AS total_transactions,
        COALESCE(bp.avg_tx_per_block, 0) AS avg_tx_per_block,
        vpe.validator_count,
        bpe.total_blocks_in_epoch,
        ei.epoch_block_count,
        ei.epoch_start_timestamp,
        ei.epoch_end_timestamp
    FROM
        snapshot_validators sv
    LEFT JOIN
        block_production bp
        ON sv.epoch = bp.epoch
        AND LOWER(sv.validator_address) = bp.validator_address
    INNER JOIN
        validators_per_epoch vpe
        ON sv.epoch = vpe.epoch
    LEFT JOIN
        blocks_per_epoch bpe
        ON sv.epoch = bpe.epoch
    LEFT JOIN
        epoch_info ei
        ON sv.epoch = ei.epoch
)

SELECT
    epoch,
    validator_id,
    validator_position,
    validator_address,
    validator_count AS validators_in_snapshot,
    epoch_block_count AS total_epoch_blocks,
    ROUND(epoch_block_count / NULLIF(validator_count, 0), 2) AS expected_blocks_per_validator,
    blocks_produced AS actual_blocks_produced,
    blocks_produced - ROUND(epoch_block_count / NULLIF(validator_count, 0), 0) AS blocks_vs_expected,
    ROUND(100.0 * blocks_produced / NULLIF(epoch_block_count, 0), 4) AS pct_of_epoch_blocks,
    RANK() OVER (PARTITION BY epoch ORDER BY blocks_produced DESC) AS production_rank,
    total_gas_used,
    avg_gas_per_block,
    total_transactions,
    avg_tx_per_block,
    epoch_start_timestamp,
    epoch_end_timestamp,
    {{ dbt_utils.generate_surrogate_key(['epoch', 'validator_id']) }} AS fact_staking_validator_epoch_performance_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    validator_performance
