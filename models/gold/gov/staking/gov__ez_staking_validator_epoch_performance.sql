{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "ez_staking_validator_epoch_performance_id",
    cluster_by = ['epoch'],
    tags = ['gov', 'curated_daily']
) }}

/*
Validator performance per epoch comparing expected vs actual block production.
Shows all validators in the snapshot set with their actual blocks produced,
rank within epoch, and percentage of total blocks mined.

Only includes epochs that started after the validator was created.
*/

WITH validator_creation AS (
    SELECT
        validator_id,
        auth_address,
        block_timestamp AS created_at
    FROM
        {{ ref('gov__fact_staking_validators_created') }}
),

snapshot_validators AS (
    SELECT
        s.epoch,
        s.validator_id,
        s.validator_position,
        vc.auth_address AS validator_address
    FROM
        {{ ref('silver__staking_snapshot_validator_set') }} s
    INNER JOIN
        validator_creation vc
        ON s.validator_id = vc.validator_id
    INNER JOIN
        {{ ref('gov__dim_staking_epochs') }} e
        ON s.epoch = e.epoch
    WHERE
        e.epoch_start_timestamp >= vc.created_at
{% if is_incremental() %}
        AND s.epoch > (SELECT MAX(epoch) - 5 FROM {{ this }})
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
        validator_id,
        blocks_produced,
        total_block_rewards,
        avg_reward_per_block
    FROM
        {{ ref('gov__ez_staking_block_production') }}
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
        COALESCE(bp.total_block_rewards, 0) AS total_block_rewards,
        COALESCE(bp.avg_reward_per_block, 0) AS avg_reward_per_block,
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
        AND sv.validator_id = bp.validator_id
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
    vp.epoch,
    vp.validator_id,
    v.validator_name,
    v.consensus_address,
    vp.validator_position,
    vp.validator_address,
    vp.validator_count AS validators_in_snapshot,
    vp.epoch_block_count AS total_epoch_blocks,
    ROUND(vp.epoch_block_count / NULLIF(vp.validator_count, 0), 2) AS expected_blocks_per_validator,
    vp.blocks_produced AS actual_blocks_produced,
    vp.blocks_produced - ROUND(vp.epoch_block_count / NULLIF(vp.validator_count, 0), 0) AS blocks_vs_expected,
    ROUND(100.0 * vp.blocks_produced / NULLIF(vp.epoch_block_count, 0), 4) AS pct_of_epoch_blocks,
    RANK() OVER (PARTITION BY vp.epoch ORDER BY vp.blocks_produced DESC) AS production_rank,
    vp.total_block_rewards,
    vp.avg_reward_per_block,
    vp.epoch_start_timestamp,
    vp.epoch_end_timestamp,
    {{ dbt_utils.generate_surrogate_key(['vp.epoch', 'vp.validator_id']) }} AS ez_staking_validator_epoch_performance_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    validator_performance vp
LEFT JOIN
    {{ ref('gov__dim_staking_validators') }} v
    ON vp.validator_id = v.validator_id
