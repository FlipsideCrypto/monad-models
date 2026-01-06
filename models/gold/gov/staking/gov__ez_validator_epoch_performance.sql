{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "ez_validator_epoch_performance_id",
    cluster_by = ['epoch'],
    tags = ['gov', 'curated_daily']
) }}

/*
Validator performance per epoch comparing expected vs actual block production.
Shows all validators in the snapshot set with their actual blocks produced,
rank within epoch, and percentage of total blocks mined.

Expected blocks are weighted by each validator's stake proportion at epoch start.
Uses snapshot_stake from fact_validator_snapshots (the stake used by protocol for weighting).

Only includes epochs that started after the validator was created.
*/

WITH validator_creation AS (
    SELECT
        validator_id,
        auth_address,
        block_timestamp AS created_at
    FROM
        {{ ref('gov__fact_validators_created') }}
),

snapshot_validators AS (
    SELECT
        s.epoch,
        s.validator_id,
        s.validator_position,
        vc.auth_address AS validator_address,
        e.epoch_start_timestamp::DATE AS epoch_start_date
    FROM
        {{ ref('silver__snapshot_validator_set') }} s
    INNER JOIN
        validator_creation vc
        ON s.validator_id = vc.validator_id
    INNER JOIN
        {{ ref('gov__dim_epochs') }} e
        ON s.epoch = e.epoch
    WHERE
        e.epoch_start_timestamp >= vc.created_at
{% if is_incremental() %}
        AND s.epoch > (SELECT MAX(epoch) - 5 FROM {{ this }})
{% endif %}
),

-- Get stake at epoch start from validator snapshots
-- Use previous day's snapshot since snapshots are taken at end of day
-- Fall back to same day if previous day not available (e.g., first day of data)
validator_stakes AS (
    SELECT
        sv.epoch,
        sv.validator_id,
        sv.validator_position,
        sv.validator_address,
        sv.epoch_start_date,
        COALESCE(vs_prev.snapshot_stake, vs_same.snapshot_stake, 0) AS validator_stake
    FROM
        snapshot_validators sv
    LEFT JOIN
        {{ ref('gov__fact_validator_snapshots') }} vs_prev
        ON sv.validator_id = vs_prev.validator_id
        AND sv.epoch_start_date - 1 = vs_prev.snapshot_date
    LEFT JOIN
        {{ ref('gov__fact_validator_snapshots') }} vs_same
        ON sv.validator_id = vs_same.validator_id
        AND sv.epoch_start_date = vs_same.snapshot_date
),

-- Calculate total stake per epoch for weighting
epoch_total_stake AS (
    SELECT
        epoch,
        SUM(validator_stake) AS total_epoch_stake
    FROM
        validator_stakes
    GROUP BY
        epoch
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
        {{ ref('gov__dim_epochs') }}
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
        {{ ref('gov__ez_block_production') }}
{% if is_incremental() %}
    WHERE
        epoch > (SELECT MAX(epoch) - 5 FROM {{ this }})
{% endif %}
),

-- Count validators in snapshot per epoch
validators_per_epoch AS (
    SELECT
        epoch,
        COUNT(DISTINCT validator_id) AS validator_count
    FROM
        validator_stakes
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

-- Join validator stakes with block production
validator_performance AS (
    SELECT
        vs.epoch,
        vs.validator_id,
        vs.validator_position,
        vs.validator_address,
        vs.validator_stake,
        ets.total_epoch_stake,
        COALESCE(bp.blocks_produced, 0) AS blocks_produced,
        COALESCE(bp.total_block_rewards, 0) AS total_block_rewards,
        COALESCE(bp.avg_reward_per_block, 0) AS avg_reward_per_block,
        vpe.validator_count,
        bpe.total_blocks_in_epoch,
        ei.epoch_block_count,
        ei.epoch_start_timestamp,
        ei.epoch_end_timestamp
    FROM
        validator_stakes vs
    LEFT JOIN
        block_production bp
        ON vs.epoch = bp.epoch
        AND vs.validator_id = bp.validator_id
    INNER JOIN
        validators_per_epoch vpe
        ON vs.epoch = vpe.epoch
    LEFT JOIN
        blocks_per_epoch bpe
        ON vs.epoch = bpe.epoch
    LEFT JOIN
        epoch_info ei
        ON vs.epoch = ei.epoch
    LEFT JOIN
        epoch_total_stake ets
        ON vs.epoch = ets.epoch
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
    vp.validator_stake,
    vp.total_epoch_stake,
    ROUND(vp.validator_stake / NULLIF(vp.total_epoch_stake, 0), 6) AS stake_weight,
    ROUND(vp.epoch_block_count * (vp.validator_stake / NULLIF(vp.total_epoch_stake, 0)), 2) AS expected_blocks_per_validator,
    vp.blocks_produced AS actual_blocks_produced,
    vp.blocks_produced - ROUND(vp.epoch_block_count * (vp.validator_stake / NULLIF(vp.total_epoch_stake, 0)), 0) AS blocks_vs_expected,
    ROUND(100.0 * vp.blocks_produced / NULLIF(vp.epoch_block_count, 0), 4) AS pct_of_epoch_blocks,
    ROUND(100.0 * vp.validator_stake / NULLIF(vp.total_epoch_stake, 0), 4) AS expected_pct_of_epoch_blocks,
    RANK() OVER (PARTITION BY vp.epoch ORDER BY vp.blocks_produced DESC) AS production_rank,
    vp.total_block_rewards,
    vp.avg_reward_per_block,
    vp.epoch_start_timestamp,
    vp.epoch_end_timestamp,
    {{ dbt_utils.generate_surrogate_key(['vp.epoch', 'vp.validator_id']) }} AS ez_validator_epoch_performance_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    validator_performance vp
LEFT JOIN
    {{ ref('gov__dim_validators') }} v
    ON vp.validator_id = v.validator_id
