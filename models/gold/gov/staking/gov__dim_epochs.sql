{{ config (
    materialized = 'view',
    tags = ['gov', 'curated_daily']
) }}

/*
Dimension table for staking epochs.
Each epoch starts when the EpochChanged event fires.
Block ranges are derived from consecutive epoch change events.
Note: Epochs are variable length (~5.5 hours) based on consensus rounds, not fixed block counts.
*/

WITH epoch_changes AS (
    SELECT
        new_epoch AS epoch,
        block_number AS epoch_start_block,
        block_timestamp AS epoch_start_timestamp,
        tx_hash,
        ROW_NUMBER() OVER (ORDER BY new_epoch) AS rn
    FROM
        {{ ref('gov__fact_epoch_changes') }}
),

with_end_blocks AS (
    SELECT
        e.epoch,
        e.epoch_start_block,
        e.epoch_start_timestamp,
        COALESCE(
            LEAD(e.epoch_start_block) OVER (ORDER BY e.epoch) - 1,
            (SELECT MAX(block_number) FROM {{ ref('core__fact_blocks') }})
        ) AS epoch_end_block,
        LEAD(e.epoch_start_timestamp) OVER (ORDER BY e.epoch) AS epoch_end_timestamp,
        e.tx_hash AS epoch_change_tx_hash
    FROM
        epoch_changes e
)

SELECT
    epoch,
    epoch_start_block,
    epoch_end_block,
    epoch_end_block - epoch_start_block + 1 AS epoch_block_count,
    epoch_start_timestamp,
    epoch_end_timestamp,
    DATEDIFF('second', epoch_start_timestamp, epoch_end_timestamp) AS epoch_duration_seconds,
    epoch_change_tx_hash
FROM
    with_end_blocks
