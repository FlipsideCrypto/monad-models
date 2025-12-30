{{ config (
    materialized = "incremental",
    incremental_strategy = 'merge',
    unique_key = ['validator_id', 'miner', 'start_block'],
    merge_update_columns = ['end_block', 'end_timestamp', 'blocks_produced', 'duration_seconds', 'is_current', 'next_miner', 'modified_timestamp'],
    cluster_by = ['validator_id'],
    tags = ['gov', 'curated_daily']
) }}

/*
Slowly Changing Dimension (Type 2) tracking miner address to validator ID relationships.
Each row represents a period where a specific miner was the block producer for a validator.
Ranges are defined by start/end blocks showing when each miner-validator association was active.

A new range is created when a validator switches to a different miner address.
This tracks the 1:1 relationship between validators and their block producer address over time.

The miner address is the block producer address from fact_blocks.
The validator_id comes from the ValidatorRewarded event for that block.

Incremental strategy:
- Only process new blocks since the last run (no full table scans)
- Extend current ranges if miner unchanged, create new ranges if miner changed
- Use merge on (validator_id, miner, start_block) to update/insert ranges
*/

{% if is_incremental() %}

-- Get current ranges for all validators (the most recent range per validator)
WITH current_ranges AS (
    SELECT
        validator_id,
        miner AS current_miner,
        start_block AS current_start_block,
        end_block AS current_end_block,
        start_timestamp AS current_start_timestamp,
        blocks_produced AS current_blocks_produced
    FROM {{ this }}
    WHERE is_current = TRUE
),

-- Get only new blocks since the global max end_block (efficient filter)
new_blocks AS (
    SELECT
        r.block_number,
        r.block_timestamp,
        r.validator_id,
        b.miner
    FROM {{ ref('gov__fact_validator_rewards') }} r
    INNER JOIN {{ ref('core__fact_blocks') }} b
        ON r.block_number = b.block_number
    WHERE r.origin_to_address = '0x0000000000000000000000000000000000001000'
        AND origin_function_signature = '0x791bdcf3'  -- syscallReward  
        AND r.block_number > (SELECT COALESCE(MAX(end_block), 0) FROM {{ this }})
),

-- Detect miner changes in new blocks by comparing to current miner
new_blocks_with_changes AS (
    SELECT
        nb.block_number,
        nb.block_timestamp,
        nb.validator_id,
        nb.miner,
        cr.current_miner,
        cr.current_start_block,
        cr.current_start_timestamp,
        cr.current_blocks_produced,
        -- Flag when miner changes from current or from previous new block
        CASE
            WHEN cr.current_miner IS NULL THEN 1  -- New validator
            WHEN LAG(nb.miner, 1, cr.current_miner) OVER (PARTITION BY nb.validator_id ORDER BY nb.block_number) != nb.miner THEN 1
            ELSE 0
        END AS is_change
    FROM new_blocks nb
    LEFT JOIN current_ranges cr ON nb.validator_id = cr.validator_id
),

-- Assign group IDs for new ranges
new_grouped AS (
    SELECT
        block_number,
        block_timestamp,
        validator_id,
        miner,
        current_miner,
        current_start_block,
        current_start_timestamp,
        current_blocks_produced,
        SUM(is_change) OVER (PARTITION BY validator_id ORDER BY block_number) AS new_group_id
    FROM new_blocks_with_changes
),

-- Aggregate new blocks into ranges
new_ranges AS (
    SELECT
        validator_id,
        miner,
        -- If first group and miner matches current, extend from current_start_block
        CASE
            WHEN new_group_id = 0 AND miner = current_miner THEN current_start_block
            ELSE MIN(block_number)
        END AS start_block,
        MAX(block_number) AS end_block,
        CASE
            WHEN new_group_id = 0 AND miner = current_miner THEN current_start_timestamp
            ELSE MIN(block_timestamp)
        END AS start_timestamp,
        MAX(block_timestamp) AS end_timestamp,
        CASE
            WHEN new_group_id = 0 AND miner = current_miner THEN current_blocks_produced + COUNT(*)
            ELSE COUNT(*)
        END AS blocks_produced,
        new_group_id
    FROM new_grouped
    GROUP BY validator_id, miner, current_miner, current_start_block, current_start_timestamp, current_blocks_produced, new_group_id
),

-- Add next miner reference and is_current flag
with_next AS (
    SELECT
        validator_id,
        miner,
        start_block,
        end_block,
        start_timestamp,
        end_timestamp,
        blocks_produced,
        LEAD(miner) OVER (PARTITION BY validator_id ORDER BY start_block) AS next_miner,
        ROW_NUMBER() OVER (PARTITION BY validator_id ORDER BY start_block DESC) = 1 AS is_current
    FROM new_ranges
)

{% else %}

-- Full refresh: compute all ranges from scratch
WITH base AS (
    SELECT
        r.block_number,
        r.block_timestamp,
        r.validator_id,
        b.miner
    FROM {{ ref('gov__fact_validator_rewards') }} r
    INNER JOIN {{ ref('core__fact_blocks') }} b
        ON r.block_number = b.block_number
    WHERE r.origin_to_address = '0x0000000000000000000000000000000000001000'
),

-- Detect when miner changes for each validator
with_changes AS (
    SELECT
        block_number,
        block_timestamp,
        validator_id,
        miner,
        CASE
            WHEN LAG(miner) OVER (PARTITION BY validator_id ORDER BY block_number) != miner
                OR LAG(miner) OVER (PARTITION BY validator_id ORDER BY block_number) IS NULL
            THEN 1
            ELSE 0
        END AS is_change
    FROM base
),

-- Assign group IDs to consecutive periods per validator
grouped AS (
    SELECT
        block_number,
        block_timestamp,
        validator_id,
        miner,
        SUM(is_change) OVER (PARTITION BY validator_id ORDER BY block_number) AS group_id
    FROM with_changes
),

-- Aggregate into ranges per validator
ranges AS (
    SELECT
        validator_id,
        miner,
        MIN(block_number) AS start_block,
        MAX(block_number) AS end_block,
        MIN(block_timestamp) AS start_timestamp,
        MAX(block_timestamp) AS end_timestamp,
        COUNT(*) AS blocks_produced
    FROM grouped
    GROUP BY validator_id, miner, group_id
),

-- Add next miner for reference and determine if range is current
with_next AS (
    SELECT
        validator_id,
        miner,
        start_block,
        end_block,
        start_timestamp,
        end_timestamp,
        blocks_produced,
        LEAD(miner) OVER (PARTITION BY validator_id ORDER BY start_block) AS next_miner,
        ROW_NUMBER() OVER (PARTITION BY validator_id ORDER BY start_block DESC) = 1 AS is_current
    FROM ranges
)

{% endif %}

SELECT
    validator_id,
    miner,
    start_block,
    end_block,
    start_timestamp,
    end_timestamp,
    blocks_produced,
    DATEDIFF('second', start_timestamp, end_timestamp) AS duration_seconds,
    is_current,
    next_miner,
    {{ dbt_utils.generate_surrogate_key(['validator_id', 'miner', 'start_block']) }} AS dim_miner_validators_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM with_next
ORDER BY validator_id, start_block
