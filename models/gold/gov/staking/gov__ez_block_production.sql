{{ config (
    materialized = "view",
    tags = ['gov', 'curated_daily']
) }}

/*
Block production metrics per validator per epoch.
Tracks how many blocks each validator produced along with reward metrics.

Block producer is identified from ValidatorRewarded events where:
- origin_to_address = staking precompile (0x0000000000000000000000000000000000001000)

Each ValidatorRewarded event with origin_to_address = staking precompile represents
a block produced by that validator.
*/

WITH block_producers AS (
    SELECT
        r.block_number,
        r.block_timestamp,
        r.validator_id,
        r.epoch,
        r.amount,
        r.amount_raw
    FROM
        {{ ref('gov__fact_validator_rewards') }} r
    WHERE
        r.origin_to_address = '0x0000000000000000000000000000000000001000'
        AND r.origin_function_signature = '0x791bdcf3'  -- syscallReward  
),

aggregated AS (
    SELECT
        epoch,
        validator_id,
        COUNT(*) AS blocks_produced,
        SUM(amount) AS total_block_rewards,
        SUM(amount_raw) AS total_block_rewards_raw,
        AVG(amount) AS avg_reward_per_block,
        MIN(block_number) AS first_block_produced,
        MAX(block_number) AS last_block_produced,
        MIN(block_timestamp) AS first_block_timestamp,
        MAX(block_timestamp) AS last_block_timestamp
    FROM
        block_producers
    GROUP BY
        epoch,
        validator_id
)

SELECT
    a.epoch,
    a.validator_id,
    v.validator_name,
    v.consensus_address,
    v.auth_address,
    a.blocks_produced,
    a.total_block_rewards,
    a.total_block_rewards_raw,
    a.avg_reward_per_block,
    a.first_block_produced,
    a.last_block_produced,
    a.first_block_timestamp,
    a.last_block_timestamp,
    {{ dbt_utils.generate_surrogate_key(['a.epoch', 'a.validator_id']) }} AS ez_block_production_id
FROM
    aggregated a
LEFT JOIN
    {{ ref('gov__dim_validators') }} v
    ON a.validator_id = v.validator_id
