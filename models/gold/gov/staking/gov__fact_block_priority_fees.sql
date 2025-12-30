{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "fact_block_priority_fees_id",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['gov', 'curated_daily']
) }}

/*
Priority fees paid to block producers (miners/validators) per block.

Priority fee calculation (for all transaction types):
  priority_fee = (effective_gas_price - base_fee_per_gas) * gas_used

This works because effective_gas_price already reflects the actual price paid,
accounting for EIP-1559 caps where: price_per_gas = min(base_fee + priority_fee, max_fee)

Validator ID and epoch are joined from gov__fact_validator_rewards where
origin_to_address = staking precompile (block producer rewards).

All fee values are decimal adjusted to MON (native token).
*/

WITH block_txs AS (
    SELECT
        t.block_number,
        t.block_timestamp,
        b.miner,
        b.base_fee_per_gas,
        t.tx_hash,
        t.gas_used,
        t.effective_gas_price,
        -- Priority fee per gas (effective_gas_price is already in Gwei, base_fee is in Wei)
        t.effective_gas_price - (b.base_fee_per_gas / POW(10, 9)) AS priority_fee_per_gas,
        -- Total priority fee for this transaction
        (t.effective_gas_price - (b.base_fee_per_gas / POW(10, 9))) * t.gas_used AS priority_fee
    FROM
        {{ ref('core__fact_transactions') }} t
    INNER JOIN
        {{ ref('core__fact_blocks') }} b
        ON t.block_number = b.block_number
    {% if is_incremental() %}
    WHERE
        t.modified_timestamp > (
            SELECT COALESCE(MAX(modified_timestamp), '1970-01-01'::TIMESTAMP)
            FROM {{ this }}
        )
    {% endif %}
),

block_aggregates AS (
    SELECT
        block_number,
        block_timestamp,
        miner,
        base_fee_per_gas,
        COUNT(*) AS tx_count,
        SUM(CASE WHEN priority_fee > 0 THEN 1 ELSE 0 END) AS tx_count_with_priority_fee,
        SUM(gas_used) AS total_gas_used,
        SUM(priority_fee) AS total_priority_fee_raw,
        AVG(priority_fee_per_gas) AS avg_priority_fee_per_gas_raw,
        MIN(priority_fee_per_gas) AS min_priority_fee_per_gas_raw,
        MAX(priority_fee_per_gas) AS max_priority_fee_per_gas_raw
    FROM
        block_txs
    GROUP BY
        block_number,
        block_timestamp,
        miner,
        base_fee_per_gas
),

-- Get validator_id and epoch from block producer rewards (syscallReward only)
block_producers AS (
    SELECT
        block_number,
        validator_id,
        epoch
    FROM
        {{ ref('gov__fact_validator_rewards') }}
    WHERE
        origin_to_address = '0x0000000000000000000000000000000000001000'
        AND origin_function_signature = '0x791bdcf3'  -- syscallReward
)

SELECT
    ba.block_number,
    ba.block_timestamp,
    bp.validator_id,
    bp.epoch,
    ba.miner,
    ba.base_fee_per_gas / POW(10, 9) AS base_fee_per_gas,
    ba.tx_count,
    ba.tx_count_with_priority_fee,
    ba.total_gas_used,
    ba.total_priority_fee_raw / POW(10, 9) AS total_priority_fee,
    ba.avg_priority_fee_per_gas_raw / POW(10, 9) AS avg_priority_fee_per_gas,
    ba.min_priority_fee_per_gas_raw / POW(10, 9) AS min_priority_fee_per_gas,
    ba.max_priority_fee_per_gas_raw / POW(10, 9) AS max_priority_fee_per_gas,
    {{ dbt_utils.generate_surrogate_key(['ba.block_number']) }} AS fact_block_priority_fees_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    block_aggregates ba
LEFT JOIN
    block_producers bp
    ON ba.block_number = bp.block_number
