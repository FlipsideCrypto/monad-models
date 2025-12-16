{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "fact_staking_validator_snapshots_id",
    cluster_by = ['snapshot_date'],
    tags = ['gov', 'curated_daily']
) }}

/*
Daily validator snapshots from getValidator() LiveQuery calls.
Adds USD valuations to the parsed silver layer data.

IMPORTANT: unclaimed_rewards here is the DELEGATOR REWARD POOL - the total
undistributed rewards for ALL delegators to claim from. This is NOT the
validator's personal earnings. For validator earnings, use
ez_staking_validator_earnings (which sources from getDelegator snapshots).
*/

WITH prices AS (
    SELECT
        hour::DATE AS price_date,
        price AS mon_price_usd
    FROM
        {{ ref('price__ez_prices_hourly') }}
    WHERE
        token_address = '0x0000000000000000000000000000000000000000'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY hour::DATE ORDER BY hour DESC) = 1
),

snapshots AS (
    SELECT
        snapshot_date,
        validator_id,
        snapshot_block,
        auth_address,
        flags,
        execution_stake_raw,
        execution_stake,
        accumulated_rewards_per_token_raw,
        execution_commission_bps,
        execution_commission_pct,
        unclaimed_rewards_raw,
        unclaimed_rewards,
        consensus_stake_raw,
        consensus_stake,
        consensus_commission_bps,
        consensus_commission_pct,
        snapshot_stake_raw,
        snapshot_stake,
        snapshot_commission_bps,
        snapshot_commission_pct,
        secp_pubkey,
        bls_pubkey
    FROM
        {{ ref('silver__staking_snapshot_get_validator') }}
{% if is_incremental() %}
    WHERE
        modified_timestamp > (SELECT MAX(modified_timestamp) FROM {{ this }})
{% endif %}
)

SELECT
    s.snapshot_date,
    s.validator_id,
    s.snapshot_block,
    s.auth_address,
    s.flags,
    s.execution_stake_raw,
    s.execution_stake,
    ROUND(s.execution_stake * p.mon_price_usd, 2) AS execution_stake_usd,
    s.accumulated_rewards_per_token_raw,
    s.execution_commission_bps,
    s.execution_commission_pct,
    s.unclaimed_rewards_raw,
    s.unclaimed_rewards,
    ROUND(s.unclaimed_rewards * p.mon_price_usd, 2) AS unclaimed_rewards_usd,
    s.consensus_stake_raw,
    s.consensus_stake,
    ROUND(s.consensus_stake * p.mon_price_usd, 2) AS consensus_stake_usd,
    s.consensus_commission_bps,
    s.consensus_commission_pct,
    s.snapshot_stake_raw,
    s.snapshot_stake,
    ROUND(s.snapshot_stake * p.mon_price_usd, 2) AS snapshot_stake_usd,
    s.snapshot_commission_bps,
    s.snapshot_commission_pct,
    s.secp_pubkey,
    s.bls_pubkey,
    p.mon_price_usd,
    {{ dbt_utils.generate_surrogate_key(['s.snapshot_date', 's.validator_id']) }} AS fact_staking_validator_snapshots_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    snapshots s
LEFT JOIN
    prices p
    ON s.snapshot_date = p.price_date
