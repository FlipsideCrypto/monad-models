{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "fact_staking_validator_snapshots_id",
    cluster_by = ['snapshot_date'],
    tags = ['gold', 'gov', 'staking']
) }}

/*
Parsed daily validator snapshots from getValidator() LiveQuery calls.
Includes stake amounts, commission rates, unclaimed rewards, and status flags.
Joined to end of day prices for USD valuations.
*/

WITH prices AS (
    SELECT
        hour::DATE AS price_date,
        price AS mon_price_usd
    FROM
        {{ ref('price__ez_prices_hourly') }}
    WHERE
        is_native = TRUE
        AND HOUR = DATEADD('hour', 23, hour::DATE)
),

raw_snapshots AS (
    SELECT
        snapshot_date,
        validator_id,
        snapshot_block,
        response
    FROM
        {{ ref('silver__staking_snapshot_get_validator') }}
{% if is_incremental() %}
    WHERE
        snapshot_date > (SELECT MAX(snapshot_date) FROM {{ this }})
{% endif %}
),

parsed AS (
    SELECT
        snapshot_date,
        validator_id,
        snapshot_block,
        response:data:result::STRING AS validator_data_hex,
        REGEXP_SUBSTR_ALL(SUBSTR(validator_data_hex, 3, LEN(validator_data_hex)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(segmented_data[0]::STRING, 25, 40)) AS auth_address,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data[1]::STRING)) AS flags,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data[2]::STRING)) AS execution_stake_raw,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data[3]::STRING)) AS accumulated_rewards_per_token_raw,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data[4]::STRING)) AS execution_commission_bps,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data[5]::STRING)) AS unclaimed_rewards_raw,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data[6]::STRING)) AS consensus_stake_raw,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data[7]::STRING)) AS consensus_commission_bps,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data[8]::STRING)) AS snapshot_stake_raw,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data[9]::STRING)) AS snapshot_commission_bps
    FROM
        raw_snapshots
    WHERE
        validator_data_hex IS NOT NULL
        AND validator_data_hex != '0x'
)

SELECT
    p.snapshot_date,
    p.validator_id,
    p.snapshot_block,
    p.auth_address,
    p.flags,
    p.execution_stake_raw,
    p.execution_stake_raw / POW(10, 18) AS execution_stake,
    ROUND((p.execution_stake_raw / POW(10, 18)) * pr.mon_price_usd, 2) AS execution_stake_usd,
    p.accumulated_rewards_per_token_raw,
    p.execution_commission_bps,
    p.execution_commission_bps / 100.0 AS execution_commission_pct,
    p.unclaimed_rewards_raw,
    p.unclaimed_rewards_raw / POW(10, 18) AS unclaimed_rewards,
    ROUND((p.unclaimed_rewards_raw / POW(10, 18)) * pr.mon_price_usd, 2) AS unclaimed_rewards_usd,
    p.consensus_stake_raw,
    p.consensus_stake_raw / POW(10, 18) AS consensus_stake,
    ROUND((p.consensus_stake_raw / POW(10, 18)) * pr.mon_price_usd, 2) AS consensus_stake_usd,
    p.consensus_commission_bps,
    p.consensus_commission_bps / 100.0 AS consensus_commission_pct,
    p.snapshot_stake_raw,
    p.snapshot_stake_raw / POW(10, 18) AS snapshot_stake,
    ROUND((p.snapshot_stake_raw / POW(10, 18)) * pr.mon_price_usd, 2) AS snapshot_stake_usd,
    p.snapshot_commission_bps,
    p.snapshot_commission_bps / 100.0 AS snapshot_commission_pct,
    pr.mon_price_usd,
    {{ dbt_utils.generate_surrogate_key(['p.snapshot_date', 'p.validator_id']) }} AS fact_staking_validator_snapshots_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    parsed p
LEFT JOIN
    prices pr
    ON p.snapshot_date = pr.price_date
