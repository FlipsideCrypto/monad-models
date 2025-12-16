{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "ez_staking_validator_balances_daily_id",
    cluster_by = ['balance_date'],
    tags = ['gov', 'curated_daily']
) }}

/*
Daily wallet balances for validator addresses.
Forward fills missing days from ez_balances_native_daily since that table
only has rows for days when the balance changed.
Joins to native price for USD valuation.
*/

WITH validators AS (
    SELECT DISTINCT
        validator_id,
        LOWER(auth_address) AS validator_address
    FROM
        {{ ref('gov__fact_staking_validators_created') }}
),

date_spine AS (
    SELECT
        date_day AS balance_date
    FROM
        {{ source('crosschain_gold', 'dim_dates') }}
    WHERE
        date_day <= CURRENT_DATE
{% if is_incremental() %}
        AND date_day >= (SELECT MAX(balance_date) - INTERVAL '7 days' FROM {{ this }})
{% endif %}
),

validator_dates AS (
    SELECT
        v.validator_id,
        v.validator_address,
        d.balance_date
    FROM
        validators v
    CROSS JOIN
        date_spine d
),

native_balances AS (
    SELECT
        block_date,
        LOWER(address) AS address,
        balance,
        balance_raw
    FROM
        {{ ref('balances__ez_balances_native_daily') }}
    WHERE
        LOWER(address) IN (SELECT validator_address FROM validators)
),

-- Join balances and forward fill missing days
balances_with_gaps AS (
    SELECT
        vd.balance_date,
        vd.validator_id,
        vd.validator_address,
        nb.balance,
        nb.balance_raw,
        nb.block_date AS last_balance_date
    FROM
        validator_dates vd
    LEFT JOIN
        native_balances nb
        ON vd.validator_address = nb.address
        AND vd.balance_date = nb.block_date
),

-- Forward fill using last non-null value
forward_filled AS (
    SELECT
        balance_date,
        validator_id,
        validator_address,
        COALESCE(
            balance,
            LAST_VALUE(balance IGNORE NULLS) OVER (
                PARTITION BY validator_id
                ORDER BY balance_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        ) AS balance,
        COALESCE(
            balance_raw,
            LAST_VALUE(balance_raw IGNORE NULLS) OVER (
                PARTITION BY validator_id
                ORDER BY balance_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        ) AS balance_raw,
        CASE WHEN last_balance_date IS NOT NULL THEN FALSE ELSE TRUE END AS is_forward_filled
    FROM
        balances_with_gaps
),

-- Get last price of each day
prices AS (
    SELECT
        hour::DATE AS price_date,
        price
    FROM
        {{ ref('price__ez_prices_hourly') }}
    WHERE
        token_address = '0x0000000000000000000000000000000000000000'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY hour::DATE ORDER BY hour DESC) = 1
)

SELECT
    ff.balance_date,
    ff.validator_id,
    v.validator_name,
    v.consensus_address,
    ff.validator_address,
    ff.balance,
    ff.balance_raw,
    p.price AS mon_price_usd,
    ROUND(ff.balance * p.price, 2) AS balance_usd,
    ff.is_forward_filled,
    {{ dbt_utils.generate_surrogate_key(['ff.balance_date', 'ff.validator_id']) }} AS ez_staking_validator_balances_daily_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    forward_filled ff
LEFT JOIN
    {{ ref('gov__dim_staking_validators') }} v
    ON ff.validator_id = v.validator_id
LEFT JOIN
    prices p
    ON ff.balance_date = p.price_date
WHERE
    ff.balance IS NOT NULL
