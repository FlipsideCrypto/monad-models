{{ config (
    materialized = "view",
    tags = ['gold', 'gov', 'staking', 'curated_daily']
) }}

/*
Daily net stake changes per validator.
Calculates delegations minus undelegations to show net flow direction.
Useful for tracking validator growth/decline trends.
*/

WITH stake_in AS (
    SELECT
        validator_id,
        block_timestamp::DATE AS change_date,
        SUM(amount) AS delegations,
        SUM(amount_raw) AS delegations_raw,
        COUNT(*) AS delegation_count
    FROM {{ ref('gov__fact_staking_delegations') }}
    GROUP BY 1, 2
),

stake_out AS (
    SELECT
        validator_id,
        block_timestamp::DATE AS change_date,
        SUM(amount) AS undelegations,
        SUM(amount_raw) AS undelegations_raw,
        COUNT(*) AS undelegation_count
    FROM {{ ref('gov__fact_staking_undelegations') }}
    GROUP BY 1, 2
),

combined AS (
    SELECT
        COALESCE(i.validator_id, o.validator_id) AS validator_id,
        COALESCE(i.change_date, o.change_date) AS change_date,
        COALESCE(i.delegations, 0) AS delegations,
        COALESCE(i.delegations_raw, 0) AS delegations_raw,
        COALESCE(i.delegation_count, 0) AS delegation_count,
        COALESCE(o.undelegations, 0) AS undelegations,
        COALESCE(o.undelegations_raw, 0) AS undelegations_raw,
        COALESCE(o.undelegation_count, 0) AS undelegation_count
    FROM stake_in i
    FULL OUTER JOIN stake_out o
        ON i.validator_id = o.validator_id
        AND i.change_date = o.change_date
)

SELECT
    c.validator_id,
    v.validator_name,
    c.change_date,
    c.delegations,
    c.delegations_raw,
    c.delegation_count,
    c.undelegations,
    c.undelegations_raw,
    c.undelegation_count,

    -- Net changes
    c.delegations - c.undelegations AS net_stake_change,
    c.delegations_raw - c.undelegations_raw AS net_stake_change_raw,

    -- Flow direction indicator
    CASE
        WHEN c.delegations > c.undelegations THEN 'net_inflow'
        WHEN c.delegations < c.undelegations THEN 'net_outflow'
        ELSE 'neutral'
    END AS flow_direction,

    -- Running total (cumulative net change)
    SUM(c.delegations - c.undelegations) OVER (
        PARTITION BY c.validator_id
        ORDER BY c.change_date
        ROWS UNBOUNDED PRECEDING
    ) AS cumulative_net_change,

    {{ dbt_utils.generate_surrogate_key(['c.validator_id', 'c.change_date']) }} AS ez_staking_net_stake_changes_id

FROM combined c
LEFT JOIN {{ ref('gov__dim_staking_validators') }} v
    ON c.validator_id = v.validator_id
