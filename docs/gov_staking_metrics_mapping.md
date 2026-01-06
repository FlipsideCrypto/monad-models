# Gov Staking Tables - Validator Metrics Mapping

## Overview

This document maps the Monad validator staking data infrastructure to specific analytics requirements. The tables are built on decoded events from the staking precompile contract (address `0x0000000000000000000000000000000000001000`) and supplemented with LiveQuery snapshots for point-in-time validator state.

### Key Design Decisions

- **End-of-day snapshots**: Validator state captured at last block before midnight UTC, providing execution stake, commission rates, and unclaimed rewards without complex calculation logic
- **Block production tracking**: Per-epoch performance with stake-weighted expected blocks and missed block detection
- **Priority fee tracking**: Block-level priority fees calculated from transaction data, attributed to block producer via validator_id
- **USD valuations**: Calculated at time of event/snapshot using end-of-day native token prices
- **14-16 day rolling window**: Monad state history is limited; tables build forward from current point

### Validator Address Types

Monad validators have multiple address types:

| Address Type | Source | Purpose |
|--------------|--------|---------|
| `validator_id` | On-chain (uint64) | Unique identifier assigned at validator creation |
| `auth_address` | ValidatorCreated event | Administrative address for withdrawals, receiving commission |
| `consensus_address` | Derived from secp_pubkey | Block production address (appears in `block.miner`) |
| `secp_pubkey` | getValidator() snapshot | SECP256k1 public key for block signing |
| `bls_pubkey` | getValidator() snapshot | BLS public key for consensus |

**Important**: The `miner` field in blocks corresponds to `consensus_address`, not `auth_address`. Validator labels in `core__dim_labels` use `consensus_address`.

### Data Limitations

- Monad on-chain state history limited to ~14-15 days (not problematic for ongoing operations)
- MEV rewards require separate contract integration
- LST incentives and foundation delegation tagging require external data sources
- Infrastructure/operational costs must be user-supplied inputs
- Multiple wallet roles (beneficiary, payout) beyond auth_address need off-chain mapping

---

## Gov Staking Tables

### Silver Layer

| Table | Purpose |
|-------|---------|
| `silver__staking_events` | Base table of all staking precompile events (decoded) |
| `silver__snapshot_validator_set` | Active validators per epoch via LiveQuery |
| `silver__snapshot_get_validator` | Daily getValidator() calls via LiveQuery (raw response) |
| `silver__snapshot_get_delegator` | Daily getDelegator() calls via LiveQuery (raw response) |
| `silver__validator_addresses` | Validator consensus addresses derived from secp_pubkey |

### Gold Layer - Dimension Tables

| Table | Purpose |
|-------|---------|
| `gov__dim_epochs` | Epoch dimension with block ranges & timestamps |
| `gov__dim_validators` | Validator dimension with name, addresses, commission, status |
| `gov__dim_miner_validators` | Mapping of miner (consensus) addresses to validator_id |

### Gold Layer - Fact Tables

| Table | Purpose |
|-------|---------|
| `gov__fact_validators_created` | Validator registration (ID, auth_address, commission) |
| `gov__fact_delegations` | Delegate events (validator_id, delegator, amount, activation_epoch) |
| `gov__fact_undelegations` | Undelegate events with withdraw_id |
| `gov__fact_withdrawals` | Completed withdrawals |
| `gov__fact_validator_rewards` | ValidatorRewarded events (block rewards to validator pool) |
| `gov__fact_rewards_claimed` | ClaimRewards events (delegator claims) |
| `gov__fact_validator_commission_changes` | Commission rate changes |
| `gov__fact_validator_status_changes` | Validator status flags |
| `gov__fact_epoch_changes` | EpochChanged events |
| `gov__fact_validator_snapshots` | Daily parsed validator state with USD valuations |
| `gov__fact_validator_self_delegation_snapshots` | Daily validator self-delegation state (source of earnings) |
| `gov__fact_block_priority_fees` | Priority fees paid per block to block producer |

### Gold Layer - Easy/Aggregated Tables

| Table | Purpose |
|-------|---------|
| `gov__ez_block_production` | Blocks produced per validator per epoch |
| `gov__ez_staking_balances_daily` | Daily delegator balances (active + pending) - event-based, see note* |
| `gov__ez_validator_balances_daily` | Validator wallet balances with USD |
| `gov__ez_validator_self_stake` | Self-stake percentage |
| `gov__ez_validator_epoch_performance` | Stake-weighted expected vs actual blocks, rank, % of epoch |
| `gov__ez_validator_earnings` | Daily validator earnings (commission + priority fees) |
| `gov__ez_validator_apr` | Daily and rolling APR/APY per validator |
| `gov__ez_staking_flows_daily` | Daily staking flows by type (delegation, undelegation, withdrawal, claim) |
| `gov__ez_net_stake_changes` | Daily net stake changes per validator |
| `gov__ez_miss_rate` | Miss rate and commission impact per epoch |
| `gov__ez_restart_impact` | Restart impact estimates per validator |
| `gov__ez_compound_decision` | Compound vs sell decision support data |
| `gov__ez_validator_totals_daily` | Daily validator-level balance totals with delegator counts |

*\*Note on `ez_staking_balances_daily`: This table reconstructs balances from delegation/undelegation/withdrawal events. If event history is incomplete (e.g., delegations before data collection started), balances may be inaccurate. For authoritative stake data, use `fact_validator_snapshots.snapshot_stake` which queries on-chain state directly via LiveQuery.*

---

## Priority Fee Calculation

Priority fees are additional fees paid by transaction senders above the base fee, which go directly to the block producer.

### Calculation Formula

```
priority_fee_per_gas = effective_gas_price - base_fee_per_gas
priority_fee = priority_fee_per_gas * gas_used
```

This formula works for ALL transaction types (legacy, Type 1, Type 2/EIP-1559) because `effective_gas_price` already reflects the actual price paid.

### Monad EIP-1559 Implementation

Monad uses EIP-1559 with a custom base fee controller:
- `price_per_gas = min(base_price_per_gas + priority_price_per_gas, max_price_per_gas)`
- The `effective_gas_price` field captures the final price after applying this formula

### Priority Fee Flow

1. Transaction sender pays `effective_gas_price * gas_used`
2. `base_fee * gas_used` is burned
3. `priority_fee` goes to the block producer (validator's consensus_address)

### Data Validation

Priority fees were validated by comparing calculated values against actual balance changes for validator consensus addresses. Results showed differences of < 0.000001 MON (6+ decimal precision) on days without external transfers.

---

## Validator Earnings Calculation

Validator earnings come from two sources:

### 1. Staking Rewards (Commission + Self-Stake)

Captured via `gov__ez_validator_earnings`:

```
total_staking_earned = claimed_rewards + unclaimed_change
```

Where:
- `claimed_rewards` = ClaimRewards events by the validator's auth_address on their own validator_id
- `unclaimed_change` = current_unclaimed - prev_unclaimed (from getDelegator snapshots)

**Why this formula works**: End-of-day snapshots capture state AFTER any claims that day. If a claim happened:
- `unclaimed_change` = new_rewards - claimed (negative if claimed > earned)
- Adding back `claimed_rewards` gives true earnings

### 2. Priority Fees

Priority fees go directly to the block producer's consensus_address (not through the staking contract).

```sql
-- Priority fees per day per validator
SELECT
    block_timestamp::DATE AS fee_date,
    validator_id,
    SUM(total_priority_fee) AS priority_fees
FROM gov__fact_block_priority_fees
GROUP BY 1, 2
```

### Total Validator Earnings

```sql
-- From ez_validator_earnings (includes both sources)
SELECT
    earning_date,
    validator_id,
    claimed_rewards,
    unclaimed_change,
    priority_fees,
    total_earned  -- = claimed + unclaimed_change + priority_fees
FROM gov__ez_validator_earnings
```

---

## Requirements Mapping

### Validator Identity & Wallet Mapping

#### 1. Validator ID / operator address
**Status**: ✅ Available

```sql
SELECT
    validator_id,
    validator_name,
    auth_address AS operator_address,
    consensus_address,
    created_at AS registration_date,
    initial_commission_pct,
    current_commission_pct,
    status
FROM monad.gov.dim_validators
ORDER BY validator_id;
```

---

#### 2. List of associated wallets tagged by role
**Status**: ⚠️ Partial - auth_address and consensus_address available on-chain

```sql
-- Currently available addresses
SELECT
    validator_id,
    auth_address AS authority_address,
    consensus_address AS block_producer_address
FROM monad.gov.dim_validators;
```

**Gap**: Staking/registration wallet, beneficiary/fee wallet, and payout/distribution wallets are not tracked on-chain. Requires user-supplied mapping table.

---

#### 3. Mapping of all wallets back to a single "validator entity"
**Status**: ⚠️ Partial - requires user input

```sql
-- With user-supplied wallet mapping, query would be:
SELECT
    v.validator_id,
    v.auth_address,
    v.consensus_address,
    m.wallet_address,
    m.wallet_role
FROM monad.gov.dim_validators v
LEFT JOIN validators_wallet_mapping m ON v.validator_id = m.validator_id;
```

---

### Rewards, Revenue & Pricing

#### 4. Total rewards earned per period (per day / week / month)
**Status**: ✅ Available

```sql
-- Daily earnings by validator (includes priority fees)
SELECT
    earning_date,
    validator_id,
    validator_name,
    claimed_rewards,
    unclaimed_change,
    priority_fees,
    blocks_produced,
    total_earned,
    total_earned_usd
FROM monad.gov.ez_validator_earnings
ORDER BY earning_date DESC, validator_id;

-- Weekly earnings
SELECT
    DATE_TRUNC('week', earning_date) AS earning_week,
    validator_id,
    SUM(claimed_rewards) AS claimed_rewards,
    SUM(unclaimed_change) AS unclaimed_change,
    SUM(priority_fees) AS priority_fees,
    SUM(blocks_produced) AS blocks_produced,
    SUM(total_earned) AS total_earned,
    SUM(total_earned_usd) AS total_earned_usd
FROM monad.gov.ez_validator_earnings
GROUP BY 1, 2
ORDER BY 1 DESC, 2;

-- Monthly earnings
SELECT
    DATE_TRUNC('month', earning_date) AS earning_month,
    validator_id,
    SUM(total_earned) AS total_earned,
    SUM(total_earned_usd) AS total_earned_usd
FROM monad.gov.ez_validator_earnings
GROUP BY 1, 2
ORDER BY 1 DESC, 2;
```

---

#### 5. Rewards split by source (base rewards, MEV, tips, LST incentives, foundation delegations)
**Status**: ⚠️ Partial

```sql
-- Validator earnings by source
SELECT
    earning_date,
    validator_id,
    validator_name,
    claimed_rewards + unclaimed_change AS staking_rewards,  -- commission + self-stake
    priority_fees,                                           -- block production tips
    total_earned
FROM monad.gov.ez_validator_earnings
ORDER BY earning_date DESC;

-- Priority fees by block (detailed)
SELECT
    block_number,
    block_timestamp,
    validator_id,
    tx_count,
    tx_count_with_priority_fee,
    total_priority_fee,
    avg_priority_fee_per_gas
FROM monad.gov.fact_block_priority_fees
ORDER BY block_number DESC;
```

**Gaps**:
- ❌ MEV rewards - need to integrate MEV contract events
- ✅ Priority fees / Tips - available in `fact_block_priority_fees`
- ❌ LST incentives - external data source required
- ⚠️ Foundation delegations - could tag known foundation addresses

---

#### 6. Token-denominated rewards (MON earned)
**Status**: ✅ Available

```sql
-- Total MON earned by validator
SELECT
    validator_id,
    SUM(total_earned) AS total_mon_earned,
    SUM(priority_fees) AS total_priority_fees,
    SUM(claimed_rewards + unclaimed_change) AS total_staking_rewards
FROM monad.gov.ez_validator_earnings
GROUP BY 1
ORDER BY total_mon_earned DESC;
```

---

#### 7. USD value of rewards using spot price at reward time
**Status**: ✅ Available

```sql
-- USD value of earnings by date
SELECT
    earning_date,
    validator_id,
    total_earned,
    total_earned_usd,
    priority_fees,
    priority_fees_usd,
    mon_price_usd
FROM monad.gov.ez_validator_earnings
ORDER BY earning_date DESC;
```

---

#### 8. Effective APR / APY per validator and for aggregate position
**Status**: ✅ Available via `ez_validator_apr`

```sql
-- Daily and rolling APR/APY per validator
SELECT
    validator_id,
    validator_name,
    earning_date,
    apr_pct,
    apy_pct,
    apr_7d_rolling_pct,
    apr_30d_rolling_pct
FROM monad.gov.ez_validator_apr
ORDER BY earning_date DESC, validator_id;
```

---

#### 9. Historical data of all rewards
**Status**: ✅ Available (rolling 14-16 days, building forward)

```sql
SELECT
    earning_date,
    validator_id,
    claimed_rewards,
    unclaimed_change,
    priority_fees,
    total_earned,
    total_earned_usd,
    unclaimed_balance
FROM monad.gov.ez_validator_earnings
ORDER BY earning_date DESC;
```

**Note**: Monad state history limited to ~14-15 days. Data builds forward from implementation date.

---

### Block Production Metrics

#### Block production by validator
**Status**: ✅ Available

```sql
-- Blocks produced per epoch
SELECT
    epoch,
    validator_id,
    validator_name,
    blocks_produced,
    total_block_rewards,
    avg_reward_per_block,
    first_block_timestamp,
    last_block_timestamp
FROM monad.gov.ez_block_production
ORDER BY epoch DESC, blocks_produced DESC;

-- Priority fees per block
SELECT
    block_number,
    block_timestamp,
    validator_id,
    epoch,
    miner,
    tx_count,
    tx_count_with_priority_fee,
    total_gas_used,
    total_priority_fee,
    avg_priority_fee_per_gas,
    base_fee_per_gas
FROM monad.gov.fact_block_priority_fees
ORDER BY block_number DESC;
```

#### Stake-weighted epoch performance
**Status**: ✅ Available

Expected blocks are now weighted by each validator's stake proportion at epoch start, using `snapshot_stake` from `fact_validator_snapshots`.

```sql
-- Validator performance with stake weighting
SELECT
    epoch,
    validator_id,
    validator_name,
    validator_stake,                    -- Validator's stake at epoch start
    total_epoch_stake,                  -- Sum of all validators' stake
    stake_weight,                       -- validator_stake / total_epoch_stake
    expected_blocks_per_validator,      -- total_blocks * stake_weight
    actual_blocks_produced,
    blocks_vs_expected,                 -- actual - expected (stake-weighted)
    pct_of_epoch_blocks,                -- Actual % of blocks produced
    expected_pct_of_epoch_blocks,       -- Expected % based on stake weight
    production_rank
FROM monad.gov.ez_validator_epoch_performance
ORDER BY epoch DESC, production_rank;
```

---

### Wallet-Level Flows & Balances

#### 10. Per-wallet balance over time (token + USD)
**Status**: ✅ Available for validator auth addresses

```sql
-- Validator wallet balances
SELECT
    balance_date,
    validator_id,
    validator_address,
    balance AS mon_balance,
    balance_usd
FROM monad.gov.ez_validator_balances_daily
ORDER BY validator_id, balance_date DESC;

-- Stake balances from snapshots
SELECT
    snapshot_date,
    validator_id,
    execution_stake,
    execution_stake_usd,
    consensus_stake,
    consensus_stake_usd
FROM monad.gov.fact_validator_snapshots
ORDER BY validator_id, snapshot_date DESC;
```

---

#### 11. Inflows/outflows per wallet per period
**Status**: ✅ Available via `ez_staking_flows_daily`

```sql
SELECT
    flow_date,
    validator_id,
    delegator_address,
    flow_type,
    amount,
    tx_count
FROM monad.gov.ez_staking_flows_daily
ORDER BY flow_date DESC;
```

---

#### 12. Transfers among validator-associated wallets
**Status**: ⚠️ Requires wallet mapping

```sql
-- With wallet mapping:
WITH validator_wallets AS (
    SELECT validator_id, wallet_address
    FROM validators_wallet_mapping
    WHERE validator_id = :validator_id
)
SELECT
    t.block_timestamp,
    t.from_address,
    t.to_address,
    t.amount,
    CASE
        WHEN t.to_address IN (SELECT wallet_address FROM validator_wallets)
        THEN 'internal'
        ELSE 'external'
    END AS flow_type
FROM monad.core.ez_native_transfers t
WHERE t.from_address IN (SELECT wallet_address FROM validator_wallets);
```

---

#### 13. Net position change per wallet
**Status**: ✅ Available via `ez_net_stake_changes`

```sql
SELECT
    validator_id,
    change_date,
    delegations,
    undelegations,
    net_stake_change,
    flow_direction,
    cumulative_net_change
FROM monad.gov.ez_net_stake_changes
ORDER BY change_date DESC;
```

---

### Profitability & Cost Metrics

#### 14. Cost basis for staked tokens
**Status**: ❌ Not available - requires user input

#### 15-16. Infrastructure and operational costs
**Status**: ❌ User input required

Recommend creating seed files for cost tracking.

#### 17. Gross revenue vs net profit per period
**Status**: ⚠️ Partial - revenue available, costs need user input

```sql
-- Gross revenue per month (including priority fees)
SELECT
    DATE_TRUNC('month', earning_date) AS month,
    validator_id,
    SUM(total_earned) AS gross_revenue_mon,
    SUM(total_earned_usd) AS gross_revenue_usd,
    SUM(priority_fees) AS priority_fee_revenue,
    SUM(claimed_rewards + unclaimed_change) AS staking_revenue
FROM monad.gov.ez_validator_earnings
GROUP BY 1, 2
ORDER BY 1 DESC, 2;
```

---

### Performance, Reliability & Penalties

#### 20. Uptime / availability per epoch
**Status**: ✅ Available

Expected blocks are stake-weighted based on each validator's proportion of total stake.

```sql
SELECT
    epoch,
    validator_id,
    validator_stake,
    stake_weight,
    expected_blocks_per_validator,      -- Stake-weighted expected blocks
    actual_blocks_produced,
    blocks_vs_expected,
    ROUND(actual_blocks_produced / NULLIF(expected_blocks_per_validator, 0) * 100, 2) AS performance_pct
FROM monad.gov.ez_validator_epoch_performance
ORDER BY epoch DESC, validator_id;
```

---

#### 21. Miss rate and impact on rewards
**Status**: ✅ Available via `ez_miss_rate`

```sql
SELECT
    epoch,
    validator_id,
    expected_blocks_per_validator,
    actual_blocks_produced,
    missed_blocks,
    miss_rate_pct,
    uptime_pct,
    estimated_missed_commission_mon,
    estimated_missed_commission_usd
FROM monad.gov.ez_miss_rate
ORDER BY epoch DESC, miss_rate_pct DESC;
```

---

#### 22. Slashing events
**Status**: ❌ Not yet live on Monad

---

#### 23. Restart correlation with missed slots
**Status**: ⚠️ Requires restart timestamp input

---

### Automation & Decision-Support

#### 30. Restart impact estimates
**Status**: ✅ Available via `ez_restart_impact`

```sql
SELECT
    validator_id,
    validator_name,
    avg_daily_commission_mon,
    missed_blocks_5min, loss_usd_5min,
    missed_blocks_15min, loss_usd_15min,
    missed_blocks_30min, loss_usd_30min,
    missed_blocks_1hr, loss_usd_1hr
FROM monad.gov.ez_restart_impact
ORDER BY avg_daily_commission_mon DESC;
```

---

#### 31. Compound vs sell decision data
**Status**: ✅ Available via `ez_compound_decision`

```sql
SELECT
    validator_id,
    validator_name,
    unclaimed_rewards,
    unclaimed_rewards_usd,
    current_price,
    price_change_7d_pct,
    price_change_30d_pct,
    days_since_last_claim,
    recommendation
FROM monad.gov.ez_compound_decision
ORDER BY unclaimed_rewards_usd DESC;
```

---

## Coverage Summary

| # | Requirement | Status | Table/Query |
|---|-------------|--------|-------------|
| 1 | Validator ID / operator address | ✅ | `dim_validators` |
| 2 | Associated wallets by role | ⚠️ | auth + consensus address; need user mapping for others |
| 3 | Wallet to entity mapping | ⚠️ | Need user-supplied mapping table |
| 4 | Total rewards per period | ✅ | `ez_validator_earnings` (includes priority fees) |
| 5 | Rewards by source | ✅ | Staking rewards + priority fees available |
| 6 | Token-denominated rewards | ✅ | `ez_validator_earnings` |
| 7 | USD value at reward time | ✅ | `ez_validator_earnings` (includes USD) |
| 8 | Effective APR/APY | ✅ | `ez_validator_apr` |
| 9 | Historical reward data | ✅ | Rolling 14-16 days, building forward |
| 10 | Per-wallet balance over time | ✅ | `ez_validator_balances_daily` |
| 11 | Inflows/outflows per wallet | ✅ | `ez_staking_flows_daily` |
| 12 | Internal vs external transfers | ⚠️ | Need wallet mapping |
| 13 | Net position change | ✅ | `ez_net_stake_changes` |
| 14 | Cost basis for staked tokens | ❌ | Need user input |
| 15 | Infrastructure costs | ❌ | Need user input (seed file) |
| 16 | Other costs (opex/labor) | ❌ | Need user input (seed file) |
| 17 | Gross revenue vs net profit | ⚠️ | Revenue ✅, costs need input |
| 18 | Profit margin | ⚠️ | Need cost data |
| 19 | Forward projection | ✅ | Based on recent run rate |
| 20 | Uptime per epoch | ✅ | `ez_validator_epoch_performance` |
| 21 | Miss rate + reward impact | ✅ | `ez_miss_rate` |
| 22 | Slashing events | ❌ | Not yet live on Monad |
| 23 | Restart correlation | ⚠️ | Need restart timestamps |
| 25 | Before/after event metrics | ⚠️ | Commission changes ✅; others need timestamps |
| 26 | Event window analysis | ⚠️ | Need event timestamps |
| 30 | Restart impact estimates | ✅ | `ez_restart_impact` |
| 31 | Compound vs sell data | ✅ | `ez_compound_decision` |
| NEW | Block-level priority fees | ✅ | `fact_block_priority_fees` |
| NEW | Block production metrics | ✅ | `ez_block_production` |
| NEW | Stake-weighted block expectations | ✅ | `ez_validator_epoch_performance` |

---

## Model Dependency Graph

```
silver__staking_events
    ├── gov__fact_validators_created
    │       └── gov__dim_validators
    │               ├── gov__ez_validator_earnings
    │               ├── gov__ez_block_production
    │               └── gov__ez_restart_impact
    ├── gov__fact_delegations
    │       └── gov__ez_staking_balances_daily
    │               └── gov__ez_validator_totals_daily
    ├── gov__fact_undelegations
    ├── gov__fact_withdrawals
    ├── gov__fact_validator_rewards
    │       └── gov__fact_block_priority_fees
    │               └── gov__ez_validator_earnings
    ├── gov__fact_rewards_claimed
    │       └── gov__ez_validator_earnings
    ├── gov__fact_epoch_changes
    │       └── gov__dim_epochs
    └── gov__fact_validator_status_changes

silver__snapshot_get_validator
    └── gov__fact_validator_snapshots
            ├── gov__ez_validator_apr
            └── gov__ez_validator_epoch_performance (stake weighting)

silver__snapshot_validator_set
    └── gov__ez_validator_epoch_performance (validator set per epoch)

silver__snapshot_get_delegator
    └── gov__fact_validator_self_delegation_snapshots
            └── gov__ez_validator_earnings

silver__validator_addresses
    └── gov__dim_validators

core__fact_transactions + core__fact_blocks
    └── gov__fact_block_priority_fees
```

---

## User Input Requirements

To enable full functionality, validators should provide:

| Data | Format | Purpose |
|------|--------|---------|
| Wallet mapping | `validator_id, wallet_address, wallet_role` | Items 2, 3, 12 |
| Infrastructure costs | `validator_id, month, cost_usd, category` | Items 15, 16, 17, 18 |
| Cost basis | `wallet_address, acquisition_date, amount, price` | Item 14 |
| Restart timestamps | `validator_id, restart_timestamp, reason` | Item 23 |
| Event timestamps | `validator_id, event_timestamp, event_type` | Items 25, 26 |

---

## Future Enhancements

- **Alerting**: Reward changes, performance degradation, missed block thresholds
- **MEV integration**: Track MEV rewards when contract available
- **LST tracking**: Foundation delegation and LST incentive tagging
- **Competitor analysis**: Stake changes relative to other validators
- **Slashing model**: Ready for when Monad implements slashing
- **Priority fee analytics**: Average priority fees by time of day, congestion patterns
