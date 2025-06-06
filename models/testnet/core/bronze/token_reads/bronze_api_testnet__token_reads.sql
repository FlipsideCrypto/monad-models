{%- set node_url = var('GLOBAL_NODE_URL', '{Service}/{Authentication}') -%}
{%- set node_secret_path = var('GLOBAL_NODE_SECRET_PATH', '') -%}

{{ config(
    materialized = 'incremental',
    merge_exclude_columns = ["_inserted_timestamp"],
    unique_key = ["token_reads_id"],
    tags = ['bronze_testnet', 'recent_test', 'contracts']
) }}

WITH base AS (

    SELECT
        contract_address,
        latest_event_block AS latest_block
    FROM
        {{ ref('silver_testnet__relevant_contracts') }}
    WHERE
        total_event_count >= 25

{% if is_incremental() %}
AND (
    -- New contracts we haven't tried yet
    contract_address NOT IN (
        SELECT
            contract_address
        FROM
            {{ this }}
    )
    OR
    -- Contracts that failed and we want to retry
    contract_address IN (
        SELECT
            contract_address
        FROM
            {{ this }}
        WHERE 
            read_result is null
        AND modified_timestamp <= SYSDATE() - INTERVAL '1 MONTH'
        AND retry_count < 3
    )
)
{% endif %}
ORDER BY
    total_event_count DESC
LIMIT
    200
), 
function_sigs AS (
    SELECT
        '0x313ce567' AS function_sig,
        'decimals' AS function_name
    UNION
    SELECT
        '0x06fdde03',
        'name'
    UNION
    SELECT
        '0x95d89b41',
        'symbol'
),
all_reads AS (
    SELECT
        *
    FROM
        base
        JOIN function_sigs
        ON 1 = 1
),
ready_reads AS (
    SELECT
        contract_address,
        latest_block,
        function_sig,
        RPAD(
            function_sig,
            64,
            '0'
        ) AS input,
        utils.udf_json_rpc_call(
            'eth_call',
            [{'to': contract_address, 'from': null, 'data': input}, utils.udf_int_to_hex(latest_block)],
            concat_ws(
                '-',
                contract_address,
                input,
                latest_block
            )
        ) AS rpc_request
    FROM
        all_reads
),
batch_reads AS (
    SELECT
        ARRAY_AGG(rpc_request) AS batch_rpc_request
    FROM
        ready_reads
),
node_call AS (
    SELECT
        *,
        live.udf_api(
            'POST',
            '{{ node_url }}',
            {},
            batch_rpc_request,
           '{{ node_secret_path }}'
        ) AS response
    FROM
        batch_reads
    WHERE
        EXISTS (
            SELECT
                1
            FROM
                ready_reads
            LIMIT
                1
        )
), flat_responses AS (
    SELECT
        VALUE :id :: STRING AS call_id,
        VALUE :result :: STRING AS read_result
    FROM
        node_call,
        LATERAL FLATTEN (
            input => response :data
        )
),
transformed_responses AS (
    SELECT
        SPLIT_PART(call_id, '-', 1) AS contract_address,
        SPLIT_PART(call_id, '-', 3) AS block_number,
        LEFT(SPLIT_PART(call_id, '-', 2), 10) AS function_sig,
        call_id,
        read_result
    FROM
        flat_responses
),
retry_counts AS (
    SELECT
        tr.contract_address,
        tr.function_sig,
        {% if is_incremental() %}
        COALESCE(prev.retry_count + 1, 1) as retry_count
        {% else %}
        1 as retry_count
        {% endif %}
    FROM transformed_responses tr
    {% if is_incremental() %}
    LEFT JOIN {{ this }} prev
        ON prev.contract_address = tr.contract_address
        AND prev.function_sig = tr.function_sig
    {% endif %}
)
SELECT
    tr.contract_address,
    tr.block_number,
    tr.function_sig,
    NULL AS function_input,
    tr.read_result,
    SYSDATE() :: TIMESTAMP AS _inserted_timestamp,
    SYSDATE() :: TIMESTAMP AS modified_timestamp,
    rc.retry_count,
    {{ dbt_utils.generate_surrogate_key(
        ['tr.contract_address', 'tr.function_sig']
    ) }} AS token_reads_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    transformed_responses tr
    LEFT JOIN retry_counts rc
        ON tr.contract_address = rc.contract_address
        AND tr.function_sig = rc.function_sig