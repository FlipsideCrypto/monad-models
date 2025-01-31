{% set node_secret_path = var("GLOBAL_NODE_SECRET_PATH") %}

{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"testnet_blocks_transactions",
        "sql_limit" :"2000000",
        "producer_batch_size" :"7200",
        "worker_batch_size" :"1800",
        "sql_source" :"{{this.identifier}}",
        "exploded_key": tojson(["result", "result.transactions"]) }
    ),
    tags = ['streamline_testnet_history']
) }}

WITH to_do AS (
    SELECT block_number
    FROM {{ ref("streamline__testnet_blocks") }}
    EXCEPT
    SELECT block_number
    FROM {{ ref("streamline__testnet_blocks_complete") }} b
    INNER JOIN {{ ref("streamline__testnet_transactions_complete") }} t USING(block_number)
),
ready_blocks AS (
    SELECT block_number
    FROM to_do
)
SELECT
    block_number,
    ROUND(block_number, -3) AS partition_key,
    live.udf_api(
        'POST',
        '{Service}/{Authentication}',
        OBJECT_CONSTRUCT(
            'Content-Type', 'application/json',
            'fsc-quantum-state', 'streamline'
        ),
        OBJECT_CONSTRUCT(
            'id', block_number,
            'jsonrpc', '2.0',
            'method', 'eth_getBlockByNumber',
            'params', ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number), TRUE)
        ),
        '{{ node_secret_path }}'
    ) AS request
FROM
    ready_blocks
    
ORDER BY block_number desc

LIMIT 
    2000000