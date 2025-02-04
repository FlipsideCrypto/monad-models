-- depends_on: {{ ref('bronze_testnet__receipts') }}
{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['modified_timestamp::DATE','partition_key'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(block_number)",
    tags = ['silver_testnet']
) }}

WITH bronze_receipts AS (
    SELECT 
        block_number,
        partition_key, 
        array_index,
        DATA AS receipts_json,
        _inserted_timestamp
    FROM 
    {% if is_incremental() %}
    {{ ref('bronze_testnet__receipts') }}
    WHERE _inserted_timestamp >= (
        SELECT 
            COALESCE(MAX(_inserted_timestamp), '1900-01-01'::TIMESTAMP) AS _inserted_timestamp
        FROM {{ this }}
    ) AND DATA IS NOT NULL
    {% else %}
    {{ ref('bronze_testnet__receipts_fr') }}
    WHERE DATA IS NOT NULL
    {% endif %}
)

SELECT 
    block_number,
    partition_key,
    array_index,
    receipts_json,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['block_number','array_index']) }} AS receipts_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM bronze_receipts
where array_index is not null
QUALIFY ROW_NUMBER() OVER (PARTITION BY receipts_id ORDER BY block_number DESC, _inserted_timestamp DESC) = 1