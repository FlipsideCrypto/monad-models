{{ config (
    materialized = "view",
    tags = ['streamline_testnet_complete']
) }}

select * 
from (
SELECT
    _id,
    (
        ({{ var('GLOBAL_BLOCKS_PER_HOUR',0) }} / 60) * {{ var('GLOBAL_CHAINHEAD_DELAY',10) }}
    ) :: INT AS block_number_delay, --minute-based block delay
    (_id - block_number_delay) :: INT AS block_number,
    utils.udf_int_to_hex(block_number) AS block_number_hex
FROM
    {{ ref('utils__number_sequence') }}
WHERE
    _id <= (
        SELECT
            COALESCE(
                block_number,
                0
            )
        FROM
            {{ ref("streamline__get_testnet_chainhead") }}
    )
)
where block_number > 0