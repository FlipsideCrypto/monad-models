{% macro migrate_token_reads_columns(model_relation) %}
    {% set columns = adapter.get_columns_in_relation(model_relation) %}
    {% set col_names = columns | map(attribute='name') | list %}

    {% if 'MODIFIED_TIMESTAMP' not in col_names %}
        {% do run_query("ALTER TABLE " ~ model_relation ~ " ADD COLUMN MODIFIED_TIMESTAMP TIMESTAMP") %}
        {% do run_query("UPDATE " ~ model_relation ~ " SET MODIFIED_TIMESTAMP = _INSERTED_TIMESTAMP") %}
    {% endif %}

    {% if 'RETRY_COUNT' not in col_names %}
        {% do run_query("ALTER TABLE " ~ model_relation ~ " ADD COLUMN RETRY_COUNT INTEGER") %}
        {% do run_query("UPDATE " ~ model_relation ~ " SET RETRY_COUNT = 0") %}
    {% endif %}

    {% if 'TOKEN_READS_ID' not in col_names %}
        {% do run_query("ALTER TABLE " ~ model_relation ~ " ADD COLUMN TOKEN_READS_ID STRING") %}
        {% do run_query("UPDATE " ~ model_relation ~ " SET TOKEN_READS_ID = " ~ dbt_utils.generate_surrogate_key(['CONTRACT_ADDRESS', 'FUNCTION_SIG'])) %}
    {% endif %}

    {% if '_INVOCATION_ID' not in col_names %}
        {% do run_query("ALTER TABLE " ~ model_relation ~ " ADD COLUMN _INVOCATION_ID STRING") %}
        {% do run_query("UPDATE " ~ model_relation ~ " SET _INVOCATION_ID = '" ~ invocation_id ~ "'") %}
    {% endif %}
{% endmacro %} 