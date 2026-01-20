-- macros/log_dbt_results.sql

{% macro log_dbt_results() %}
  {% if execute %}

    {# --- Ensure schema exists --- #}
    {% do run_query("CREATE SCHEMA IF NOT EXISTS COMMISSIONING_MODELLING") %}

    {# --- Create run results table if not exists --- #}
    {% do run_query("""
      CREATE TABLE IF NOT EXISTS COMMISSIONING_MODELLING.DBT_OBSERVABILITY_RUNS (
        run_id STRING,
        invocation_id STRING,
        execution_time TIMESTAMP,
        status STRING,
        model_name STRING,
        rows_affected NUMBER,
        execution_time_seconds FLOAT
      );
    """) %}

    {# --- Filter for only model results (not tests) --- #}
    {% set model_results = results | selectattr('node.resource_type', 'in', ['model', 'seed', 'snapshot']) | list %}
    
    {% if model_results | length > 0 %}
      {% set results_sql %}
        INSERT INTO COMMISSIONING_MODELLING.DBT_OBSERVABILITY_RUNS (
          run_id,
          invocation_id,
          execution_time,
          status,
          model_name,
          rows_affected,
          execution_time_seconds
        )
        VALUES
        {% for result in model_results %}
          (
            '{{ invocation_id }}',
            '{{ invocation_id }}',
            CURRENT_TIMESTAMP(),
            '{{ result.status }}',
            '{{ result.node.name }}',
            {{ result.adapter_response.rows_affected | default(0) }},
            {{ result.execution_time }}
          )
          {% if not loop.last %},{% endif %}
        {% endfor %}
      {% endset %}

      {% do run_query(results_sql) %}
    {% endif %}

  {% endif %}
{% endmacro %}