{{ config(
    materialized='table',
    schema='marts',
    alias='mart_pipeline_audit_status',
    post_hook=[
      "CREATE UNIQUE INDEX IF NOT EXISTS idx_mart_pipeline_audit_status_job_target ON {{ this }} (job_name, target_table);",
      "CREATE INDEX IF NOT EXISTS idx_mart_pipeline_audit_status_status ON {{ this }} (status);",
      "CREATE INDEX IF NOT EXISTS idx_mart_pipeline_audit_status_job_type ON {{ this }} (job_type);",
      "CREATE INDEX IF NOT EXISTS idx_mart_pipeline_audit_status_target_table ON {{ this }} (target_table);",
      "CREATE INDEX IF NOT EXISTS idx_mart_pipeline_audit_status_started_at ON {{ this }} (started_at);",
      "CREATE INDEX IF NOT EXISTS idx_mart_pipeline_audit_status_finished_at ON {{ this }} (finished_at);",
      "CREATE INDEX IF NOT EXISTS idx_mart_pipeline_audit_status_freshness ON {{ this }} (freshness_status);"
    ]
) }}

with audit_runs as (

    select
        job_run_id,
        job_name,
        job_type,
        source,
        target_system,
        target_table,
        app_env,

        status,

        started_at,
        finished_at,

        case
            when started_at is not null
             and finished_at is not null
                then extract(epoch from finished_at - started_at)::integer
            else null
        end as duration_seconds,

        effective_load_date,

        coalesce(freshness_status, 'unknown') as freshness_status,
        snapshot_age_days,

        coalesce(rows_read, 0) as rows_read,
        coalesce(rows_inserted, 0) as rows_inserted,

        error_message,
        metadata_json

    from audit.job_runs

),

latest_runs as (

    select
        audit_runs.*,

        row_number() over (
            partition by
                job_name,
                target_table
            order by
                started_at desc,
                job_run_id desc
        ) as latest_run_rank

    from audit_runs

),

run_history as (

    select
        job_name,
        target_table,

        count(*) as total_run_count,

        count(*) filter (
            where status in ('success', 'success_with_warnings')
        ) as success_run_count,

        count(*) filter (
            where status not in ('success', 'success_with_warnings')
        ) as failed_or_incomplete_run_count,

        max(finished_at) filter (
            where status in ('success', 'success_with_warnings')
        ) as last_success_at,

        max(finished_at) filter (
            where status not in ('success', 'success_with_warnings')
        ) as last_failure_at,

        max(started_at) as last_started_at,
        max(finished_at) as last_finished_at

    from audit_runs

    group by
        job_name,
        target_table

),

final as (

    select
        l.job_run_id,
        l.job_name,
        l.job_type,
        l.source,
        l.target_system,
        l.target_table,
        l.app_env,

        l.status,

        case
            when l.status in ('success', 'success_with_warnings') then true
            else false
        end as is_successful_latest_run,

        case
            when l.status not in ('success', 'success_with_warnings') then true
            else false
        end as is_failed_or_incomplete_latest_run,

        l.started_at,
        l.finished_at,
        l.duration_seconds,

        l.effective_load_date,

        l.freshness_status,
        l.snapshot_age_days,

        l.rows_read,
        l.rows_inserted,

        coalesce(h.total_run_count, 0) as total_run_count,
        coalesce(h.success_run_count, 0) as success_run_count,
        coalesce(h.failed_or_incomplete_run_count, 0) as failed_or_incomplete_run_count,

        h.last_success_at,
        h.last_failure_at,

        case
            when h.last_success_at is not null then true
            else false
        end as has_successful_run,

        case
            when l.status in ('success', 'success_with_warnings') then 'healthy'
            when l.status not in ('success', 'success_with_warnings')
             and h.last_success_at is not null then 'latest_failed_previous_success_available'
            else 'not_healthy'
        end as pipeline_health_status,

        l.error_message,
        l.metadata_json,

        current_timestamp as mart_loaded_at

    from latest_runs l

    left join run_history h
        on l.job_name = h.job_name
       and (
            l.target_table = h.target_table
            or (l.target_table is null and h.target_table is null)
       )

    where l.latest_run_rank = 1

)

select *
from final