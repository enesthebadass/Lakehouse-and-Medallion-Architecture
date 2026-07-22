{{ config(materialized='ephemeral') }}

with ranked_context as (
    select
        *,
        row_number() over (
            partition by loan_application_hk
            order by
                source_lsn desc nulls last,
                effective_from desc nulls last,
                kafka_partition desc,
                kafka_offset desc
        ) as context_rank
    from {{ source('cdc_raw_vault', 'sat_application_context_effectivity') }}
)

select *
from ranked_context
where context_rank = 1
  and record_status = 'ACTIVE'
  and not is_deleted
