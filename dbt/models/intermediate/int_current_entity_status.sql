{{ config(materialized='ephemeral') }}

with ranked_status as (
    select
        entity_type,
        entity_hk,
        entity_business_key,
        record_status,
        is_deleted,
        source_event_id,
        effective_from,
        load_datetime,
        source_lsn,
        kafka_partition,
        kafka_offset,
        row_number() over (
            partition by entity_type, entity_hk
            order by
                source_lsn desc nulls last,
                effective_from desc nulls last,
                kafka_partition desc,
                kafka_offset desc
        ) as status_rank
    from {{ source('cdc_raw_vault', 'sat_entity_record_status') }}
)

select *
from ranked_status
where status_rank = 1
  and record_status = 'ACTIVE'
  and not is_deleted
