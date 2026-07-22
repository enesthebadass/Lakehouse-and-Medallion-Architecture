with ranked_currency_details as (
    select
        currency_hk,
        currency_name,
        minor_unit,
        is_active,
        updated_at,
        effective_from,
        load_datetime,
        source_lsn,
        kafka_partition,
        kafka_offset,
        row_number() over (
            partition by currency_hk
            order by
                effective_from desc nulls last,
                source_lsn desc nulls last,
                kafka_partition desc,
                kafka_offset desc
        ) as detail_rank
    from {{ source('cdc_raw_vault', 'sat_currency_details') }}
)

select
    hub.currency_hk,
    hub.currency_bk as currency_code,
    details.currency_name,
    try_cast(nullif(trim(details.minor_unit), '') as integer) as minor_unit,
    coalesce(try_cast(details.is_active as boolean), false) as is_active,
    {{ cdc_timestamp('details.updated_at') }} as source_updated_at,
    details.effective_from as source_effective_from,
    details.load_datetime as source_load_datetime,
    current_timestamp as dbt_loaded_at
from {{ source('cdc_raw_vault', 'hub_currency') }} as hub
inner join {{ ref('int_current_entity_status') }} as entity_status
    on hub.currency_hk = entity_status.entity_hk
    and entity_status.entity_type = 'CURRENCY'
inner join ranked_currency_details as details
    on hub.currency_hk = details.currency_hk
    and details.detail_rank = 1
