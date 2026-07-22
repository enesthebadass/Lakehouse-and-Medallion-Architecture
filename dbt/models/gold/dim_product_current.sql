with ranked_product_details as (
    select
        product_hk,
        product_name,
        product_type,
        default_currency_code,
        is_active,
        updated_at,
        effective_from,
        load_datetime,
        source_lsn,
        kafka_partition,
        kafka_offset,
        row_number() over (
            partition by product_hk
            order by
                effective_from desc nulls last,
                source_lsn desc nulls last,
                kafka_partition desc,
                kafka_offset desc
        ) as detail_rank
    from {{ source('cdc_raw_vault', 'sat_product_details') }}
)

select
    hub.product_hk,
    hub.product_bk as product_code,
    details.product_name,
    details.product_type,
    details.default_currency_code,
    coalesce(try_cast(details.is_active as boolean), false) as is_active,
    {{ cdc_timestamp('details.updated_at') }} as source_updated_at,
    details.effective_from as source_effective_from,
    details.load_datetime as source_load_datetime,
    current_timestamp as dbt_loaded_at
from {{ source('cdc_raw_vault', 'hub_product') }} as hub
inner join {{ ref('int_current_entity_status') }} as entity_status
    on hub.product_hk = entity_status.entity_hk
    and entity_status.entity_type = 'PRODUCT'
inner join ranked_product_details as details
    on hub.product_hk = details.product_hk
    and details.detail_rank = 1
