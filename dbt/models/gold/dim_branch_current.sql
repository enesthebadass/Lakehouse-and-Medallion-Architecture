with ranked_branch_details as (
    select
        branch_hk,
        branch_name,
        city,
        is_active,
        updated_at,
        effective_from,
        source_lsn,
        kafka_partition,
        kafka_offset,
        row_number() over (
            partition by branch_hk
            order by
                effective_from desc nulls last,
                source_lsn desc nulls last,
                kafka_partition desc,
                kafka_offset desc
        ) as detail_rank
    from {{ source('cdc_raw_vault', 'sat_branch_details') }}
)

select
    hub.branch_hk,
    hub.branch_bk as branch_code,
    details.branch_name,
    details.city,
    coalesce(try_cast(details.is_active as boolean), false) as is_active,
    {{ cdc_timestamp('details.updated_at') }} as source_updated_at,
    details.effective_from as source_effective_from,
    current_timestamp as dbt_loaded_at
from {{ source('cdc_raw_vault', 'hub_branch') }} as hub
inner join ranked_branch_details as details
    on hub.branch_hk = details.branch_hk
    and details.detail_rank = 1
