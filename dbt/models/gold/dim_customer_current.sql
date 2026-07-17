with ranked_customer_details as (
    select
        customer_hk,
        customer_type,
        date_of_birth,
        segment_code,
        status_code,
        home_branch_code,
        updated_at,
        effective_from,
        source_lsn,
        kafka_partition,
        kafka_offset,
        row_number() over (
            partition by customer_hk
            order by
                effective_from desc nulls last,
                source_lsn desc nulls last,
                kafka_partition desc,
                kafka_offset desc
        ) as detail_rank
    from {{ source('cdc_raw_vault', 'sat_customer_details') }}
),

current_customer_details as (
    select
        customer_hk,
        customer_type,
        {{ cdc_date('date_of_birth') }} as date_of_birth,
        segment_code,
        status_code,
        home_branch_code,
        {{ cdc_timestamp('updated_at') }} as source_updated_at,
        effective_from as source_effective_from
    from ranked_customer_details
    where detail_rank = 1
),

customer_with_age as (
    select
        *,
        date_diff('year', date_of_birth, current_date) as customer_age
    from current_customer_details
)

select
    hub.customer_hk,
    hub.customer_bk as customer_id,
    details.customer_type,
    details.segment_code,
    details.status_code,
    details.home_branch_code,
    case
        when details.customer_age is null then 'UNKNOWN'
        when details.customer_age < 18 then 'UNDER_18'
        when details.customer_age < 25 then '18_24'
        when details.customer_age < 35 then '25_34'
        when details.customer_age < 45 then '35_44'
        when details.customer_age < 55 then '45_54'
        when details.customer_age < 65 then '55_64'
        else '65_PLUS'
    end as age_band,
    details.status_code = 'ACTIVE' as is_active,
    details.source_updated_at,
    details.source_effective_from,
    current_timestamp as dbt_loaded_at
from {{ source('cdc_raw_vault', 'hub_customer') }} as hub
inner join customer_with_age as details
    on hub.customer_hk = details.customer_hk
