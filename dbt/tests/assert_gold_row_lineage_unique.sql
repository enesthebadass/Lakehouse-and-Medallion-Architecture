select
    gold_model,
    gold_business_key,
    raw_vault_object,
    source_event_id,
    count(*) as duplicate_count
from {{ ref('gold_row_lineage') }}
group by 1, 2, 3, 4
having count(*) > 1
