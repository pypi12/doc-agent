select distinct order_status
from {{ ref('stg_sales') }}
order by 1;
