select *
from {{ source('pachotel_dbt', 'customer') }}