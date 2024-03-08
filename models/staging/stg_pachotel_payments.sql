select * 
from {{ source('pachotel_dbt', 'payment') }}