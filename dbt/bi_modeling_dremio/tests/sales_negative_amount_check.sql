 select 
    id 
from 
    {{ source('layer_bronze', 'sales')}} 
where amount_paid <= 0
 /*
  This test checks if there are any negative or zero amounts in the sales table.
  */