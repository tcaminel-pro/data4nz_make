/* 
Combine stg_ecoinvent, comming from the Excel file, with calculated records
  
Copyright (C) 2023 Eviden. All rights reserved
*/

select * from {{ref('stg_ecoinvent')}}
union
select * from {{ref('ecoinvent_elect_large_countries')}}

