/* Get chemical proxies (substances havig similar EF) for some EcoInvent items
  from mappings maintained by Lau (EcoAct) 

  Copyright (C) 2023 Eviden. All rights reserved  
*/

with 
proxies_without_cas as (
    select 
        ecoinvent_process_name, 
        substance_description  as subst_desc
    from {{ref('stg_mapping_ei_desc')}} 
    where ecoinvent_process_name is not null) , 

proxies_with_cas as (
    select 
        ecoinvent_process_name, 
        concat (substance_description, ' #0000', substance_cas_number)  as subst_desc
    from {{ref('stg_mapping_ei_cas')}} 
    where ecoinvent_process_name is not null) , 

union_proxies as (
    select ecoinvent_process_name, subst_desc from proxies_without_cas
    union all
    select ecoinvent_process_name, subst_desc from proxies_with_cas),

group_to_json as (
    select 
        ecoinvent_process_name, 
        json_agg(subst_desc) as mapped_proxies
    from union_proxies
	group by ecoinvent_process_name )

select * from group_to_json 
