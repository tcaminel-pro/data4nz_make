# lca_engine_dbt

## Overview
The objective of this dbt package is to align different datasets with emission factors (EF) for LCA and carbon footprint search and calculation.

It creates
   - SQL views to align the different datasets
   - 2 tabbles, one with the EF description, and another one for the values.
   - Index for Full Search Text in the EF descriptions
   - Queries to compute LCA for a given operational data table. 
   
The datasets merged are :
 - EcoInvent 3.8  with EF3.0 method
    - quite complete injection 
 - ADEME 'Base Carbone' 2022 
    - Only GWP (CO2)
 - NegaOctet 2022
    - Some EF missing
 - CEDA 2022
     - Only for US
     - Only GWP 
 - DEFRA 2022
      - Only GWP

     
 The project needs dbt 1.3.0.
 It has been tested with PostgreSQL 14 only.  Some changes will be needed to generate views and tables in another DWH (for array, string and full text indexing).
         
      
## Installation
   
The package is aimed to be incorporated in a more operational dbt project, with actual data.

The datasets has been injected by runnig Python scripts from the system project 'lca_engine_inject'. 

See dbt documentation abour how to install a package in a project. In few words:  

- create a packages.yml file with:
  ```
    packages:
       git: https://github.com/FR-PAR-ECOACT/lca_engine_dbt.git
       revision: master
   ```

- add in dbt-poject.yml:
  ```
    vars:
        lca_engine_dbt:
           operational_data: "{{ ref('your operational data table') }}" 
   ```
- lastly :
  ```
  dbt deps
  dbt run
  dbt test
  ```
## Model
Emission Factors datasets are merged into 2 tables:
- ef_merged_items : description of the items. Contains :
  - Unique id
  - Short description
  - Json field with other information when available (dataset specific). 
  - Product unit (not aligned yet)
  - Full text index (for Postgress)
- ef_merged_values : emission factor values. Contains :
  - item id
  - emission factor name (aligned to EcoAct vocabulary)
  - Geography (not aligned yet)
  - value
  - Json field for other custom specific information.
 
That schema should allow to combine good performances and ability to store dataset specific information.    

Operational data should be stored in a model whose name is in  the var `operational_data` 
The required fields are:  
  - tbd
  - tbd
The model `opdata_ef_grouped` calculate the LCA / Carbon footprint.
`opdata_ef_grouped_row` generate a pivot view.

 
     
