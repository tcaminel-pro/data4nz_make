# data4nz_make

The project consits of two parts, a
- dbt project
- pyhton code.

The purpose of the project is to generate a generalized data-model, for the given not hetrogen input-data. 

## Workflow

The image below shows the workflow, it shows what the project within this repository are used for.
How to execute the steps is described below.

![Worfklow](workflowdata4zmake.jpg?raw=true "Title")



## DBT Project

The dbt project does have the following structure https://docs.getdbt.com/docs/build/projects.  The model section defines which transformations will be done.
Depending on the given input some files needed to be removed before running dbt.

## Pyhton Code

The Pyhton code section included various scripts used either to import datasets into postgres DB, used to setup the db before running the dbt project or to
export the produced data from postgres in to a parquet files.

## Data sets.

The Input data-sets which are of course essential for the project are not included within this repository due to leagl reason. To get one please ask
thierry.caminel@atos.net or wolfgang.a.friedl@atos.net.

# Run

## Run - local

### Prerequisites


Having DBT 1.5.0 installed. https://docs.getdbt.com/docs/core/pip-install (`pip install --upgrade dbt-core==1.5.0`)

Having DBT-Postgres installed (`pip install dbt-postgres`)

Having Posgres 13.8 installed.

Having input Datsets available.


## Data Import using Pyhton

### Configure Posgress DB

Create schema `ref_data` on Postgres DB.

### Configure DBT

Adapt your `C:\Users\<USERID>\.dbt\profiles.yml` and add

```
emission_factors_make:  
  outputs:
  
    dev:
      type: postgres
      threads: 1
      host: localhost
      port: 5432
      user: postgres
      pass: '<POSTGRES-PASSWORD>'
      dbname: postgres
      schema: ref_data
      
  target: dev
 ```
adapt the values above if a differnt PORT or HOST or Password is used according the definition in the `\dbt\emission_factors\dbt_project.yml`

### Import Initial - Datasets into Postgres

To avoid getting in conflict with other already installed pyhton libs and application we use pythons virutal environment.

Got to ` python>`

Create an virtual enviornment call

`py -m venv <NAME-of-virutal-env>`

within the projet folder. Once this is done you call

`.\<NAME-of-virutal-env>\Scripts\activate`

to activate it. To stop the virtual environement simply call `deactivate`

Now install the needed requirements for the application by calling 
`pip install -r .\requirements.txt`

Set environment variable DATASET_PATH to the current path where your datasets are stored


#### Import Carbon-Minds data

On windows the following call will work, which includes setting the DATA4NZ_DATALAKE,DATA4NZ_DATASETS,DATA4NZ_MAKE_FOLDER env variable 

`$ENV:DATA4NZ_DATALAKE="C:\Repo\data4nz_make\datalake";$Env:DATA4NZ_DATASETS="C:\Repo\data4nz_make\datasets";$Env:DATA4NZ_MAKE_FOLDER="C:\Repo\data4nz_make\";py .\python\inject\cmd_from_dbt_model.py gen-tables .\dbt\emission_factors\models\sources.yml`

Call `py .\python\inject\cmd_from_dbt_model.py gen-tables .\dbt\emission_factors\models\sources.yml`   to import carbonminds data

#### Import Eco-Invent data


#### Import negaoctet data


## DBT 

Got to ` dbt\emission_factors>`

Adapt `dbt\emission_factors\models\sources.yml` based on what datae are available. (Errors while running dbt run will show it)

Remove `.sql` files from the `dbt\emission_factors\models\ef_alligment` folder for which you have not imported data into the db (see pyhton section)

Adapt the files `ef_merged_items_json.sql, ef_merged_items_view.sql, ef_merged_values.ql and ef_merged_values_view.sql` and remove thos tables from the script which you have no input for. 


### Execute DBT 

Call `dbt debug`

Call `dbt deps`

Call `dbt seed`

Call `dbt run` which should end with `Completed successfully`

If succesfull the generalized data-sets are written into the defined postgres db.

## Create Parquet file from Postgres Data

Create folder structure `datalake\parquet\emission_factors`

Call `$ENV:DATA4NZ_DATALAKE="C:\Repo\data4nz_make\datalake";$Env:DATA4NZ_DATASETS="C:\Repo\data4nz_make\datasets";$Env:DATA4NZ_MAKE_FOLDER="C:\Repo\data4nz_make\"; py .\inject\cmd_from_dbt_model.py gen-parquet`

Parquet files (ef_merged_items_1a.parquet, ef_merged_values_1a.parquet) are created within the folders `datalake\parquet\emission_factors` 
