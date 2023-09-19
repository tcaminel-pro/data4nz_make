/*
Map EcoInvent emission factors categories with EcoAct taxomomy. (taken from Guillaume)

WARNING: It's a subset, gathered for L'Oréal project. There are more in the complete EI dataset.

*/
select
    category as ei_indicator,
    case
        when category like 'acidification%'
        then 'AE'
        when category like 'climate change%'
        then 'GWP'
        when category like 'ecotoxicity: freshwater%'
        then 'CTUe'
        when category like 'human toxicity: non-carcinogenic%'
        then 'CTUh-nc'
        when category like 'human toxicity: carcinogenic%'
        then 'CTUh-c'
        when category = 'ionising radiation: human health'
        then 'IR'
        when category = 'water use'
        then 'WU'
        when category = 'particulate matter formation'
        then 'PM'
        when category = 'ozone depletion'
        then 'ODP'
        when category = 'eutrophication: terrestrial'
        then 'Ept'
        when category = 'eutrophication: marine'
        then 'Epm'
        when category = 'eutrophication: freshwater'
        then 'Epf'
        when category = 'land use'
        then 'LU'
        when category = 'photochemical ozone formation: human health'
        then 'POCP'
        when category = 'material resources: metals/minerals'
        then 'ADPe'
        when category like 'energy resources: non-renewable%'
        then 'ADPf'
        else concat('??', category)
    end as emission_factor

from ref_data."EI3.9_Categories"
