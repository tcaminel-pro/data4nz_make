/*
List of supported emission factors

Copyright (C) 2023 Eviden. All rights reserved
*/

select * from (values
    ('GWP','kg CO2 eq.','climate change'),
    ('GWPf','kg CO2 eq.','climate change: fossil'),
    ('GWPb','kg CO2 eq.','climate change: biogenic'),
    ('GWPlu','kg CO2 eq.','climate change: land use and land use change'),
    ('CTUe','CTUe','ecotoxicity: freshwater'),
    ('CTUh-nc','CTUh','human toxicity: non-carcinogenic'),
    ('CTUh-c','CTUh','human toxicity: carcinogenic'),
    ('Ept','mol N eq.','eutrophication: terrestrial'),
    ('Epm','kg N eq.','eutrophication: marine'),
    ('Epf','kg P eq.','eutrophication: freshwater'),
    ('ADPe','kg SB eq.','material resources: metals/minerals'),
    ('ADPf','MJ','energy resources: non-renewable'),
    ('PM','Disease occurrence','particulate matter formation'),
    ('ODP','kg CFC-11 eq.','ozone depletion'),
    ('AP','mol H+ eq.','acidification'),
    ('WU','m3 eq.','water use'),
    ('LU','No dimension','land use'),
    ('IR','kg U235 eq.','ionising radiation: human health'),
    ('POCP','kg NMVOC eq.','photochemical ozone formation: human health')
) as t("name","unit","description")