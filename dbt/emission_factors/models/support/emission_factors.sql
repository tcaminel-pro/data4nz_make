/*
List of supported emission factors

Copyright (C) 2023 Atos. All rights reserved
*/

select * from (values
  ('AE','acidification','accumulated exceedance (AE)','mol H+ eq'),
  ('GWP','climate change','global warming potential (GWP100)','kg CO2 eq'),
  ('CTUe','ecotoxicity: freshwater','comparative toxic unit for ecosystems (CTUe)','CTUe'),
  ('ADPf','energy resources: non-renewable','abiotic depletion potential (ADP): fossil fuels','MJ (net calorific value)'),
  ('Epf','eutrophication: freshwater','fraction of nutrients reaching freshwater end compartment (P)','kg P eq'),
  ('Epm','eutrophication: marine','fraction of nutrients reaching marine end compartment (N)','kg N eq'),
  ('Ept','eutrophication: terrestrial','accumulated exceedance (AE)','mol N eq'),
  ('CTUh-c','human toxicity: carcinogenic','comparative toxic unit for human (CTUh)','CTUh'),
  ('CTUh-nc','human toxicity: non-carcinogenic','comparative toxic unit for human (CTUh)','CTUh'),
  ('IR','ionising radiation: human health','human exposure efficiency relative to u235','kBq U235 eq'),
  ('LU','land use','soil quality index','dimensionless'),
  ('ADPe','material resources: metals/minerals','abiotic depletion potential (ADP): elements (ultimate reserves)','kg Sb eq'),
  ('ODP','ozone depletion','ozone depletion potential (ODP)','kg CFC-11 eq'),
  ('PM','particulate matter formation','impact on human health','disease incidence'),
  ('POCP','photochemical oxidant formation: human health','tropospheric ozone concentration increase','kg NMVOC eq'),
  ('WU','water use','user deprivation potential (deprivation-weighted water consumption)','m3 world eq. deprived')

) as t("emission_factor","category","indicator", "unit")