/*
List of supported datasets
MODIFY WITH CARE (they are sometime hardcoded)

Copyright (C) 2023 Eviden. All rights reserved
*/

select * from (values
    ('EI3.9','EcoInvent','3.9 - EF3.1','EcoInvent database - version 3.9 - method EF 3.1'),
    ('N022','NegaOctet','2022','NegaOctet dataset - as 08/2022'),
  --  ('ADM22FR','Ademe','2022 - French','ADEME Base Carbone 2022 - with descriptions in French'),
  --  ('ADM22EN','Ademe','2022 - English','ADEME Base Carbone 2022 - with descriptions in English'),
  --  ('CEDA','CEDA','2022 - US/US','CEDA Database - with US as target'),
  --  ('DEFRA22','DEFRA','2022','DEFRA Database'),
    ('CM22','CarbonMind','2022 - EF3.0','CarbonMind database - version 2022 - method EF 3.0" ')
) as  t(id,provider,version,description)