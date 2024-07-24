/*
List of supported datasets
MODIFY WITH CARE (they are sometime hardcoded)

Copyright (C) 2023 Eviden. All rights reserved
*/

select * from (values
    (1,'EcoInvent','3.8 - EF3.0','EcoInvent database - version 3.8 - method EF 3.0'),
    (2,'NegaOctet','2022','NegaOctet dataset - as 08/2022'),
    (3,'Ademe','2022 - French','ADEME Base Carbone 2022 - with descriptions in French'),
    (4,'Ademe','2022 - English','ADEME Base Carbone 2022 - with descriptions in English'),
    (5,'CEDA','2022 - US/US','CEDA Database - with US as target'),
    (6,'DEFRA','2022','DEFRA Database'),
    (7,'CarbonMind','2022 - EF3.0','CarbonMind database - version 2022 - method EF 3.0" ')
) as  t(id,provider,version,description)
