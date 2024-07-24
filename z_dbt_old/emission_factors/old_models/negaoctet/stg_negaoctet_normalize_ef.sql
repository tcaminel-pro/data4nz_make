/*
Map NegaOctet emission factors categories with EcoAct taxomomy. (taken from Guillaume)

TO BE CHECKED  /extended by EcoAct consultant.

*/
select
    indicator as negaoctet_indicator,
    case
        indicator
        when 'Acidification'
        then 'AP'
        when 'Climate change'
        then 'GWP'
        when 'Ecotoxicity, freshwater'
        then 'CTUe'
        when 'Ionising radiation, human health'
        then 'IR'
        when 'Water use'
        then 'WU'
        when 'Resource use, minerals and metals'
        then 'ADPe'
        when 'Particulate matter'
        then 'PM'
        else concat('??', indicator)
    end as emission_factor
/*
Question to Guillaume : how to deal with:
         WHEN 'Total Primary Energy-[TPE (MJ)]
         WHEN 'Mass of Electric and Electronic Wastes-[WEEE (kg)]
         WHEN 'Material Input per Service-Unit-[MIPS (kg)]


*/
from ref_data."NO22_Indicators"
