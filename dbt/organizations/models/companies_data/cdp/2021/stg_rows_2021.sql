select row_number() over (), *  
from {{source("cdp_excel_files","rows_2021")}}