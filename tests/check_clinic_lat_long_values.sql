select * 
from  {{ ref('VW_CLINICS_CREATE_UPDATE')}} 
where
    TRY_TO_NUMBER(latitude, 6) = 0 
    or TRY_TO_NUMBER(longitude, 6) = 0 
    or  TRY_TO_NUMBER(split_part(map_coordinates, ' ', 1), 6) = 0 
    or TRY_TO_NUMBER(split_part(map_coordinates, ' ', 2), 6) = 0