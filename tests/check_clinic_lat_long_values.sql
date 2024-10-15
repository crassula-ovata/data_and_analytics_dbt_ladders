select * 
from  {{ ref('VW_CLINICS_CREATE_UPDATE')}} 
where
    latitude = 0 
    or longitude = 0 
    or split_part(map_coordinates, ' ', 1) = 0 
    or split_part(map_coordinates, ' ', 2) = 0 