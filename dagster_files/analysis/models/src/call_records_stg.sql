with stg_call_records as(
select * from {{ source('nyc_311_calls', 'call_records')}}
)

select distinct(unique_key), created_date, closed_date,agency, agency_name, complaint_type,descriptor,
location_type, incident_zip, address_type, city, status, due_date, resolution_description,resolution_action_updated_date,
community_board,bbl, borough, open_data_channel_type, park_borough, latitude, longitude
from stg_call_records
