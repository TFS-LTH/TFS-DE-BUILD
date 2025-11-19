With total_hotel_mat as(
Select
a.business_date as as_of_date
,b.hotel_id
,CASE WHEN a.hotel_code='RFHDL1' THEN 'LTHDL'
    WHEN a.hotel_code='KSHPP' THEN 'KPHPN'
    ELSE a.hotel_code
    END AS hotel_code
,b.no_of_rooms AS inventory
,c.source as source_nm
,c.segment as segment_nm
,a.room_nights
,a.room_revenue
FROM gold.fact_hotel_tags a
LEFT JOIN md.hotels b ON a.hotel_code=b.hotel_code
LEFT JOIN gold.dim_source_segment c ON a.src_sgmnt_id=c.src_sgmnt_id
-- where business_date='2025-11-18' and a.hotel_code='AHRMB1'
ORDER BY business_date
)


SELECT
as_of_date
,hotel_code
,inventory
,SUM(room_nights) as ROB
,SUM(room_revenue) as total_room_revenue
From total_hotel_mat
Group BY as_of_date,hotel_code,inventory
ORDER BY as_of_date