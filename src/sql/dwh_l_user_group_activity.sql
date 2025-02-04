CREATE TABLE IF NOT EXISTS STV2025011438__DWH.l_user_group_activity (
    hk_l_user_group_activity INT PRIMARY KEY,   
    hk_user_id               INT,              
    hk_group_id              INT,               
    load_dt                  TIMESTAMP,        
    load_src                 VARCHAR(20)       
);


--сюда же добавлю код вставки данных
INSERT INTO STV2025011438__DWH.l_user_group_activity(
    hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_src
)
SELECT DISTINCT
    HASH(hu.hk_user_id || hg.hk_group_id) AS hk_l_user_group_activity,
    hu.hk_user_id,
    hg.hk_group_id,
    NOW() AS load_dt,
    's3' AS load_src
FROM STV2025011438__STAGING.group_log AS gl
LEFT JOIN STV2025011438__DWH.h_users AS hu 
       ON gl.user_id = hu.user_id
LEFT JOIN STV2025011438__DWH.h_groups AS hg 
       ON gl.group_id = hg.group_id;