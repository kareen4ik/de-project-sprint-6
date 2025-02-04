CREATE TABLE IF NOT EXISTS STV2025011438__DWH.s_auth_history (
    hk_l_user_group_activity INT, 
    user_id_from             INT,  
    event                    VARCHAR(50), 
    event_dt                 TIMESTAMP,   
    load_dt                  TIMESTAMP,   
    load_src                 VARCHAR(20)  
);

--заливаем данные

INSERT INTO STV2025011438__DWH.s_auth_history(
    hk_l_user_group_activity, user_id_from, event, event_dt, load_dt, load_src
)
SELECT 
    luga.hk_l_user_group_activity,
    gl.user_id_from,
    gl.event,
    gl.datetime AS event_dt,
    NOW() AS load_dt,
    's3' AS load_src
FROM STV2025011438__STAGING.group_log AS gl
LEFT JOIN STV2025011438__DWH.h_groups AS hg 
       ON gl.group_id = hg.group_id
LEFT JOIN STV2025011438__DWH.h_users AS hu 
       ON gl.user_id = hu.user_id
LEFT JOIN STV2025011438__DWH.l_user_group_activity AS luga
       ON hg.hk_group_id = luga.hk_group_id
      AND hu.hk_user_id = luga.hk_user_id;