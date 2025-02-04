WITH user_group_log AS (
    SELECT 
        luga.hk_group_id,
        COUNT(DISTINCT luga.hk_user_id) AS cnt_added_users
    FROM STV2025011438__DWH.s_auth_history AS sah
    JOIN STV2025011438__DWH.l_user_group_activity AS luga
        ON sah.hk_l_user_group_activity = luga.hk_l_user_group_activity
    WHERE sah.event = 'add'  
    GROUP BY luga.hk_group_id
),

old_groups AS (
    SELECT hk_group_id
    FROM STV2025011438__DWH.h_groups
    ORDER BY registration_dt ASC
    LIMIT 10
)

SELECT 
    ulg.hk_group_id,
    ulg.cnt_added_users
FROM user_group_log AS ulg
JOIN old_groups AS og ON ulg.hk_group_id = og.hk_group_id
ORDER BY ulg.cnt_added_users DESC;