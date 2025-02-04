WITH user_group_messages AS (
    SELECT 
        luga.hk_group_id,
        COUNT(DISTINCT lum.hk_user_id) AS cnt_users_in_group_with_messages
    FROM STV2025011438__DWH.l_user_message AS lum
    JOIN STV2025011438__DWH.l_user_group_activity AS luga
        ON lum.hk_user_id = luga.hk_user_id  
    GROUP BY luga.hk_group_id
)

SELECT 
    hk_group_id,
    cnt_users_in_group_with_messages
FROM user_group_messages
ORDER BY cnt_users_in_group_with_messages DESC
LIMIT 10;
