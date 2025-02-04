CREATE TABLE IF NOT EXISTS STV2025011438__STAGING.group_log (
    group_id       INT,
    user_id        INT,
    user_id_from   INT,
    event          VARCHAR(50),
    datetime       TIMESTAMP
)
ORDER BY group_id
SEGMENTED BY HASH(group_id) ALL NODES
PARTITION BY datetime::DATE
GROUP BY calendar_hierarchy_day(datetime::DATE, 3, 2)
