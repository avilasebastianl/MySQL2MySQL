SELECT 
    t.PROCESSLIST_ID as id,
    IF(NAME = 'thread/sql/event_scheduler','event_scheduler',t.PROCESSLIST_USER) as user,
    t.PROCESSLIST_INFO as info
FROM
    performance_schema.threads t
        LEFT OUTER JOIN
    performance_schema.session_connect_attrs a ON t.processlist_id = a.processlist_id
        AND (a.attr_name IS NULL
        OR a.attr_name = 'program_name')
WHERE
    t.PROCESSLIST_USER <> '{usuario}' and t.PROCESSLIST_INFO like '%{table_name}%' 