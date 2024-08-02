SELECT 
    CASE 
        WHEN COLUMN_TYPE = 'int'      THEN max
        WHEN COLUMN_TYPE = 'date'     THEN date_format(max - INTERVAL {__INTERVAL_DAY_ROLL_BACK__} DAY ,"%Y-%m-%d")
        WHEN COLUMN_TYPE = 'datetime' THEN max - INTERVAL {__INTERVAL_HOUR_ROLL_BACK__} HOUR
	ELSE max
    END AS LAST_ROW,
    COLUMN_TYPE
FROM
    (SELECT 
        (SELECT 
				`{column_name}`
			FROM
				`{bbdd}`.`{table_name}`
			ORDER BY `{column_name}` {order_mode}
			LIMIT 1) AS max,
		c.COLUMN_TYPE
    FROM
        information_schema.COLUMNS c
    WHERE
        c.TABLE_SCHEMA = '{bbdd}' 
        AND c.TABLE_NAME = '{table_name}' 
        AND c.COLUMN_NAME = '{column_name}') AS a;
