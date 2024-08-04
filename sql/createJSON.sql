SELECT 
    DENSE_RANK() OVER (ORDER BY ip_or ASC,port_or ASC,bbdd_or ASC) AS cid,
    ip_or, 
    port_or, 
    bbdd_or, 
    ip_des, 
    port_des, 
    bbdd_des, 
    table_name_or, 
    table_name_des, 
    `column_name`, 
    column_type, 
    `mode`, 
    ejecucion
FROM 
    tb_metadata_espejos_de_tablas
WHERE ejecucion = '{ejecucion}';