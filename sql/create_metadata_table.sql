CREATE TABLE `tb_metadata_espejos_de_tablas` (
  `ip_or` varchar(16) NOT NULL,
  `port_or` int NOT NULL,
  `bbdd_or` varchar(64) NOT NULL,
  `ip_des` varchar(16) NOT NULL,
  `port_des` int NOT NULL,
  `bbdd_des` varchar(64) NOT NULL,
  `table_name_or` varchar(64) DEFAULT NULL,
  `table_name_des` varchar(64) DEFAULT NULL,
  `column_name` varchar(32) DEFAULT NULL,
  `column_type` enum('DATE','DATETIME','ID') NOT NULL,
  `mode` enum('TRUNCATE','DELETE','REPLACE') NOT NULL,
  `ejecucion` enum('Hora a hora','Dia Vencido') NOT NULL,
  PRIMARY KEY (`ip_or`,`port_or`,`bbdd_or`,`ip_des`,`port_des`,`bbdd_des`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
