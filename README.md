# ETL MySQL-to-MySQL
Este proyecto realiza una migración de información entre dos instancias de MySQL, utilizando dos archivos JSON que definen las configuraciones para las extracciones horarias y diarias.

## Descripción General
La aplicación es un proceso ETL (Extract, Transform, Load) que se encarga de extraer datos de una instancia de MySQL y cargarlos en otra (espejos de información o migraciones rapidas entre servidores agendos a los que el desarrollador administra). El flujo de datos está definido por dos archivos JSON:

data_to_run_hora_a_hora.json: Este archivo define las tablas que deben ser migradas cada hora.
data_to_run_dia_vencido.json: Este archivo define las tablas que deben ser migradas a la madrugada de cada día.
El proceso se ejecuta de forma automática según los intervalos definidos en los archivos de configuración.

## Estructura del Proyecto

```bash
ETL-MySQL-to-MySQL/
│
├── config/
│   ├── logger.yml
│   └── credentials.py
|
├── data/
│   ├── data_to_run_hora_a_hora.json
│   └── data_to_run_dia_vencido.json
│
├── sql/
│   ├── kill_query.sql
│   └── last_row.sql
|
├── docs/
│   └── documentation.txt
|
├── sql/
│   ├── kill_query.sql
│   └── last_row.sql
|
├── src/
│   ├── paths.py
│   └── utils.py
│
├── Main.pyw
├── README.md
├── .gitignore
└── requirements.txt
```

1. config/: Carpeta que contiene los archivos de configuración de credenciales y logguer.
2. data/: Carpeta que contiene los archivos JSON que seran leidos para su ejecucion segun su hora.
3. sql/: Carpeta que contiene las querys que son estandar para el proceso de encontrar max y min de una tabla e identificacion de querys que bloquean inserciones.
4. docs/: Carpeta que contiene la documentacion GNU que mediante la bandera --help muestra la metodologia de ejecucion con sus respectivo ejemplos.
5. log/: Carpeta que contiene los archivos .log donde se deja registro de ejecuciones y errores.
6. src/: Carpeta que contiene el código fuente del proyecto.
7. Main.py: Archivo a ejecutar con entorno virtual configurado.


# Configuración
Los archivos de configuración JSON deben seguir una estructura específica para definir qué datos deben ser migrados. Aquí hay un ejemplo de cada archivo o pueden basarse en el maket de la carpeta data:

```bash
[
    {   
        "cid":1,
        "ip_or":"",
        "port_or":"",
        "bbdd_or":"",
        "ip_des":"",
        "port_des":"",
        "bbdd_des":"",
        "table_name_or":"",
        "table_name_des":"",
        "column_name":"",
        "column_type":"",
        "mode":"REPLACE"
    },
    {   
        "cid":2,
        "ip_or":"",
        "port_or":"",
        "bbdd_or":"",
        "ip_des":"",
        "port_des":"",
        "bbdd_des":"",
        "table_name_or":"",
        "table_name_des":"",
        "column_name":"",
        "column_type":"",
        "mode":"TRUNCATE"
    },
    {   
        "cid":3,
        "ip_or":"",
        "port_or":"",
        "bbdd_or":"",
        "ip_des":"",
        "port_des":"",
        "bbdd_des":"",
        "table_name_or":"",
        "table_name_des":"",
        "column_name":"",
        "column_type":"",
        "mode":"DELETE"
    }
]
```
Anotaciones:
    - Existen solo 3 modos de insercion de datos en el destino: DELETE, TRUNCATE y REPLACE.
    - Si se da como referencia de fecha inicio y fecha fin un '*' migrara toda la informacion de dicha tabla.
    - Variable 'cid' es un autoincremental

# Instalación
Para instalar las dependencias necesarias, ejecuta el siguiente comando una vez activado el entorno virutal:
```bash
pip install -r requirements.txt
```
    - El script se ha probado con las librerias especificadas en entornos de Python3.10 y Python3.11