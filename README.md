# ETL MySQL-to-MySQL :rocket:

Este proyecto realiza una migración de información entre dos instancias de MySQL, utilizando dos archivos JSON que definen las configuraciones para las extracciones hora a hora y a dia vencido.

## Descripción General :memo:

La aplicación es un proceso ETL (Extract, Transform, Load) que se encarga de extraer datos de una instancia de MySQL y cargarlos en otra (espejos de información o migraciones rapidas entre servidores agedos a los que el desarrollador administra).

**Developed and documented by:** [@avilasebastianl](https://github.com/avilasebastianl) :nerd_face:

## Estructura del Proyecto :bar_chart:

```bash
ETL-MySQL-to-MySQL/
│
├── config/
│   ├── logger.yml
│   └── credentials.py
│
├── data/
│   ├── data_to_run_hora_a_hora.json
│   └── data_to_run_dia_vencido.json
│
├── sql/
│
├── docs/
│   └── documentation.txt
│
├── logs/
│   ├── execution.log
│   └── error.log
│
├── src/
│   ├── paths.py
│   └── utils.py
│
├── venv/
│
├── Main.pyw
├── README.md
├── .gitignore
└── requirements.txt
```

1. config/: Carpeta que contiene los archivos de configuración de credenciales y logger.
2. data/: Carpeta que contiene los archivos JSON que seran leidos para su ejecucion segun su hora.
3. sql/: Sentencias SQL que definen una tabla.
4. docs/: Carpeta que contiene la documentacion GNU que mediante la bandera --help muestra la metodologia de ejecucion con sus respectivo ejemplos.
5. log/: Carpeta que contiene los archivos .log donde se deja registro de ejecuciones y errores.
6. src/: Carpeta que contiene el código fuente del proyecto.
7. Main.py: Archivo a ejecutar con entorno virtual configurado.

## Configuración :gear:

El flujo de datos está definido por dos archivos JSON de configuración que deben seguir una estructura específica para definir los datos que deben deben ser migrados y de que manera. Aquí hay un ejemplo de cada archivo o pueden basarse en el maket de la carpeta data:

```json
[
    {
        "cid":1...n, // type: int
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
        "mode":"REPLACE | TRUNCATE | DELETE"
    }
]
```

> [!NOTE]
> Existen 3 modos de insercion de datos en el destino: DELETE, TRUNCATE y REPLACE.

> [!NOTE]
> Si se da como referencia de fecha inicio y fecha fin un '*' migrara toda la informacion de dicha tabla.

> [!NOTE]
> Variable 'cid' es un numero incremental que se define manualmente en cada uno de los JSON.

> [!TIP]
> Para las tablas que se migren de un servidor a varios pueden manejar el mismo cid, asi se asegurara que iran a la par en cada ejecucion.

> [!TIP]
> Los comentarios del codigo se ven mejor usando la extencion de VScode 'Better Comments'.

> [!IMPORTANT]
> El codigo maneja rutas relativas por lo que ejecutarlo tanto en Windows como en Linux no sera problema.

> [!CAUTION]
> Si la tabla que se quiere migrar no se encuentra en alguno de los servidores de Big Data, asegurese que el DDL (Metadata) de la creacion de la tabla sea compatible con la configuracion (Preferiblemente realize la creacion de la tabla de manera manual en el destino a partir del DDL del origen sin CONTRAINTS, FOREIGN KEYS y/o AUTOINCREMENTALES).

## Instalación :computer:

Para hacer uso del script del proyecto se debera ejeuctar el archivo .sh o .bat según el sistema operativo para crear los archivos de configuración necesarios para ejecutar el Main.pyw

:hammer_and_wrench: Versiones de Python con las que se ha probado el script: :snake:

- [ ] Python3.9
- [x] Python3.10
- [x] Python3.11
