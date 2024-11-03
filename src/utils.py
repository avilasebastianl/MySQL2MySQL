"""
Modulo principal de clases, metodos y funciones
# ! Developed by Big Data
"""

import datetime
import logging
import logging.config
import sys
import time
import json
from datetime import datetime
from urllib.parse import quote

import pandas as pd
import yaml
from pandas import json_normalize
from sqlalchemy import Column, MetaData, Table, create_engine, inspect, text
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.engine import reflection

from paths import *
sys.path.append(str(path_to_config))
from credentials import *  # type: ignore

# TODO: Configuracion de logger
with open(join_func(path_to_config,"logger.yml")) as f:
    logging.config.dictConfig(yaml.safe_load(f))

# ! DECLARACION DE CONSTANTES
# * Numero entero de las horas en la se ejecutara el archivo dia_vencido.json
__HOURS_TO_EXECUTE_EXPIRED_DAY__:list[int] = [4,6]
# * Modos de insercion de datos en la tabla destino
__ETL_INSERT_MODE__:list[str] = ['delete','truncate','replace']
# * Modos de insercion de datos en la tabla destino
__DATE_FORMAT_AVAIL__:list[str] = ['date','datetime','int','id']
# * Entero de la hora actual
__CURRENT_HOUR__:int = int(datetime.now().strftime("%H"))
# * Fecha de inicio de las consultas que no existen en el destino
__BASE_DATETIME_TO_EXECUTE__:str = "2024-04-01 00:00:00"
__BASE_DATE_TO_EXECUTE__:str = "2024-04-01"
# * Cantidad de registros enviado a mysql por paquetes
__CHUNKSIZE_TO_INSERT__:int = 150000
# * Numero de registros los cuales se filtraran cuando 
# * sea la columna filtro un autoincremental
__MAX_ID_TO_FILTER__:int = 50000
# * Maximo de dias que se se van a ejecutar hacia atras
__INTERVAL_DAY_ROLL_BACK__:int = 7 # * Cuando la columna es tipo date
__INTERVAL_HOUR_ROLL_BACK__:int = 3 # * Cuando la columna es tipo datetime
# * Dicionario de formatos para las fechas
__DICT_DATES_FORMAT__:dict[str:str] = {
    "datetime" : "%Y-%m-%d %H:%M:%S",
    "date" : "%Y-%m-%d"
}

class DatabaseConnections:
    """
    Clase para realizar transacciones para el manejo de 
    variables y para las conexiones a MySQL.

    Esta clase proporciona métodos para crear motores 
    de MySQL y extraer información relevante de dichos motores.
    """

    # * Funcion para retornar cadenas de conexion a MySQL
    def mysql_engine(ip:str,port:str,bbdd:str) -> Engine:
        """
        Creacion de motor de MySQL para generar conexiones
        y acceso a metadata segun la base de datos obtenida

        Args:
            ip (str): IP de instancia de MySQL destino de conexion
            port (str): Puerto de instancia de MySQL de destino de conexion
            bbdd (str): Base de datos donde se dea hacer la conexion

        Returns:
            Engine: Motor de MySQL. Solo se accede a la metadata 
            de la base de datos ingresada.
        """
        if ip in dict_user.keys(): # type:ignore
            return create_engine(f'mysql+pymysql://{dict_user.get(ip)}:{quote(dict_pwd.get(ip))}@{ip}:{port}/{bbdd}',pool_recycle=9600,isolation_level="AUTOCOMMIT") # type: ignore
        else:
            logging.getLogger("dev").error(f"Unkown IP '{ip}'. First add it to the credentials file diccionary with it's user and password")
            sys.exit(1)

    # * Funcion para obetener el usuario con el cual se hizo la url del motor de MySQL
    def obtain_info_from_engine(engine:Engine,to_extract:str) -> str:
        """
        Extrae información sobre el motor de MySQL basándose en el parámetro to_extract.

        Args:
            engine (Engine): Motor de MySQL.
            to_extract (str): Indicador de qué información extraer ('usuario', 'info', 'ip', 'bbdd').

        Returns:
            str: Información extraída según to_extract.
        """
        info_map:dict[str:str] = {
            'usuario': lambda _ : engine.url.username,
            'user': lambda _ : engine.url.username,
            'info': lambda _ : f"{engine.url.host}:{engine.url.port}@{engine.url.database}",
            'ip': lambda _ : f"{engine.url.host}:{engine.url.port}@{engine.url.database}",
            'bbdd': lambda _ : engine.url.database
        }
        return info_map.get(to_extract.lower(), lambda _: "")()

class ReadFiles:
    """
    Clase para realizar transacciones para el manejo de archivos SQL y para la obtención de consultas SQL.

    Esta clase proporciona métodos para obtener la consulta SQL formateada y para leer consultas SQL desde archivos.
    """

    # * Funcion para retornar el maximo de fecha de una tabla junto con el tipo de dato
    def get_max_n_type(bbdd:str,table_name:str,column_name:str,order_mode:str) -> str:
        """
        Funcion para obtener el ultimo dato de una columna en especifica de la tabla proporcionada de manera ascendente o desendente segun parametro

        Args:
            bbdd (str): Base de datos donde se encuentra la tabla
            table_name (str): Nombre de la tabla la cual sera migrada
            column_name (str): Nombre de la columnas por la cual se filtrara la tabla
            order_mode str(str): Order en la que buscara el dato dentro de la columna (ASC,DESC)

        Returns:
            str: MySQL query lista para ejecucion formateada segundo los parametros.
        """
        with open(join_func(path_to_share,"last_row.sql"),'r') as f:
            sql = f.read()
        sql = sql.format(bbdd = bbdd, table_name = table_name, column_name = column_name,\
                        __INTERVAL_DAY_ROLL_BACK__ = __INTERVAL_DAY_ROLL_BACK__,\
                        __INTERVAL_HOUR_ROLL_BACK__ = __INTERVAL_HOUR_ROLL_BACK__,\
                        order_mode = 'DESC' if order_mode.lower() == 'desc' else 'ASC')
        return sql

    # * Funcion para leer querys y formatear en el caso que sea necesario
    def get_sql_query(file:str,table_name:str=None,fecha_inicio:str=None,fecha_fin:str=None) -> str:
        """
        Funcion provee una query de tipo select filtrada o no segun parametros dado

        Args:
            file (str): Ruta al archivo que almacena la query a ejecutar.
            fecha_inicio (str, optional): Fecha inicio a filtrar en la consulta. Defaults to execute * from the table.
            fecha_fin (str, optional): Fecha fin a filtrar en la consulta. Defaults to execute * from the table.

        Returns:
            str: Consulta de MySQL para ejecucion y obtenion de DataFrame
        """
        with open(join_func(path_to_share,f"{file}.sql"),'r') as f:
            sql = f.read()
            if fecha_inicio and fecha_fin and table_name:
                return sql.format(table_name = table_name,fecha_inicio = fecha_inicio,fecha_fin = fecha_fin)
            else:
                return sql

class TheEtl:
    """
    Clase para realizar transacciones de ETL (Extract, Transform, Load) genéricas entre instancias de MySQL.

    Esta clase proporciona métodos para ejecutar procesos ETL genéricos, obtener el último registro de una tabla y matar procesos que impiden la ejecución de reemplazos en una tabla.
    """

    # * Funcion de ETL generica
    def generic_etl(
        engine_or:Engine,
        engine_des:Engine,
        table_name_or:str,
        table_name_des:str,
        column_name:str,
        mode:str,
        fecha_inicio:datetime=None,
        fecha_fin:datetime=None) -> None:
        """
        ETL generica de entornos de MySQL a MySQL para generar espejos de tablas en los servidores de Big Data

        Args:
            engine_or (Engine): Motor de MySQL de la instancia origen.
            engine_des (Engine): Motor de MySQL de la instancia destino.
            table_name_or (str): Nombre de la tabla que se quiere migrar en la instancia de MySQL de origen.
            table_name_des (str): Nombre de la tabla que se quiere migrar en la instancia de MySQL destino.
            column_name (str): Nombre de la columna por la cual se desea filtrar
            mode (str): Modo de insercion en la tabla destino.
            fecha_inicio (datetime, optional): Filtro de fecha inicio o id desde donde se hara el filtro. Defaults to None.
            fecha_fin (datetime, optional): Filtro de fecha de fin o id desde donde se hara el filtro. (Defaults: columns type date: 2024-04-01 -> type datetime: 2024-04-01 00:00:00 -> type int: 1).
        """
        ini = time.time()
        try:
            if (fecha_inicio == '*' or fecha_fin == '*') or (not fecha_inicio and not fecha_fin):
                sql = f"SELECT * FROM `{table_name_or}`;"
            elif fecha_inicio and fecha_fin:
                sql = f"SELECT * FROM `{table_name_or}` WHERE `{column_name}` BETWEEN '{fecha_inicio}' AND '{fecha_fin}';"

            logging.getLogger("user").debug(sql)
            logging.getLogger("user").info(f"[ START: origin: {DatabaseConnections.obtain_info_from_engine(engine_or,'info')} -> target: {DatabaseConnections.obtain_info_from_engine(engine_des,'info')} ]")
            logging.getLogger("user").info(f"[ TABLE: {table_name_or} >> column '{column_name}' range: ( {fecha_inicio if fecha_inicio else '*'} - {fecha_fin if fecha_fin else '*'} ) ]")

            with engine_or.connect() as conn_or:
                df = pd.read_sql(text(sql),conn_or)
            logging.getLogger("user").debug(f"Dataframe obtenido -> {df.shape[0]} registros")
            if not df.empty:
                for i in list(df.select_dtypes(include=['timedelta64']).columns):
                    df[i] = df[i].astype(str).map(lambda x: x[7:])
                TheEtl.kill_processes(engine_des,table_name_des)
                with engine_des.connect() as conn_des:
                    tabla_real = Table(table_name_des, MetaData(), autoload_with = engine_des if str(DatabaseConnections.obtain_info_from_engine(engine_or,'ip')) not in big_data_servers else engine_or) # type: ignore
                    if mode.lower() == 'truncate':
                        logging.getLogger("user").debug(f"Mode: {mode} -> Truncando e insertando datos en tabla: {table_name_des}")
                        conn_des.execute(text(f"TRUNCATE `{table_name_des}`;"))
                        df.to_sql(table_name_des, conn_des, if_exists='append',index=False,chunksize=__CHUNKSIZE_TO_INSERT__)
                    elif mode.lower() == 'delete':
                        logging.getLogger("user").debug(f"Mode: {mode} -> Eliminando e insertando datos en tabla: {table_name_des}")
                        conn_des.execute(text(f"DELETE FROM `{tabla_real.name}` WHERE `{column_name}` BETWEEN '{fecha_inicio}' AND '{fecha_fin}';"))
                        df.to_sql(table_name_des, conn_des, if_exists='append',index=False,chunksize=__CHUNKSIZE_TO_INSERT__)
                    elif mode.lower() == 'replace':
                        columnas_nuevas = [Column(c.name, c.type) for c in tabla_real.c]
                        tmp = Table(f"{table_name_des}_tmp", MetaData(), *columnas_nuevas)
                        tmp.drop(bind = engine_des,checkfirst=True)
                        tmp.create(bind = engine_des)
                        logging.getLogger("user").debug(f"Insertando datos en tabla temporal: {table_name_des}_tmp")
                        df.to_sql(f"{table_name_des}_tmp", conn_des, if_exists='append',index=False,chunksize=__CHUNKSIZE_TO_INSERT__)
                        logging.getLogger("user").debug(f"Ejecutando replace en: {table_name_des}")
                        conn_des.execute(text(f"REPLACE INTO `{tabla_real.name}` SELECT * FROM `{tmp.name}`;"))
                        tmp.drop(bind = engine_des)
                logging.getLogger("user").info(f"[ SUCCESS -> {table_name_des} >> {mode} >> date range: ( {fecha_inicio} - {fecha_fin} ) >> DataFrame info: {df.shape[0]} rows with {df.shape[1]} columns  >> {time.time()-ini:.2f} sec ]\n")
            else:
                logging.getLogger("user").info(f"[ EMPTY DATAFRAME: {table_name_or} ]\n")
        except ValueError as e:
            logging.getLogger("dev").error(f"{e} -> {table_name_or} >> {DatabaseConnections.obtain_info_from_engine(engine_or,'info')}")

    # * Funcion para obtener el ultimo registro filtrando dentro de una tabla y columna especifica
    def get_last_row(table_name:str,column_name:str,engine_des:Engine,engine_or:Engine) -> str:
        """
        Obtiene el ultimo registro (Maximo) almacenado dentro de una tabla especifica filtrando por la columna asignada en el destino. Si no existe la tabla en el destino obtendra el minimo en el origen

        Args:
            table_name (str): Nombre de la tabla
            column_name (str): Columna asiganada para filtrar la información
            ip (str): IP de instancia de MySQL donde se revisara la tabla

        Returns:
            str: Ultimo registro dentro de la tabla, sea un tipo fecha hora o id
        """
        bbdd = DatabaseConnections.obtain_info_from_engine(engine_des,"bbdd")
        table_exists = table_name in inspect(engine_des).get_table_names(schema=bbdd)
        if table_exists:
            sql = ReadFiles.get_max_n_type(bbdd,table_name,column_name,"DESC")
            with engine_des.connect() as conn:
                df = pd.read_sql(text(sql),conn)
            last_row, column_type = df.iloc[0,0] ,df.iloc[0,1]
            logging.getLogger("user").debug(f"Last data in {table_name}: {last_row} -> type: {column_type}")
        else:
            tabla_real = Table(table_name, MetaData(), autoload_with = engine_or if str(DatabaseConnections.obtain_info_from_engine(engine_or,'ip')) not in big_data_servers else engine_des) # type: ignore
            tabla_real.create(bind=engine_des,checkfirst=True)
            bbdd = DatabaseConnections.obtain_info_from_engine(engine_or,"bbdd")
            sql = ReadFiles.get_max_n_type(bbdd,table_name,column_name,"ASC")
            with engine_or.connect() as conn:
                df = pd.read_sql(text(sql),conn)
            last_row, column_type = df.iloc[0,0] ,df.iloc[0,1]
            logging.getLogger("user").debug(f"Last data in {table_name}: {last_row} -> type: {column_type}")
            logging.getLogger("user").info(f"[ Table {table_name} does not exists in target. Executing since {last_row} ]")
        return last_row

    # * Funcion para matar querys que impiden la ejecucion del replace sobre una tabla
    def kill_processes(engine_des:Engine,table_name:str) -> None:
        """
        Funcion para matar querys que esten obstruyendo la insercion en la tabla destino

        Args:
            engine_des (Engine): Motor de MySQL en donde se mataran querys toxicas.
            table_name (str): Nombre de la tabla destino
        """
        with open(join_func(path_to_share,"kill_query.sql"),'r') as f:
            kill = f.read()
        usuario = DatabaseConnections.obtain_info_from_engine(engine_des,"user")
        kill_query = kill.format(usuario=usuario,table_name=table_name)
        logging.getLogger("user").debug(f"Matando querys toxicas")
        try:
            with engine_des.connect() as conn_des:
                df = pd.read_sql(text(kill_query),conn_des)
            ids = df['id'].tolist()
            for i in ids:
                with engine_des.connect() as conn_des:
                    try:
                        conn_des.execute(text(f"KILL {i}"))
                    except:
                        continue
                logging.getLogger("dev").error(f"Matando : {i}")
        except ValueError as e:
            logging.getLogger("dev").error(e)
            with engine_des.connect() as conn_des:
                df = pd.read_sql(text(kill_query),conn_des)
            ids = df['id'].tolist()
            for i in ids:
                with engine_des.connect() as conn_des:
                    try:
                        conn_des.execute(text(f"KILL {i}"))
                    except:
                        continue
                logging.getLogger("dev").error(f"Matando : {i}")

class TheMetadata:
    """
    Clase que gestiona la metadata de los archivos JSON en una base de datos.
    """

    # * Funcion para insertar los archivos JSON en la tabla metadata del administrador
    def import_json() -> None:
        """
        Funcion para importar la metadata de los archivos JSON en la tabla matriz del adminitrador del desarrollo
        """
        data_to_run:dict[str:str] = {
            "data_to_run_dia_vencido": "Dia vencido",
            "data_to_run_hora_a_hora": "Hora a hora"
        }
        dfs = [json_normalize(TheExecution.data_to_run(key)).assign(ejecucion=value)
            for key, value in data_to_run.items()]
        df_concat = pd.concat(dfs, axis=0).drop(columns=['cid'])
        engine_dw = DatabaseConnections.mysql_engine(admin_ip,admin_port,admin_bbdd) # type: ignore
        with engine_dw.connect() as conn_dw:
            conn_dw.execute(text(ReadFiles.get_sql_query("create_metadata_table")))
            conn_dw.execute(text("DROP TABLE IF EXISTS tb_metadata_espejos_de_tablas_tmp;"))
            df_concat.to_sql("tb_metadata_espejos_de_tablas_tmp", conn_dw, if_exists="append", index=False, chunksize=10)
            conn_dw.execute(text("REPLACE INTO tb_metadata_espejos_de_tablas SELECT * FROM tb_metadata_espejos_de_tablas_tmp;"))
            conn_dw.execute(text("DROP TABLE tb_metadata_espejos_de_tablas_tmp;"))
        logging.getLogger("user").info(f"[ SUCCESS -> JSON objects imported >> {DatabaseConnections.obtain_info_from_engine(engine_dw,'info')} ]")

    # * Funcion para exportar los archivos JSON de las tablas metadata del administrador
    def export_json() -> None:
        """
        Funcion para exportar la informacion en el formato json indicado segun la tabla del adminitrador ubicada en la instancia MySQL seleccionada
        """
        data_to_run = {
            "Dia vencido" :"data_to_run_dia_vencido",
            "Hora a hora" :"data_to_run_hora_a_hora"
        }
        for i in data_to_run.keys():
            with open(join_func(path_to_share,"createJSON.sql"),'r') as f:
                get_json = f.read()
            get_json = get_json.format(ejecucion=i)
            engine_dw = DatabaseConnections.mysql_engine(admin_ip,admin_port,admin_bbdd) # type: ignore
            with engine_dw.connect() as conn_dw:
                df = pd.read_sql(text(get_json),conn_dw)
            df.to_json(join_func(path_to_data,f"{data_to_run.get(i)}.json"),orient="records",index=False,indent=2,force_ascii=False)
            logging.getLogger("user").info(f"[ SUCCESS -> JSON objects exported {i} >> {DatabaseConnections.obtain_info_from_engine(engine_dw,'info')} ]")

class TheExecution:
    """
    Clase que gestiona la ejecución de procesos ETL basados en archivos JSON y parámetros del sistema.
    """

    # * Funcion de mostrar ayuda
    def show_help() -> None:
        """
        Funcion que abre la documentacion sobre la ejecucion del script
        """
        with open(join_func(path_to_docs,"documentation.txt"),'r') as file:
            print(file.read())

    # * Funcion para leer el archivo .json con las tablas a ejecutar
    def data_to_run(file:str) -> json:
        """
        Lectura de archivo JSON con los elementos a ejecutar en proceso de ETL
        Args:
            file (Engine): Nombre del archivo .JSON que se va a leer..
        Returns:
            json: Cadena de texto tipo json con los valores a ejecutar
        """
        with open(join_func(path_to_data,f'{file}.json')) as file:
            return json.load(file)

    # * Funcion para listar tablas con su respectivo cid 
    def list_cid_tables() -> None:
        """
        Lista los elementos almacenados en los archivos JSON.
        """
        print(f"\n[ {'Table':^46} |{'origin (IP:Port) -> target (IP:Port)':^45}| {'Column Type':12} |{'CID':^8}|{'Mode':^17}|{'File':^17}]\n[{'-'*154}]")
        [ print(f"[ {i['table_name_or'] if len(str(i['table_name_or'])) > 0 else 'None':-<47}|{i['ip_or'] if len(str(i['ip_or'])) > 0 else 'None':->16}:{i['port_or'] if len(str(i['port_or'])) > 0 else 'None':-<5}->{i['ip_des'] if len(str(i['ip_des'])) > 0 else 'None':->12}:{i['port_des'] if len(str(i['port_des'])) > 0 else 'None':-<8}|{i['column_type'] if len(str(i['column_type'])) > 0 else 'None':-^14}|{i['cid'] if len(str(i['cid'])) > 0 else 'None':-^8}|{i['mode'] if len(str(i['mode'])) > 0 else 'None' :-^17}|{'Hora a hora':-^17}]") for i in TheExecution.data_to_run("data_to_run_hora_a_hora")]
        [ print(f"[ {i['table_name_or'] if len(str(i['table_name_or'])) > 0 else 'None':-<47}|{i['ip_or'] if len(str(i['ip_or'])) > 0 else 'None':->16}:{i['port_or'] if len(str(i['port_or'])) > 0 else 'None':-<5}->{i['ip_des'] if len(str(i['ip_des'])) > 0 else 'None':->12}:{i['port_des'] if len(str(i['port_des'])) > 0 else 'None':-<8}|{i['column_type'] if len(str(i['column_type'])) > 0 else 'None':-^14}|{i['cid'] if len(str(i['cid'])) > 0 else 'None':-^8}|{i['mode'] if len(str(i['mode'])) > 0 else 'None' :-^17}|{'Dia vencido':-^17}]") for i in TheExecution.data_to_run("data_to_run_dia_vencido")]

    # * Funcion para obetenerlas fechas a ejecutar a partir de los argumentos del sistema
    def get_start_n_end_dates(auto_execution:bool, type_format:str, fecha_inicio:str=None, fecha_fin:str=None) -> str:
        """
        Obtiene la fecha inicio y fecha fin a partir de los argumentos del sistema dependiendo del tipo de dato

        Args:
            auto_execution (bool): Opcion para verificar si la ejecucion es masiva o por medio de cid
            type_format (str): Topo de formato de las fechas proporcionadas
            fecha_inicio (str, optional): Fecha de inicio para la ejecucion del proceso. Defaults to constanst defined at the start of the code or None.
            fecha_fin (str, optional): Fecha fin par ala ejecucion del proceso. Defaults to constanst defined at the start of the code or None.

        Returns:
            str: Retorna la fecha inicio y fecha fin para ejecucion del proceso ETl
        """
        if auto_execution == True:
            if type_format.lower() == 'datetime':
                fecha_inicio = f"{sys.argv[2]} {sys.argv[3]}" if len(sys.argv) > 4 else None
                fecha_fin    = f"{sys.argv[4]} {sys.argv[5]}" if len(sys.argv) > 6 else None
            elif type_format.lower() in ['date','id','int']:
                fecha_inicio = sys.argv[2] if len(sys.argv) > 3 else None
                fecha_fin    = sys.argv[4] if len(sys.argv) > 4 else None
        else:
            if type_format.lower() == 'datetime':
                fecha_inicio = f"{sys.argv[3]} {sys.argv[4]}" if len(sys.argv) > 4 else None
                fecha_fin    = f"{sys.argv[5]} {sys.argv[6]}" if len(sys.argv) > 6 else None
            elif type_format.lower() in ['date','id','int']:
                fecha_inicio = sys.argv[3] if len(sys.argv) > 3 else None
                fecha_fin    = sys.argv[4] if len(sys.argv) > 4 else None
        return fecha_inicio, fecha_fin

    # * Funcion de ejecucion mediante cid
    def exec_by_cid() -> None:
        """
        Funcion para ejecutar la etl de una tabla en especifico dado su CID que se pasa como argumento
        """
        avail_cid = []
        cid = int(sys.argv[2]) if len(sys.argv) > 2 else None
        data_json  = TheExecution.data_to_run("data_to_run_hora_a_hora") + TheExecution.data_to_run("data_to_run_dia_vencido")
        [ avail_cid.append(i['cid']) for i in data_json ]
        for i in data_json:
            try:
                if i['column_type'] in __DATE_FORMAT_AVAIL__:
                    if cid in avail_cid:
                        if i["cid"] == cid:
                            if str(i['mode']).lower() in __ETL_INSERT_MODE__:
                                engine_or  = DatabaseConnections.mysql_engine(i['ip_or'],i['port_or'],i['bbdd_or'])
                                engine_des = DatabaseConnections.mysql_engine(i['ip_des'],i['port_des'],i['bbdd_des'])
                                fecha_inicio, fecha_fin = TheExecution.get_start_n_end_dates(False,i['column_type'])
                                if not fecha_inicio and not fecha_fin:
                                    fecha_inicio = TheEtl.get_last_row(i['table_name_des'],i['column_name'], engine_des, engine_or)
                                    fecha_fin    = datetime.now().strftime(__DICT_DATES_FORMAT__.get(i['column_type'])) if i['column_type'] != 'id' else fecha_inicio + 50000
                                TheEtl.generic_etl(engine_or, engine_des, i['table_name_or'], i['table_name_des'], i['column_name'], i['mode'], fecha_inicio, fecha_fin)
                            else:
                                logging.getLogger("dev").error(f"Unkown method '{i['mode']}'. Valid options: '{', '.join(__ETL_INSERT_MODE__)}'")
                                sys.exit()
                    else:
                        logging.getLogger("dev").error(f"Unkown cid '{cid}'. Check availables with '--list' flag.")
                        sys.exit()
                else:
                    logging.getLogger("dev").error(f"[ Column type for table '{i['table_name_des']}' does not match with availables: '{', '.join(__DATE_FORMAT_AVAIL__)}' ]\n")
            except ValueError as e:
                logging.getLogger("dev").error(f"Error : {e}")
                continue

    # * Funcion de ejecucion de distro
    def exec_data_auto() -> None:
        """
        Funcion que ejecutara todos los elementos dentro del archivo JSON que se encuentre en ejecucion. 
        """
        file_to_execute = 'data_to_run_hora_a_hora' if __CURRENT_HOUR__ not in [__HOURS_TO_EXECUTE_EXPIRED_DAY__] else 'data_to_run_dia_vencido'
        for i in TheExecution.data_to_run(file_to_execute):
            try:
                if i['column_type'] in __DATE_FORMAT_AVAIL__:
                    if str(i['mode']).lower() in __ETL_INSERT_MODE__:
                        engine_or  = DatabaseConnections.mysql_engine(i['ip_or'],i['port_or'],i['bbdd_or'])
                        engine_des = DatabaseConnections.mysql_engine(i['ip_des'],i['port_des'],i['bbdd_des'])
                        fecha_inicio, fecha_fin = TheExecution.get_start_n_end_dates(True,i['column_type'])
                        if not fecha_inicio and not fecha_fin:
                            fecha_inicio = TheEtl.get_last_row(i['table_name_des'],i['column_name'], engine_des, engine_or)
                            fecha_fin    = datetime.now().strftime(__DICT_DATES_FORMAT__.get(i['column_type'])) if i['column_type'] != 'id' else fecha_inicio + 50000
                            TheEtl.generic_etl(engine_or, engine_des, i['table_name_or'], i['table_name_des'], i['column_name'], i['mode'], fecha_inicio, fecha_fin)
                    else:
                        logging.getLogger("dev").error(f"[ {i['table_name_des']}@{i['ip_des']} >> cid: {i['cid']} with unkown defined method of insertion '{i['mode']}'. Valid options: {__ETL_INSERT_MODE__} ]\n")
                        continue
                else:
                    logging.getLogger("dev").error(f"[ Column type for table '{i['table_name_des']}' does not match with availables: '{', '.join(__DATE_FORMAT_AVAIL__)}' ]\n")
            except ValueError as e:
                logging.getLogger("dev").error(f"{i['ip_des']} >> {i['table_name_or']} >> {e}\n")
                continue

    # * Main de ejecucion dependiente del diccionario
    def execution(action:str) -> None:
        """
        Funcion main para la ejecucion mediante banderas del diccionario __DICT_ACTIONS__.
        Args:
            action (str): Bandera de ejecucion que se pasa como argumento del sistema
        """
        if action in TheExecution.__DICT_ACTIONS__:
            if isinstance(TheExecution.__DICT_ACTIONS__[action], list):
                for func in TheExecution.__DICT_ACTIONS__[action]:
                    try:
                        func()
                    except ValueError as e:
                        logging.getLogger("dev").error(e)
                    finally:
                        continue
            else:
                try:
                    TheExecution.__DICT_ACTIONS__[action]()
                except ValueError as e:
                    logging.getLogger("dev").error(e)
                finally:
                    pass
        else:
            logging.getLogger("dev").error("Unknown action provided, use '--help' flag to check availables")

    # * Diccionario de banderas para ejecucion por consola
    __DICT_ACTIONS__:dict[str:str]= {
        '--help'         : show_help,         # TODO: Muestra la ayuda para ejecucion 
        '-h'             : show_help,         # * """"""
        '--list'         : list_cid_tables,   # TODO: lista las tablas que se estan migrando automaticamente (a dia vencido y hora a hora)
        '-l'             : list_cid_tables,   # * """"""
        '--cid'          : exec_by_cid,       # TODO: ejecuta una tabla en especifico de las tablas que estan automaticas cid
        '-c'             : exec_by_cid,       # * """"""
        '--execute'      : [exec_data_auto],  # TODO: Ejecuta una lista de funciones (las previamente menciondas en el diccionario)
        '-exe'           : [exec_data_auto]   # * """"""
    }

if __name__ == '__main__':
    pass # ! Todo codigo bajo esta linea no debera existir para los commits
