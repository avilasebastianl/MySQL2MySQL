import os
import sys
import glob 
import time
import yaml
import json
import logging
import datetime
import numpy as np
import pandas as pd
import logging.config
from paths import *
from pandas import DataFrame
from urllib.parse import quote
from datetime import timedelta, datetime
from sqlalchemy.engine import reflection
from sqlalchemy.engine.url import make_url
from sqlalchemy import Table, MetaData, create_engine, Column,text,Engine,inspect
sys.path.append(path_to_config)
from credentials import * # type: ignore

"""
# ! Primer desarrollador: @avilasebastianl
# ! Fecha de inicio de desarrollo: 2024-07-13
# ! Fecha de inicio de produccion: None
"""

# TODO: Configuracion de logger
with open(os.path.join(path_to_config,"logger.yml")) as f:
    logging.config.dictConfig(yaml.safe_load(f))

# TODO: Declaracion de constantes
__HOURS_TO_EXECUTE_EXPIRED_DAY__ = [4,6] # * Numero entero de las horas en la se ejecutara el archivo dia_vvencido.json
__ETL_INSERT_MODE__              = ['delete','truncate','replace'] # * Modos de insercion de datos en la tabla destino
__CURRENT_HOUR__                 = int(datetime.now().strftime("%H"))
__BASE_DATETIME_TO_EXECUTE__     = "2024-04-01 00:00:00" 
__BASE_DATE_TO_EXECUTE__         = "2024-04-01"
__CHUNKSIZE_TO_INSERT__          = 150000
__MAX_ID_TO_FILTER__             = 50000
__INTERVAL_DAY_ROLL_BACK__       = 7 # * Dias calendario a reejecutar (cuando la columna es tipo date)
__INTERVAL_HOUR_ROLL_BACK__      = 3 # * Horas a reejecutar (cuando la columna es tipo datetime)

dict_dates_format = {
    "datetime" : "%Y-%m-%d %H:%M:%S",
    "date"     : "%Y-%m-%d"
}

class database_connections:

    # * Funcion para retornar cadenas de conexion a MySQL
    def mysql_engine(ip:str, port:str, bbdd:str) -> Engine:
        """Creacion de motor de MySQL para generar conexiones y acceso a metadata segun la base de datos obtenida

        Args:
            ip (str): IP de instancia de MySQL destino de conexion
            port (str): Puerto de instancia de MySQL de destino de conexion
            bbdd (str): Base de datos donde se dea hacer la conexion

        Returns:
            Engine: Motor de MySQL. Solo se accede a la metadata de la base de datos ingresada.
        """    
        return create_engine(f'mysql+pymysql://{dict_user.get(ip)}:{quote(dict_pwd.get(ip))}@{ip}:{port}/{bbdd}',pool_recycle=9600,isolation_level="AUTOCOMMIT") # type: ignore

    # * Funcion para obetener el usuario con el cual se hizo la url del motor de MySQL
    def obtain_info_from_engine(engine:Engine, to_extract:str) -> str:
        """Extrae el nombre del usuario con el cual se crea de datos del string del engine.

        Args:
            engine (Engine): Motor de MySQL.

        Returns:
            str: Nombre de la base de datos.
        """
        if to_extract in ['usuario','user']:
            return make_url(str(engine.url)).username
        elif to_extract in ['info','information']:
            return f"{make_url(str(engine.url)).host}:{make_url(str(engine.url)).port}@{make_url(str(engine.url)).database}"
        elif to_extract in ['ip','IP']:
            return f"{make_url(str(engine.url)).host}:{make_url(str(engine.url)).port}@{make_url(str(engine.url)).database}"
        elif to_extract in ['BBDD','bbdd']:
            return f"{make_url(str(engine.url)).database}"
    
class read_files:

    # * Funcion para retornar el maximo de fecha de una tabla junto con el tipo de dato
    def get_max_n_type(bbdd:str, table_name:str, column_name:str, order_mode:str) -> str:
        """Funcion para obtener el ultimo dato de una columna en especifica de la tabla proporcionada de manera ascendente o desendente segun parametro

        Args:
            bbdd (str): Base de datos donde se encuentra la tabla
            table_name (str): Nombre de la tabla la cual sera migrada
            column_name (str): Nombre de la columnas por la cual se filtrara la tabla
            order_mode str(str): Order en la que buscara el dato dentro de la columna (ASC,DESC)

        Returns:
            str: MySQL query lista para ejecucion formateada segundo los parametros.
        """        
        with open(os.path.join(path_to_sql,"last_row.sql"),'r') as f:
            sql = f.read()
        sql = sql.format(bbdd = bbdd, table_name = table_name, column_name = column_name,\
                        __INTERVAL_DAY_ROLL_BACK__ = __INTERVAL_DAY_ROLL_BACK__,\
                        __INTERVAL_HOUR_ROLL_BACK__ = __INTERVAL_HOUR_ROLL_BACK__,
                        order_mode = 'DESC' if order_mode.lower() == 'desc' else 'ASC')
        return sql
    
    # * Funcion para leer querys y formatear en el caso que sea necesario
    def get_sql_query(file:str, table_name:str, fecha_inicio:str=None, fecha_fin:str=None) -> str:
        """Funcion provee una query de tipo select filtrada o no segun parametros dado

        Args:
            file (str): Ruta al archivo que almacena la query a ejecutar.
            fecha_inicio (str, optional): Fecha inicio a filtrar en la consulta. Defaults to execute * from the table.
            fecha_fin (str, optional): Fecha fin a filtrar en la consulta. Defaults to execute * from the table.

        Returns:
            str: Consulta de MySQL para ejecucion y obtenion de DataFrame
        """        
        with open(os.path.join(path_to_sql,f"{file}.sql"),'r') as f:
            sql = f.read()
            if fecha_inicio and fecha_fin:
                return sql.format(table_name = table_name,fecha_inicio = fecha_inicio,fecha_fin = fecha_fin)
            else:
                return sql

class the_etl:

    # * Funcion de ETL generica
    def generic_etl(engine_or:Engine, engine_des:Engine, table_name_or:str, table_name_des:str, column_name:str, mode:str, fecha_inicio:datetime=None, fecha_fin:datetime=None) -> None:
        """ETL generic de entornos de MySQL a MySQL para generar espejos de tablas en los servidores de Big Data

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
            logging.getLogger("user").info(f"[ START: origin: {database_connections.obtain_info_from_engine(engine_or,'info')} -> target: {database_connections.obtain_info_from_engine(engine_des,'info')} ]")
            logging.getLogger("user").info(f"[ TABLE: {table_name_or} >> column '{column_name}' range: ( {fecha_inicio if fecha_inicio else '*'} - {fecha_fin if fecha_fin else '*'} ) ]")

            with engine_or.connect() as conn_or:
                df = pd.read_sql(text(sql),conn_or)
            logging.getLogger("user").debug(f"Dataframe obtenido -> {df.shape[0]} registros")
            if not df.empty:
                for i in list(df.select_dtypes(include=['timedelta64']).columns):
                    df[i] = df[i].astype(str).map(lambda x: x[7:])
                logging.getLogger("user").debug(f"Matando querys toxicas")
                the_etl.kill_processes(engine_des,table_name_des)
                with engine_des.connect() as conn_des:
                    tabla_real = Table(table_name_des, MetaData(), autoload_with = engine_or if str(database_connections.obtain_info_from_engine(engine_or,'ip')) in __BIGDATA_SERVERS__ else engine_des) # type: ignore
                    tabla_real.create(bind=engine_des,checkfirst=True)
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
            logging.getLogger("dev").error(f"{e} -> {table_name_or} >> {database_connections.obtain_info_from_engine(engine_or,'info')}")

    # * Funcion para obtener el ultimo registro filtrando dentro de una tabla y columna especifica
    def get_last_row(table_name:str, column_name:str, engine_des:Engine, engine_or:Engine) -> str:
        """ Obtiene el ultimo registro (Maximo) almacenado dentro de una tabla especifica filtrando por la columna asignada 

        Args:
            table_name (str): Nombre de la tabla
            column_name (str): Columna asiganada para filtrar la información
            ip (str): IP de instancia de MySQl donde se revisara la tabla

        Returns:
            str: Ultimo registro dentro de la tabla, sea un tipo fecha hora o id
        """    
        bbdd = database_connections.obtain_info_from_engine(engine_des,"bbdd")
        table_exists = table_name in inspect(engine_des).get_table_names(schema=bbdd)
        if table_exists:
            sql = read_files.get_max_n_type(bbdd,table_name,column_name,"DESC")
            with engine_des.connect() as conn:
                df = pd.read_sql(text(sql),conn)
            last_row, column_type = df.iloc[0,0] ,df.iloc[0,1]
            logging.getLogger("user").debug(f"Last data in {table_name}: {last_row} -> type: {column_type}")
        else:
            sql = read_files.get_max_n_type(bbdd,table_name,column_name,"ASC")
            with engine_or.connect() as conn:
                df = pd.read_sql(text(sql),conn)
            last_row, column_type = df.iloc[0,0] ,df.iloc[0,1]
            logging.getLogger("user").debug(f"Last data in {table_name}: {last_row} -> type: {column_type}")
            logging.getLogger("user").info(f"Table {table_name} does not exists in target: executing since {last_row} ")
            print(df)
        return last_row

    # * Funcion para matar querys que impiden la ejecucion del replace sobre una tabla
    def kill_processes(engine_des:Engine, table_name:str) -> None:
        """Funcion para matar querys que esten obstruyendo la insercion en la tabla destino

        Args:
            engine_des (Engine): Motor de MySQL en donde se mataran querys toxicas.
            table_name (str): Nombre de la tabla destino
        """        
        with open(os.path.join(path_to_sql,"kill_query.sql"),'r') as f:
            kill = f.read()
        usuario = database_connections.obtain_info_from_engine(engine_des,"user")
        kill_query = kill.format(usuario=usuario,table_name=table_name)
        try:
            with engine_des.connect() as conn_des:
                df = pd.read_sql(text(kill_query),conn_des)
            ids = df['id'].tolist()                    
            for i in ids:
                with engine_des.connect() as conn_des:
                    try:
                        conn_des.execute(text(f"KILL {i}"))
                    except:
                        pass
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
                        pass
                logging.getLogger("dev").error(f"Matando : {i}")

class the_execution:

    # * Funcion de mostrar ayuda
    def show_help() -> None:
        """Funcion que abre la documentacion sobre la ejecucion del script
        """        
        with open(os.path.join(path_to_docs,"documentation.txt"),'r') as file:
            print(file.read())

    # * Funcion para leer el archivo .json con las tablas a ejecutar
    def data_to_run(file:str) -> json:
        """Lectura de archivo JSON con los elementos a ejecutar en proceso de ETL
        Args:
            file (Engine): Nombre del archivo .JSON que se va a leer..
        Returns:
            json: Cadena de texto tipo json con los valores a ejecutar
        """    
        with open(os.path.join(path_to_data,f'{file}.json')) as file:
            return json.load(file)

    # * Funcion para listar tablas con su respectivo cid 
    def list_cid_tables() -> None:
        """Lista los elementos almacenados en los archivos JSON.
        """        
        print(f"\n[ {'Table':^46} |{'origin (IP:Port) -> target (IP:Port)':^42}| {'Column Type':12} |{'CID':^8}|{'Mode':^17}|{'File':^17}]\n[{'-'*151}]")
        [ print(f"[ {i['table_name_or'] if len(str(i['table_name_or'])) > 0 else 'None':45}  | {i['ip_or'] if len(str(i['ip_or'])) > 0 else 'None':>12}:{i['port_or'] if len(str(i['port_or'])) > 0 else 'None':<5} -> {i['ip_des'] if len(str(i['ip_des'])) > 0 else 'None':>12}:{i['port_des'] if len(str(i['port_des'])) > 0 else 'None':<5} | {i['column_type'] if len(str(i['column_type'])) > 0 else 'None':^12} | {i['cid'] if len(str(i['cid'])) > 0 else 'None':^5}  | {i['mode'] if len(str(i['mode'])) > 0 else 'None' :^15} | {'Hora a hora':^15} ]") for i in the_execution.data_to_run("data_to_run_hora_a_hora")]
        [ print(f"[ {i['table_name_or'] if len(str(i['table_name_or'])) > 0 else 'None':45}  | {i['ip_or'] if len(str(i['ip_or'])) > 0 else 'None':>12}:{i['port_or'] if len(str(i['port_or'])) > 0 else 'None':<5} -> {i['ip_des'] if len(str(i['ip_des'])) > 0 else 'None':>12}:{i['port_des'] if len(str(i['port_des'])) > 0 else 'None':<5} | {i['column_type'] if len(str(i['column_type'])) > 0 else 'None':^12} | {i['cid'] if len(str(i['cid'])) > 0 else 'None':^5}  | {i['mode'] if len(str(i['mode'])) > 0 else 'None' :^15} | {'Dia vencido':^15} ]") for i in the_execution.data_to_run("data_to_run_dia_vencido")]

    # * Funcion para obetenerlas fechas a ejecutar a partir de los argumentos del sistema
    def get_start_n_end_dates(auto_execution:bool,type_format:str) -> str:
        """Obtiene la fecha inicio y fecha fin a partir de los argumentos del sistema dependiendo del tipo de dato

        Args:
            auto_execution (bool): Opcion para verificar si la ejecucion es masiva o por medio de cid
            type_format (str): Topo de formato de las fechas proporcionadas
            fecha_inicio (str, optional): Fecha de inicio para la ejecucion del proceso. Defaults to constanst defined at the start of the code or None.
            fecha_fin (str, optional): Fecha fin par ala ejecucion del proceso. Defaults to constanst defined at the start of the code or None.

        Returns:
            str: Retorna la fecha inicio y fecha fin para ejecucion del proceso ETl
        """
        if type_format.lower() == 'datetime':
            fecha_inicio = f"{sys.argv[ 2 if auto_execution == True else 3 ]} {sys.argv[ 3 if auto_execution == True else 4 ]}" if len(sys.argv) > 4 else None
            fecha_fin    = f"{sys.argv[ 4 if auto_execution == True else 5 ]} {sys.argv[ 5 if auto_execution == True else 6 ]}" if len(sys.argv) > 6 else None
        elif type_format.lower() in ['date','id']:
            fecha_inicio = sys.argv[ 2 if auto_execution == True else 3 ] if len(sys.argv) > 3 else None
            fecha_fin    = sys.argv[ 4 if auto_execution == True else 5 ] if len(sys.argv) > 5 else None
        return fecha_inicio,fecha_fin
    
    # * Funcion de ejecucion mediante cid   
    def exec_by_cid() -> None:
        """Funcion para ejecutar la etl de una tabla en especifico dado su CID que se pasa como argumento
        """        
        cid = int(sys.argv[2]) if len(sys.argv) > 2 else None
        data_json  = the_execution.data_to_run("data_to_run_hora_a_hora") + the_execution.data_to_run("data_to_run_dia_vencido")
        for i in data_json:
            try:
                if i["cid"] == cid:
                    if str(i['mode']).lower() in __ETL_INSERT_MODE__:
                        engine_or  = database_connections.mysql_engine(i['ip_or'],i['port_or'],i['bbdd_or'])
                        engine_des = database_connections.mysql_engine(i['ip_des'],i['port_des'],i['bbdd_des'])
                        fecha_inicio, fecha_fin = the_execution.get_start_n_end_dates(False,i['column_type'])
                        if not fecha_inicio and not fecha_fin:
                            fecha_inicio = the_etl.get_last_row(i['table_name_des'],i['column_name'], engine_des, engine_or)
                            fecha_fin    = datetime.now().strftime(dict_dates_format.get(i['column_type'])) if i['column_type'] != 'id' else fecha_inicio + 50000
                        the_etl.generic_etl(engine_or, engine_des, i['table_name_or'], i['table_name_des'], i['column_name'], i['mode'], fecha_inicio, fecha_fin)
                    else:
                        logging.getLogger("dev").error(f"El metodo '{i['mode']}' es desconocido. Opciones validas: '{__ETL_INSERT_MODE__}'")
                        continue
            except ValueError as e:
                logging.getLogger("dev").error(f"Error : {e}")
                continue

    # * Funcion de ejecucion de distro
    def exec_data_auto() -> None:
        """Funcion que ejecutara todos los elementos dentro del archivo JSON que se encuentre en ejecucion. 
        """        
        file_to_execute = 'data_to_run_hora_a_hora' if __CURRENT_HOUR__ in [__HOURS_TO_EXECUTE_EXPIRED_DAY__] else 'data_to_run_dia_vencido'
        for i in the_execution.data_to_run(file_to_execute):
            try:
                if str(i['mode']).lower() in __ETL_INSERT_MODE__:
                    engine_or  = database_connections.mysql_engine(i['ip_or'],i['port_or'],i['bbdd_or'])
                    engine_des = database_connections.mysql_engine(i['ip_des'],i['port_des'],i['bbdd_des'])
                    fecha_inicio, fecha_fin = the_execution.get_start_n_end_dates(True,i['column_type'])
                    if not fecha_inicio and not fecha_fin:
                        fecha_inicio = the_etl.get_last_row(i['table_name_des'],i['column_name'], engine_des, engine_or)
                        fecha_fin    = datetime.now().strftime(dict_dates_format.get(i['column_type'])) if i['column_type'] != 'id' else fecha_inicio + 50000
                        the_etl.generic_etl(engine_or, engine_des, i['table_name_or'], i['table_name_des'], i['column_name'], i['mode'], fecha_inicio, fecha_fin)
                else:
                    logging.getLogger("dev").error(f"{i['table_name_des']}@{i['ip_des']} >> cid: {i['cid']} -> Posee el metodo '{i['mode']}' y es desconocido. Opciones validas: {__ETL_INSERT_MODE__}")
                    continue
            except ValueError as e:
                logging.getLogger("dev").error(f"{i['ip_des']} >> {i['table_name_or']} >> {e}")
                continue

    # * Main de ejecucion dependiente del diccionario
    def execution(action) -> None:
        """Funcion main para la ejecucion mediante banderas del diccionario dict_actions.
        Args:
            action (_type_): Bandera de ejecucion que se pasa como argumento del sistema
        """        
        if action in the_execution.dict_actions:
            if isinstance(the_execution.dict_actions[action], list):
                for func in the_execution.dict_actions[action]:
                    try:
                        func()
                    except ValueError as e:
                        logging.getLogger("dev").error(e)
                    finally:
                        pass
            else:
                try:
                    the_execution.dict_actions[action]()
                except ValueError as e:
                    logging.getLogger("dev").error(e)
                finally:
                    pass
        else:
            logging.getLogger("dev").error("Unknown action provided")

    # * Diccionario de banderas para ejecucion por consola
    dict_actions = {
        '--help'         : show_help,               # TODO: Muestra la ayuda para ejecucion 
        '-h'             : show_help,               # * """"""
        '--list'         : list_cid_tables,         # TODO: lista las tablas que se estan migrando automaticamente (a dia vencido y hora a hora)
        '-l'             : list_cid_tables,         # * """"""
        '--cid'          : exec_by_cid,             # TODO: ejecuta una tabla en especifico de las tablas que estan automaticas cid
        '-c'             : exec_by_cid,             # * """"""
        '--execute'      : [exec_data_auto],        # TODO: Ejecuta una lista de funciones (las previamente menciondas en el diccionario)
        '-exe'           : [exec_data_auto]         # * """"""
    }

if __name__ == '__main__':

    pass
    