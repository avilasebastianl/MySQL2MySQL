"""
Modulo de rutas constantes de cada carpeta del proyectos
"""
import os
from pathlib import Path

script_directory:Path = Path(__file__).parent.absolute()
project_directory:Path = script_directory.parent
path_to_config:Path = project_directory / "config"
path_to_logs:Path = project_directory / "log"
path_to_data:Path = project_directory / "data"
path_to_docs:Path = project_directory / "docs"
path_to_bin:Path = project_directory / "bin"
path_to_sql:Path = project_directory / "sql"
config_path:Path = project_directory / ".conf"
path_to_share:Path = path_to_sql / "share"

def join_func(prefix:Path,sufix:str) -> Path:
    """Funcion para unir strings y retornar una ruta en especifico

    Args:
        prefix (Path): Ruta definida a una carpeta.
        sufix (str): Nombre del archivo a unir.

    Returns:
        Path: Ruta final del archivo
    """
    return Path(os.path.join(prefix,sufix))

if __name__ == '__main__':
    pass # ! No borre esta linea de codigo, realice pruebas abajo
