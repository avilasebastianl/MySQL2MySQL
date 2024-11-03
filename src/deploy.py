"""
Modulo de despliegue para configurar la aplicacion
en nuevos dispositivos
"""

import ctypes
import platform
import paths

class TheDeployment:
    """
    Clase que configura los requerimientos para ejecutar
    el script correctamente

    Methods:
    -------
    build_proyect(self): Crea el archivo final de configuración
    logger y otros archivos de configuración.

    create_hyperlinks(self): Pregunta al usuario si desea crear los archivos de ejecucion
    y actualizacion en el escritorio como acceso directo.
    """

    # *  Funcion que configura el logger basado en la maqueta
    def build_proyect(self) -> None:
        """
        Crea el archivo final de configuración logger y otros archivos de configuración.
        Este método utiliza una maqueta de archivo para generar configuraciones en 
        las rutas específicas y guarda los archivos configurados.
        """
        print("Creating configuration files...")
        files: dict[str, str] = {
            'logger.yml': 'logger.yml',
            'credentials.py': 'credentials.py',
            'data_to_run_hora_a_hora.json': 'data_to_run_maket.json',
            'data_to_run_dia_vencido.json': 'data_to_run_maket.json'
        }
        for file, source_file in files.items():
            file_path = paths.join_func(paths.config_path, source_file)
            with open(file_path, 'r', encoding="latin-1") as logg:
                build = logg.read()
            if file == 'logger.yml':
                build = build.format(path_to_logs=paths.path_to_logs)
            output_path = paths.join_func(
                paths.path_to_data if file.endswith('.json') else paths.path_to_config,
                file
            )
            with open(output_path, 'w', encoding="latin-1") as logg:
                logg.write(build)
            print(f"{file} configuration file ready.")

    # * Funcion para crear los archivos de ejecucion y actualizacion en el escritorio
    def create_hyperlinks(self) -> None:
        """
        Pregunta al usuario si desea crear los archivos de ejecucion
        y actualizacion en el escritorio como acceso directo.
        Este método pregunta al usuario si desea crear accesos directos de archivos `.bat`
        en el escritorio para facilitar la ejecución y actualización del proyecto en sistemas
        operativos Windows.
        """
        buf = ctypes.create_unicode_buffer(512)
        ctypes.windll.shell32.SHGetFolderPathW(None, 0x00000000, None, 0, buf)
        desktop_path = buf.value

        operative_system = platform.system()
        if operative_system == 'Linux':
            print("Operative system not integrated YET.")
        elif operative_system == 'Windows':
            while True:
                files = {'execute.bat': 'MySQL2MySQL.bat','update.bat': 'Update-MySQL2MySQL.bat'}
                link = input("Do you want to have desktop shortcut for execution? [Y/n]: ")
                if link.lower() in ['y','s','yes','si']:
                    for i in files:
                        with open(paths.join_func(paths.path_to_docs, i),'r',
                                encoding="latin-1") as cons:
                            bat = cons.read()
                        bat = bat.format(project_directory=paths.project_directory)
                        with open(paths.join_func(desktop_path, files.get(i)),'w',
                                encoding="latin-1") as cons:
                            cons.write(bat)
                    print("Shortcuts were created...")
                    break
                if link.lower() in ['n','no']:
                    print("No shortcut was created...")
                    break
                else:
                    print("No match, try again... Use [Y/n]")
        else:
            print("Unsupported OS")

if __name__ == '__main__':
    deploy = TheDeployment()
    deploy.build_proyect()
    deploy.create_hyperlinks()
    print("┌───────────────────────────────────────┐")
    print("│                SUCCESS                │")
    print("└───────────────────────────────────────┘")
