"""
Modulo de despliegue para configurar la aplicacion
en nuevos dispositivos
"""
import sys
import ctypes
import platform
import paths

class TheDeployment:
    """Clase que configura los requerimientos para ejecutar
    el script correctamente
    """

    # *  Funcion que configura el logger basado en la maqueta
    def build_proyect(self) -> None:
        """Crea el archivo final de configuración logger y otros archivos de configuración
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
    def create_hyperlinks(os:str) -> None:
        """Pregunta al usuario si desea crear los archivos de ejecucion
        y actualizacion en el escritorio como acceso directo
        """
        buf = ctypes.create_unicode_buffer(512)
        ctypes.windll.shell32.SHGetFolderPathW(None, 0x00000000, None, 0, buf)
        desktop_path = buf.value

        if os == 'Linux':
            pass
        elif os == 'Windows':
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
                    sys.exit(1)
                else:
                    print("No match, try again... Use [Y/n]")
        else:
            print("Unsupported OS")

if __name__ == '__main__':
    TheDeployment().build_proyect()
    TheDeployment().create_hyperlinks(os=platform.system())
    print("┌───────────────────────────────────────┐")
    print("│                SUCCESS                │")
    print("└───────────────────────────────────────┘")
