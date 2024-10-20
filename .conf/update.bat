@echo off
@REM El proposito de este archivo es para cuando tenga GUI )))
echo Updating project
cd {project_directory}
git pull
call venv\Scripts\activate
echo Updating project libraries
pip install -r requirements.txt
echo #########################################
echo #            Update complete            #
echo #########################################

choice /M "Do you want to exit..."
if %errorlevel%==1 (
    echo Bye...
    timeout /t 3 /nobreak 
    exit /b 0
) else (
    echo Check results...
    pause
)