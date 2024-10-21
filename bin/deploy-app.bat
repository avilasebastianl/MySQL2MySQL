@echo off
echo Deploying ETL MySQL2MySQL

set current_path=%cd%
cd %current_path%
cd ..

type docs\devs.txt
mkdir config
echo config folder created
mkdir data
echo data folder created
mkdir logs
echo logs folder created

where py >nul 2>&1
if %errorlevel%==0 (
    set PYTHON_COMMAND=py
) else (
    where python >nul 2>&1
    if %errorlevel%==0 (
        set PYTHON_COMMAND=python
    ) else (
        echo No Python interpreter found.
        exit /b 1
    )
)
echo Python command found: "%PYTHON_COMMAND%"

echo Creating virtual enviroment
%PYTHON_COMMAND% -m venv venv --upgrade-deps
call venv\Scripts\activate
echo Installing libraries
pip install -r requirements.txt
echo Libraries:
pip list
%PYTHON_COMMAND% src\deploy.py
%PYTHON_COMMAND% Main.pyw -h

choice /M "Do you want to exit..."
if %errorlevel%==1 (
    echo Bye...
    timeout /t 2 /nobreak 
    exit /b 0
) else (
    echo Check results...
    pause
)