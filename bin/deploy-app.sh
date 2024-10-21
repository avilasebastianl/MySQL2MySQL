#!/bin/bash
echo "Deploying ETL MySQL2MySQL"

current_path=$(pwd)
cd "$current_path"
cd ..

cat docs/devs.txt
mkdir config
echo config folder created
mkdir data
echo data folder created
mkdir log
echo logs folder created

if command -v py >/dev/null 2>&1; then
    PYTHON_COMMAND=py
elif command -v python >/dev/null 2>&1; then
    PYTHON_COMMAND=python
else
    echo "No Python interpreter found."
    exit 1
fi

echo "Python command found: $PYTHON_COMMAND"

echo "Creating virtual environment"
$PYTHON_COMMAND -m venv venv --upgrade-deps

source venv/bin/activate

echo "Installing libraries"
pip install -r requirements.txt

echo "Libraries:"
pip list

$PYTHON_COMMAND src/deploy.py
$PYTHON_COMMAND Main.pyw -h

read -p "Do you want to exit? (y/n): " choice
if [ "$choice" == "y" ] || [ "$choice" == "Y" ]; then
    echo "Bye..."
    sleep 2
    exit 0
else
    echo "Check results..."
    read -p "Press Enter to continue..."
fi
