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

echo "Creating virtual environment"
python3 -m venv venv

source venv/bin/activate

echo "Installing libraries"
pip install -r requirements.txt

echo "Libraries:"
pip list

python3 src/deploy.py
python3 Main.pyw -h

read -p "Do you want to exit? (y/n): " choice
if [ "$choice" == "y" ] || [ "$choice" == "Y" ]; then
    echo "Bye..."
    sleep 2
    exit 0
else
    echo "Check results..."
    read -p "Press Enter to continue..."
fi
