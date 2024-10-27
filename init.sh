#!/bin/bash

# Get the system user
sysuser=$(whoami)

# Activate the virtual environment
source "/home/$USER/.venv/bin/activate"

# Change directory and install requirements
cd "~/repos/pipe-dmeyf24/" && pip install -r requirements.txt

# Check if the file exists
if [ -f "/home/$sysuser/buckets/b1/datasets/compe_02.duckdb" ]; then
    # If the file exists, copy it to the target directory
    cp "/home/$sysuser/buckets/b1/datasets/compe_02.duckdb" "~/repos/pipe-dmeyf24/duckdb/"
else
    # If the file does not exist, download it from the specified URL
    wget -P "~/repos/pipe-dmeyf24/datasets" "https://storage.googleapis.com/open-courses/dmeyf2024-b725/competencia_02_crudo.csv.gz"

    # run the script to convert the file to duckdb
    python "~/repos/pipe-dmeyf24/ingenieria-datos/crear-db.py"
fi

