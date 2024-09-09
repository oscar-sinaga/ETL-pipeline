#!/bin/bash

#Locate Directory
cd "/mnt/h/My Drive/pacmann/Intro to DE/ETL-pipeline"

# Ensure log directory exists
mkdir -p "log"

# Logging simple
dt=$(date '+%d/%m/%Y %H:%M:%S');
echo "Luigi Started at ${dt}" >> "log/luigi-info.log"
echo "Start Luigi ETL Pipeline Process at ${dt} "

# Virtual Environment Path
VENV_PATH="/home/oscar-sinaga/venv_etl/bin/activate"

# Activate venv
source "$VENV_PATH"

# Run Luigi Visualizer
luigid --port 8082 > /dev/null 2> /dev/null &

# Set Python script
PYTHON_SCRIPT="etl_luigi.py"

# Run Python script
python "$PYTHON_SCRIPT" >> "log/logfile.log" 2>&1

dt=$(date '+%d/%m/%Y %H:%M:%S');
echo "End Luigi ETL Pipeline Process ${dt}"
