#!/bin/bash

# Cek apakah virtual environment sudah ada
if [ ! -d "$VENV_DIRECTORY" ]; then
  echo "Virtual environment belum ada, membuat virtual environment..."
  # Buat virtual environment
  python3 -m venv "$ETL_DIRECTORY"
  
  # Aktifkan virtual environment
  source "$VENV_DIRECTORY"

  # Instal dependencies
  pip install -r "$ETL_DIRECTORY/requirements.txt"
else
  echo "Virtual environment sudah ada, melewati pembuatan dan instalasi dependencies."
  
  # Aktifkan virtual environment
  source "$VENV_DIRECTORY"
fi

#Locate Directory
cd "$ETL_DIRECTORY"

# Ensure log directory exists
mkdir -p "log"

# Logging simple
dt=$(date '+%d/%m/%Y %H:%M:%S');
echo "Luigi Started at ${dt}" >> "log/luigi-info.log"
echo "Start Luigi ETL Pipeline Process at ${dt} "

# Run Luigi Visualizer
luigid --port 8082 > /dev/null 2> /dev/null &

# Set Python script
PYTHON_SCRIPT="etl_luigi.py"

# Run Python script
python "$PYTHON_SCRIPT" >> "log/logfile.log" 2>&1

dt=$(date '+%d/%m/%Y %H:%M:%S');
echo "End Luigi ETL Pipeline Process ${dt}"
