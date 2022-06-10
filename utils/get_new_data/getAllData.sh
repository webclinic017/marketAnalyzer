#!/bin/bash
source /home/$(whoami)/anaconda3/etc/profile.d/conda.sh
conda activate bolsa
export PYTHONPATH=${PYTHONPATH}:../../
DIR=../../
python main_market_data.py
python main_results.py
python main_fundamentals.py
python main_fundamentals_update.py
python main_prizes.py
