#!/bin/bash
source /home/$(whoami)/anaconda3/etc/profile.d/conda.sh
conda activate bolsa
export PYTHONPATH=${PYTHONPATH}:../../
python main_splits.py
python main_market_data.py
python main_results.py
python main_fundamentals.py
python main_fundamentals_update.py
python main_prizes.py
cd datosProcesados
export PYTHONPATH=${PYTHONPATH}:../../../
python update_precios_h5s.py
python ratios_earnings.py
