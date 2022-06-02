#!/bin/bash
source /home/$(whoami)/anaconda3/etc/profile.d/conda.sh
conda activate bolsa
export PYTHONPATH=${PYTHONPATH}:../../
DIR=../../
python eventsInvesting.py >$DIR/logs/log_files/logs.txt 2>$DIR/logs/log_files/errores.txt
python forexOanda.py >$DIR/logs/log_files/logs.txt 2>$DIR/logs/log_files/errores.txt
python savePrizes.py >$DIR/logs/log_files/logsPrizes.txt 2>$DIR/logs/log_files/erroresPrizes.txt
python getLastFundamentalResults.py >$DIR/logs/log_files/logsResults.txt 2>$DIR/logs/log_files/erroresResults.txt
python saveIndexPrizes.py >$DIR/logs/log_files/logsIndex.txt 2>$DIR/logs/log_files/erroresIndex.txt
python otros_activos.py > $DIR/logs/log_files/logs_otros_activos.txt 2> $DIR/logs/log_files/logsErrores_otros_activos.txt
python saveFundamentals.py > $DIR/logs/log_files/logsFundamentals.txt 2>$DIR/logs/log_files/erroresFundamentals.txt
