#!/bin/bash
source /home/$(whoami)/anaconda3/etc/profile.d/conda.sh
conda activate bolsa
python eventsInvesting.py >../logs/logs.txt 2>../logs/errores.txt
python forexOanda.py >../logs/logs.txt 2>../logs/errores.txt
python savePrizes.py >../logs/logsPrizes.txt 2>../logs/erroresPrizes.txt
#python saveFundamentals.py >../logs/logsFundamentals.txt 2>../logs/erroresFundamentals.txt
python getLastFundamentalResults.py >../logs/logsResults.txt 2>../logs/erroresResults.txt
python saveIndexPrizes.py >../logs/logsIndex.txt 2>../logs/erroresIndex.txt
python otros_activos.py > ../logs/logs_otros_activos.txt 2> ../logs/logsErrores_otros_activos.txt
