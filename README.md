# allweather_backtest

This repo contains the full backtests resources for the all weather portfolio as described in: http://www.lazyportfolioetf.com/allocation/ray-dalio-all-weather/

A conda environment set with zipline and pyfolio should be configured beforehand:
*conda install -u Quantopian zipline

*pip install pyfolio

*pip install jupyter

Then, csv data should be downloaded using download_csv.ipynb script.

extensions.py enables ingesting downloaded csv data into zipline (the file should go under ~/.zipline folder)
