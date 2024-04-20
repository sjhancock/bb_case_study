# Case Study Notes
The case study uses Pandas and Dask Dataframes to perform:
* Transformations to some trade executions
* Enrichment with referential data
* Enrichment with market data
* Simple slippage analysis


# Installation
You will need to create a virtual environment with Python 3.11+ and then install the libraries defined in requirements.txt.

I like to use micromamba (https://mamba.readthedocs.io/en/latest/index.html) which is a super-fast alternative to conda.
The steps to create a virtual environment using Micromamba are similar to using conda:
```commandline
# create the envirnoment with python 3.11
micromamba create --name case_study python=3.11

# activate the environment
micromamba activate case_study

# install the libraries
micromamba install -f requirements.txt
```

# The code
The code is found in the src directory in tca_analysis.py.

* I use pandas to read in executions and refdata parquet files because they are small. 
* I use Dask Dataframes to read in marketdata because it is a bit larger
* I use the Dask Futures API to parallelize the enrichment of market data for each execution because joining the market data to the transactions is not simple.
  * Whilst the process is running you should be able to monitor the Dask Cluster activity at: http://127.0.0.1:8787/status
* Logs are streamed to the console and created in the src/logs dir.


# Running Analysis
To run the analysis you need:
1. To be in the src directory
2. To activate your virtual environment 

Then run the following:
```commandline
python tca_analysis.py
```

The output file enriched_executions.parquet will be produced in the directory data and the log file will be produced in directory src/logs.

The log will contain executions counts for each stage of the processing and timing stats at the end.

On my machine running 4 Dask workers timings were around:
```json
{
    "get_executions_sec": 0.0741429328918457,
    "data_cleansing_sec": 0.0017478466033935547,
    "data_transformation_sec": 0.010604143142700195,
    "data_calculations_sec": 12.125104904174805,
    "write_output_sec": 0.02497720718383789,
    "total_sec": 12.236586093902588
}
```

You can explore the parquet files using Jupyter Lab. 

Make sure you change your working directory to the top project level and then run:
```commandline
jupyter lab
```
This will start a browser which will allow you to open the exploration.ipynb notebook. 

Note this notebook automatically uses Dask (just for fun!) to read in the parquet files.

If you want to run the Dask Dashboard in Jupyter lab then see: https://www.youtube.com/watch?v=EX_voquHdk0


Enjoy!