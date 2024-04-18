# Case Study Notes
The case study uses Pandas and Dask to perform:
* Simple transformations to some trade executions
* Enrichment with referential data
* Enrichment with market data
* Simple Slippage analysis


# Installation
You will need to create a virtual environment with Python 3.11+ and then install the libraries defined in requirements.txt.

I like to use micromamba (https://mamba.readthedocs.io/en/latest/index.html) which is a super-fast alternative to conda.
Using micromamba to create a virtual environment is similar to the conda:
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
* I use the Dask Futures API to parallelize the enrichment of market data for each execution. 
* Logs are streamed to the console and created in the src/logs dir.


# Running Analysis
To run the analysis you need:
1. To be in the src directory
2. To activate your virtual environment 

Then run the following:
```commandline
python tca_analysis.py
```

The output file enriched_executions.parquet will be produced in the data dir

You can explore the parquet files using Jupyter Lab by running the following whilst in the top level directory
```commandline
jupyter lab
```
This will start a browser which will allow you to open the exploration.ipynb notebook. Note this notebook also uses Dask to read in the parquet files.