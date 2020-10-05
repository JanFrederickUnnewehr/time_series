#!/usr/bin/env python
# coding: utf-8

# <div style="width:100%; background-color: #D9EDF7; border: 1px solid #CFCFCF; text-align: left; padding: 10px;">
#       <b>Time series: Processing Notebook</b>
#       <ul>
#         <li><a href="main.ipynb">Main Notebook</a></li>
#         <li>Processing Notebook</li>
#       </ul>
#       <br>This Notebook is part of the <a href="http://data.open-power-system-data.org/time_series">Time series Data Package</a> of <a href="http://open-power-system-data.org">Open Power System Data</a>.
# </div>

# <h1>Table of Contents<span class="tocSkip"></span></h1>
# <div class="toc"><ul class="toc-item"><li><span><a href="#Introductory-Notes" data-toc-modified-id="Introductory-Notes-1"><span class="toc-item-num">1&nbsp;&nbsp;</span>Introductory Notes</a></span></li><li><span><a href="#Settings" data-toc-modified-id="Settings-2"><span class="toc-item-num">2&nbsp;&nbsp;</span>Settings</a></span><ul class="toc-item"><li><span><a href="#Set-version-number-and-recent-changes" data-toc-modified-id="Set-version-number-and-recent-changes-2.1"><span class="toc-item-num">2.1&nbsp;&nbsp;</span>Set version number and recent changes</a></span></li><li><span><a href="#Import-Python-libraries" data-toc-modified-id="Import-Python-libraries-2.2"><span class="toc-item-num">2.2&nbsp;&nbsp;</span>Import Python libraries</a></span></li><li><span><a href="#Display-options" data-toc-modified-id="Display-options-2.3"><span class="toc-item-num">2.3&nbsp;&nbsp;</span>Display options</a></span></li><li><span><a href="#Set-directories" data-toc-modified-id="Set-directories-2.4"><span class="toc-item-num">2.4&nbsp;&nbsp;</span>Set directories</a></span></li><li><span><a href="#Chromedriver" data-toc-modified-id="Chromedriver-2.5"><span class="toc-item-num">2.5&nbsp;&nbsp;</span>Chromedriver</a></span></li><li><span><a href="#Set-up-a-log" data-toc-modified-id="Set-up-a-log-2.6"><span class="toc-item-num">2.6&nbsp;&nbsp;</span>Set up a log</a></span></li><li><span><a href="#Select-timerange" data-toc-modified-id="Select-timerange-2.7"><span class="toc-item-num">2.7&nbsp;&nbsp;</span>Select timerange</a></span></li><li><span><a href="#Select-download-source" data-toc-modified-id="Select-download-source-2.8"><span class="toc-item-num">2.8&nbsp;&nbsp;</span>Select download source</a></span></li><li><span><a href="#Select-subset" data-toc-modified-id="Select-subset-2.9"><span class="toc-item-num">2.9&nbsp;&nbsp;</span>Select subset</a></span></li></ul></li><li><span><a href="#Download" data-toc-modified-id="Download-3"><span class="toc-item-num">3&nbsp;&nbsp;</span>Download</a></span><ul class="toc-item"><li><span><a href="#Automatic-download-(for-most-sources)" data-toc-modified-id="Automatic-download-(for-most-sources)-3.1"><span class="toc-item-num">3.1&nbsp;&nbsp;</span>Automatic download (for most sources)</a></span></li><li><span><a href="#Manual-download" data-toc-modified-id="Manual-download-3.2"><span class="toc-item-num">3.2&nbsp;&nbsp;</span>Manual download</a></span><ul class="toc-item"><li><span><a href="#Energinet.dk" data-toc-modified-id="Energinet.dk-3.2.1"><span class="toc-item-num">3.2.1&nbsp;&nbsp;</span>Energinet.dk</a></span></li><li><span><a href="#CEPS" data-toc-modified-id="CEPS-3.2.2"><span class="toc-item-num">3.2.2&nbsp;&nbsp;</span>CEPS</a></span></li><li><span><a href="#ENTSO-E-Power-Statistics" data-toc-modified-id="ENTSO-E-Power-Statistics-3.2.3"><span class="toc-item-num">3.2.3&nbsp;&nbsp;</span>ENTSO-E Power Statistics</a></span></li></ul></li></ul></li><li><span><a href="#Read" data-toc-modified-id="Read-4"><span class="toc-item-num">4&nbsp;&nbsp;</span>Read</a></span><ul class="toc-item"><li><span><a href="#Preparations" data-toc-modified-id="Preparations-4.1"><span class="toc-item-num">4.1&nbsp;&nbsp;</span>Preparations</a></span></li><li><span><a href="#Reading-loop" data-toc-modified-id="Reading-loop-4.2"><span class="toc-item-num">4.2&nbsp;&nbsp;</span>Reading loop</a></span></li><li><span><a href="#Save-raw-data" data-toc-modified-id="Save-raw-data-4.3"><span class="toc-item-num">4.3&nbsp;&nbsp;</span>Save raw data</a></span></li></ul></li><li><span><a href="#Processing" data-toc-modified-id="Processing-5"><span class="toc-item-num">5&nbsp;&nbsp;</span>Processing</a></span><ul class="toc-item"><li><span><a href="#Missing-data-handling" data-toc-modified-id="Missing-data-handling-5.1"><span class="toc-item-num">5.1&nbsp;&nbsp;</span>Missing data handling</a></span><ul class="toc-item"><li><span><a href="#Interpolation" data-toc-modified-id="Interpolation-5.1.1"><span class="toc-item-num">5.1.1&nbsp;&nbsp;</span>Interpolation</a></span></li></ul></li><li><span><a href="#Country-specific-calculations" data-toc-modified-id="Country-specific-calculations-5.2"><span class="toc-item-num">5.2&nbsp;&nbsp;</span>Country specific calculations</a></span><ul class="toc-item"><li><span><a href="#Calculate-onshore-wind-generation-for-German-TSOs" data-toc-modified-id="Calculate-onshore-wind-generation-for-German-TSOs-5.2.1"><span class="toc-item-num">5.2.1&nbsp;&nbsp;</span>Calculate onshore wind generation for German TSOs</a></span></li><li><span><a href="#Calculate-aggregate-wind-capacity-for-Germany-(on-+-offshore)" data-toc-modified-id="Calculate-aggregate-wind-capacity-for-Germany-(on-+-offshore)-5.2.2"><span class="toc-item-num">5.2.2&nbsp;&nbsp;</span>Calculate aggregate wind capacity for Germany (on + offshore)</a></span></li><li><span><a href="#Aggregate-German-data-from-individual-TSOs-and-calculate-availabilities/profiles" data-toc-modified-id="Aggregate-German-data-from-individual-TSOs-and-calculate-availabilities/profiles-5.2.3"><span class="toc-item-num">5.2.3&nbsp;&nbsp;</span>Aggregate German data from individual TSOs and calculate availabilities/profiles</a></span></li><li><span><a href="#Agregate-Italian-data" data-toc-modified-id="Agregate-Italian-data-5.2.4"><span class="toc-item-num">5.2.4&nbsp;&nbsp;</span>Agregate Italian data</a></span></li></ul></li><li><span><a href="#Fill-columns-not-retrieved-directly-from-TSO-webites-with--ENTSO-E-Transparency-data" data-toc-modified-id="Fill-columns-not-retrieved-directly-from-TSO-webites-with--ENTSO-E-Transparency-data-5.3"><span class="toc-item-num">5.3&nbsp;&nbsp;</span>Fill columns not retrieved directly from TSO webites with  ENTSO-E Transparency data</a></span></li><li><span><a href="#Resample-higher-frequencies-to-60'" data-toc-modified-id="Resample-higher-frequencies-to-60'-5.4"><span class="toc-item-num">5.4&nbsp;&nbsp;</span>Resample higher frequencies to 60'</a></span></li><li><span><a href="#Insert-a-column-with-Central-European-(Summer-)time" data-toc-modified-id="Insert-a-column-with-Central-European-(Summer-)time-5.5"><span class="toc-item-num">5.5&nbsp;&nbsp;</span>Insert a column with Central European (Summer-)time</a></span></li></ul></li><li><span><a href="#Create-a-final-savepoint" data-toc-modified-id="Create-a-final-savepoint-6"><span class="toc-item-num">6&nbsp;&nbsp;</span>Create a final savepoint</a></span></li><li><span><a href="#Write-data-to-disk" data-toc-modified-id="Write-data-to-disk-7"><span class="toc-item-num">7&nbsp;&nbsp;</span>Write data to disk</a></span><ul class="toc-item"><li><span><a href="#Limit-time-range" data-toc-modified-id="Limit-time-range-7.1"><span class="toc-item-num">7.1&nbsp;&nbsp;</span>Limit time range</a></span></li><li><span><a href="#Different-shapes" data-toc-modified-id="Different-shapes-7.2"><span class="toc-item-num">7.2&nbsp;&nbsp;</span>Different shapes</a></span></li><li><span><a href="#Write-to-SQL-database" data-toc-modified-id="Write-to-SQL-database-7.3"><span class="toc-item-num">7.3&nbsp;&nbsp;</span>Write to SQL-database</a></span></li><li><span><a href="#Write-to-Excel" data-toc-modified-id="Write-to-Excel-7.4"><span class="toc-item-num">7.4&nbsp;&nbsp;</span>Write to Excel</a></span></li><li><span><a href="#Write-to-CSV" data-toc-modified-id="Write-to-CSV-7.5"><span class="toc-item-num">7.5&nbsp;&nbsp;</span>Write to CSV</a></span></li></ul></li><li><span><a href="#Create-metadata" data-toc-modified-id="Create-metadata-8"><span class="toc-item-num">8&nbsp;&nbsp;</span>Create metadata</a></span><ul class="toc-item"><li><span><a href="#Write-checksums.txt" data-toc-modified-id="Write-checksums.txt-8.1"><span class="toc-item-num">8.1&nbsp;&nbsp;</span>Write checksums.txt</a></span></li></ul></li></ul></div>

# # Introductory Notes

# This Notebook handles missing data, performs calculations and aggragations and creates the output files.

# # Settings

# This section performs some preparatory steps.

# ## Set version number and recent changes
# Executing this script till the end will create a new version of the data package.
# The Version number specifies the local directory for the data <br>
# We include a note on what has been changed.

# In[27]:


version = '2019-06-05'
changes = 'Correct Error in German wind generation data'


# ## Import Python libraries

# In[51]:


# Python modules
from datetime import datetime, date, timedelta, time
import pandas as pd
import numpy as np
import logging
import json
import sqlite3
import yaml
import itertools
import os
import pytz
from shutil import copyfile
import pickle

# Skripts from time-series repository
from timeseries_scripts.read import read
from timeseries_scripts.download import download
from timeseries_scripts.imputation import find_nan, mark_own_calc
from timeseries_scripts.make_json import make_json, get_sha_hash

# Reload modules with execution of any code, to avoid having to restart
# the kernel after editing timeseries_scripts
get_ipython().run_line_magic('load_ext', 'autoreload')
get_ipython().run_line_magic('autoreload', '2')

# speed up tab completion in Jupyter Notebook
get_ipython().run_line_magic('config', 'Completer.use_jedi = False')


# Display options

#In[29]:


# Allow pretty-display of multiple variables
from IPython.core.interactiveshell import InteractiveShell
InteractiveShell.ast_node_interactivity = "all"

# Adjust the way pandas DataFrames a re displayed to fit more columns
pd.reset_option('display.max_colwidth')
pd.options.display.max_columns = 60
# pd.options.display.max_colwidth=5


# ## Set directories

# In[30]:


# make sure the working directory is this file's directory
try:
    os.chdir(home_path)
except NameError:
    home_path = os.path.realpath('.')

# optionally, set a different directory to store outputs and raw data,
# which will take up around 15 GB of disk space
#Milos: save_path is None <=> use_external_dir == False
use_external_dir = True
if use_external_dir:
    save_path = os.path.join('C:', os.sep, 'OPSD_time_series_data')
else:
    save_path = home_path

input_path = os.path.join(home_path, 'input')
sources_yaml_path = os.path.join(home_path, 'input', 'sources.yml')
areas_csv_path = os.path.join(home_path, 'input', 'areas.csv')
data_path = os.path.join(save_path, version, 'original_data')
out_path = os.path.join(save_path, version) 
temp_path = os.path.join(save_path, 'temp')
parsed_path = os.path.join(save_path, 'parsed')
for path in [data_path, out_path, temp_path, parsed_path]:
    os.makedirs(path, exist_ok=True)

# change to temp directory
os.chdir(temp_path)
os.getcwd()


# ## Chromedriver
# 
# If you want to download from sources which require scraping, download the appropriate version of Chromedriver for your platform, name it `chromedriver`, create folder `chromedriver` in the working directory, and move the driver to it. It is used by `Selenium` to scrape the links from web pages.
# 
# The current list of sources which require scraping (as of December 2018):
#   - Terna
#     - Note that the package contains a database of Terna links up to **20 December 2018**. Bu default, the links are first looked up for in this database, so if the end date of your query is not after **20 December 2018**, you won't need Selenium. In the case that you need later dates, you have two options. If you set the variable `extract_new_terna_urls` to `True`, then Selenium will be used to download the files for those later dates. If you set `extract_new_terna_urls` to `False` (which is the default value), only the recorded links will be consulted and Selenium will not be used.
#       - Note: Make sure that the database file, `recorded_terna_urls.csv`, is located in the working directory.

# In[31]:


# Deciding whether to use the provided database of Terna links
extract_new_terna_urls = False

# Saving the choice
f = open("extract_new_terna_urls.pickle", "wb")
pickle.dump(extract_new_terna_urls, f)
f.close()


# ## Set up a log

# In[32]:


# Configure the display of logs in the notebook and attach it to the root logger
logstream = logging.StreamHandler()
logstream.setLevel(logging.INFO)  #threshold for log messages displayed in here
logging.basicConfig(level=logging.INFO, handlers=[logstream])

# Set up an additional logger for debug messages from the scripts
script_logger = logging.getLogger('timeseries_scripts')
script_logger.setLevel(logging.DEBUG)
formatter = logging.Formatter(fmt='%(asctime)s %(name)s %(levelname)s %(message)s',
                              datefmt='%Y-%m-%d %H:%M:%S',)
logfile = logging.handlers.TimedRotatingFileHandler(os.path.join(temp_path, 'logfile.log'), when='midnight')
logfile.setFormatter(formatter)
logfile.setLevel(logging.DEBUG)   #threshold for log messages in logfile
script_logger.addHandler(logfile)

# Set up a logger for logs from the notebook
logger = logging.getLogger('notebook')
logger.addHandler(logfile)


# Execute for more detailed logging message (May slow down computation).

# In[33]:


logstream.setLevel(logging.DEBUG)


# ## Select timerange

# This section: select the time range and the data sources for download and read. Default: all data sources implemented, full time range available.
# 
# **Source parameters** are specified in [input/sources.yml](input/sources.yml), which describes, for each source, the datasets (such as wind and solar generation) alongside all the parameters necessary to execute the downloads.
# 
# The option to perform downloading and reading of subsets is for testing only. To be able to run the script succesfully until the end, all sources have to be included, or otherwise the script will run into errors (i.e. the step where aggregate German timeseries are caculated requires data from all four German TSOs to be loaded).

# In order to do this, specify the beginning and end of the interval for which to attempt the download.
# 
# Type `None` to download all available data.

# In[34]:


start_from_user = date(2018, 12, 31)
end_from_user = date(2019, 1, 30)


# ## Select download source

# Instead of downloading from the sources, the complete raw data can be downloaded as a zip file from the OPSD Server. Advantages are:
# - much faster download
# - back up of raw data in case it is deleted from the server at the original source
# 
# In order to do this, specify an archive version to use the raw data from that version that has been cached on the OPSD server as input. All data from that version will be downloaded - timerange and subset will be ignored.
# 
# Type `None` to download directly from the original sources.

# In[35]:


archive_version = None  # i.e. '2016-07-14'


# ## Select subset

# Read in the configuration file which contains all the required infos for the download.

# In[36]:


with open(sources_yaml_path, 'r', encoding='UTF-8') as f:
    sources = yaml.load(f.read())


# The next cell prints the available sources and datasets.<br>
# Copy from its output and paste to following cell to get the right format.<br>

# In[37]:


for k, v in sources.items():
    print(yaml.dump({k: list(v.keys())}, default_flow_style=False))


# Optionally, specify a subset to download/read.<br>
# Type `subset = None` to include all data.

# In[38]:


subset = yaml.load('''
ENTSO-E Transparency FTP:
- Actual Generation per Production Type
''')
#subset = None  # to include all sources

exclude = yaml.load('''
''')


# Now eliminate sources and datasets not in subset.

# In[39]:


with open(sources_yaml_path, 'r', encoding='UTF-8') as f:
    sources = yaml.load(f.read())
if subset:  # eliminate sources and datasets not in subset
    sources = {source_name: 
                  {k: v for k, v in sources[source_name].items()
                        if k in dataset_list}
                for source_name, dataset_list in subset.items()}
if exclude:  # eliminate sources and variables in exclude
    sources = {source_name: dataset_dict
                for source_name, dataset_dict in sources.items()
                if not source_name in exclude}

# Printing the selected sources (all of them or just a subset)
print("Selected sources: ")
for k, v in sources.items():
    print(yaml.dump({k: list(v.keys())}, default_flow_style=False))


# # Download

# This section: download data. Takes about 1 hour to run for the complete data set (`subset=None`).
# 
# First, a data directory is created on your local computer. Then, download parameters for each data source are defined, including the URL. These parameters are then turned into a YAML-string. Finally, the download is executed file by file.
# 
# Each file is saved under it's original filename. Note that the original file names are often not self-explanatory (called "data" or "January"). The files content is revealed by its place in the directory structure.

# Some sources (currently only ENTSO-E Transparency) require an account to allow downloading. For ENTSO-E Transparency, set up an account [here](https://transparency.entsoe.eu/usrm/user/createPublicUser).

# In[40]:


auth = yaml.load('''
ENTSO-E Transparency FTP:
    username: jan.frederick.unnewehr@inatech.uni-freiburg.de
    password: TRcelle1989
Elexon:
    username: your email
    password: your password
''')


# ## Automatic download (for most sources)

# In[ ]:


# download(sources, data_path, input_path, auth,
#           archive_version=None,
#           start_from_user=start_from_user,
#           end_from_user=end_from_user,
#           testmode=False)


# ## Manual download

# ### Energinet.dk

# Go to http://osp.energinet.dk/_layouts/Markedsdata/framework/integrations/markedsdatatemplate.aspx.
# 
# 
# **Check The Boxes as specified below:**
# - Periode
#     - Hent udtræk fra perioden: **01-01-2005** Til: **01-01-2019**
#     - Select all months
# - Datakolonner
#     - Elspot Pris, Valutakode/MWh: **Select all**
#     - Produktion og forbrug, MWh/h: **Select all**
# - Udtræksformat
#     - Valutakode: **EUR**
#     - Decimalformat: **Engelsk talformat (punktum som decimaltegn**
#     - Datoformat: **Andet datoformat (ÅÅÅÅ-MM-DD)**
#     - Hent Udtræk: **Til Excel**
# 
# Click **Hent Udtræk**
# 
# You will receive a file `Markedsata.xls` of about 50 MB. Open the file in Excel. There will be a warning from Excel saying that file extension and content are in conflict. Select "open anyways" and and save the file as `.xlsx`.
# 
# In order to be found by the read-function, place the downloaded file in the following subdirectory:  
# **`{{data_path}}{{os.sep}}Energinet.dk{{os.sep}}prices_wind_solar{{os.sep}}2005-01-01_2019-01-01`**

# ### CEPS
# 
# Go to http://www.ceps.cz/en/all-data#GenerationRES
# 
# **check boxes as specified below:**
# 
# DISPLAY DATA FOR: **Generation RES**  
# TURN ON FILTER **checked**  
# FILTER SETTINGS: 
# - Set the date range
#     - interval
#     - from: **2012** to: **2019**
# - Agregation and data version
#     - Aggregation: **Hour**
#     - Agregation function: **average (AVG)**
#     - Data version: **real data**
# - Filter
#     - Type of power plant: **ALL**
# - Click **USE FILTER**
# - DOWNLOAD DATA: **DATA V TXT**
# 
# You will receive a file `data.txt` of about 1.5 MB.
# 
# In order to be found by the read-function, place the downloaded file in the following subdirectory:  
# **`{{data_path}}{{os.sep}}CEPS{{os.sep}}wind_pv{{os.sep}}2012-01-01_2019-01-01`**

# ### ENTSO-E Power Statistics
# 
# Go to https://www.entsoe.eu/data/statistics/Pages/monthly_hourly_load.aspx
# 
# **check boxes as specified below:**
# 
# - Date From: **01-01-2016** Date To: **28-02-2019**
# - Country: **(Select All)**
# - Scale values to 100% using coverage ratio: **YES**
# - **View Report**
# - Click the Save symbol and select **Excel**
# 
# You will receive a file `MHLV.xlsx` of about 8 MB.
# 
# In order to be found by the read-function, place the downloaded file in the following subdirectory:  
# **`{{os.sep}}original_data{{os.sep}}ENTSO-E Power Statistics{{os.sep}}load{{os.sep}}2016-01-01_2016-04-30`**
# 
# The data covers the period from 01-01-2016 up to the present, but 4 months of data seems to be the maximum that interface supports for a single download request, so you have to repeat the download procedure for 4-Month periods to cover the whole period until the present.

# # Read

# This section: Read each downloaded file into a pandas-DataFrame and merge data from different sources if it has the same time resolution. Takes ~15 minutes to run.

# ## Preparations

# Set the title of the rows at the top of the data used to store metadata internally. The order of this list determines the order of the levels in the resulting output.

# In[41]:


headers = ['region', 'variable', 'attribute', 'source', 'web', 'unit']


# Read a prepared table containing meta data on the geographical areas

# In[42]:


areas = pd.read_csv(areas_csv_path)


# View the areas table

# In[43]:


areas.loc[areas['area ID'].notnull(), :'EIC'].fillna('')


# ## Reading loop

# Loop through sources and datasets to do the reading.
# First read the original CSV, Excel etc. files into pandas DataFrames.

# In[52]:


for source_name, source_dict in sources.items():
    print(source_name)
    for dataset_name, param_dict in source_dict.items():
        print(dataset_name)
        print(param_dict)
        dataset_dir = os.path.join(data_path, source_name, dataset_name)
        print(sorted(os.listdir(dataset_dir)))


# In[53]:


sources


# In[54]:


read(sources, data_path, parsed_path, areas, headers,
      start_from_user=start_from_user, end_from_user=end_from_user,
      testmode=False)


# Then combine the DataFrames that have the same temporal resolution

# In[55]:


# Create a dictionary of empty DataFrames to be populated with data
data_sets = {'15min': pd.DataFrame(),
              '30min': pd.DataFrame(),
              '60min': pd.DataFrame()}
entso_e = {'15min': pd.DataFrame(),
            '30min': pd.DataFrame(),
            '60min': pd.DataFrame()}
for filename in os.listdir(parsed_path):
    res_key, source_name, dataset_name, = filename.split('_')[:3]
    if subset and not source_name in subset.keys():
        continue
    logger.info('include %s', filename)
    df_portion = pd.read_pickle(os.path.join(parsed_path, filename))

    if source_name == 'ENTSO-E Transparency FTP':
        dfs = entso_e
    else:
        dfs = data_sets

    if dfs[res_key].empty:
        dfs[res_key] = df_portion
    elif not df_portion.empty:
        dfs[res_key] = dfs[res_key].combine_first(df_portion)
    else:
        logger.warning(filename + ' WAS EMPTY')


# In[56]:


entso_e


# In[57]:


for res_key, df in data_sets.items():
    logger.info(res_key + ': %s', df.shape)
for res_key, df in entso_e.items():
    logger.info('ENTSO-E ' + res_key + ': %s', df.shape)


# Display some rows of the dataframes to get a first impression of the data.

# In[60]:


entso_e['30min'].head()


# ## Save raw data

# Save the DataFrames created by the read function to disk. This way you have the raw data to fall back to if something goes wrong in the ramainder of this notebook without having to repeat the previos steps.

# In[61]:


os.chdir(temp_path)
data_sets['15min'].to_pickle('raw_data_15.pickle')
data_sets['30min'].to_pickle('raw_data_30.pickle')
data_sets['60min'].to_pickle('raw_data_60.pickle')
entso_e['15min'].to_pickle('raw_entso_e_15.pickle')
entso_e['30min'].to_pickle('raw_entso_e_30.pickle')
entso_e['60min'].to_pickle('raw_entso_e_60.pickle')


# Load the DataFrames saved above

# In[62]:


os.chdir(temp_path)
data_sets = {}
data_sets['15min'] = pd.read_pickle('raw_data_15.pickle')
data_sets['30min'] = pd.read_pickle('raw_data_30.pickle')
data_sets['60min'] = pd.read_pickle('raw_data_60.pickle')
entso_e = {}
entso_e['15min'] = pd.read_pickle('raw_entso_e_15.pickle')
entso_e['30min'] = pd.read_pickle('raw_entso_e_30.pickle')
entso_e['60min'] = pd.read_pickle('raw_entso_e_60.pickle')


# # Processing

# This section: missing data handling, aggregation of sub-national to national data, aggregate 15'-data to 60'-resolution. Takes 30 minutes to run.

# ## Missing data handling

# ### Interpolation

# Patch missing data. At this stage, only small gaps (up to 2 hours) are filled by linear interpolation. This catched most of the missing data due to daylight savings time transitions, while leaving bigger gaps untouched
# 
# The exact locations of missing data are stored in the `nan_table` DataFrames.

# Patch the datasets and display the location of missing Data in the original data. Takes ~5 minutes to run.

# In[63]:


nan_tables = {}
overviews = {}
for res_key, df in data_sets.items():
    data_sets[res_key], nan_tables[res_key], overviews[res_key] = find_nan(
        df, res_key, headers, patch=True)


# In[64]:


for res_key, df in entso_e.items():
    entso_e[res_key], nan_tables[res_key + ' ENTSO-E'], overviews[res_key + ' ENTSO-E'] = find_nan(
        df, res_key, headers, patch=True)


# Execute this to see an example of where the data has been patched.

# Display the table of regions of missing values

# In[ ]:


nan_tables['60min']


# You can export the NaN-tables to Excel in order to inspect where there are NaNs

# In[ ]:


# os.chdir(temp_path)
# writer = pd.ExcelWriter('NaN_table.xlsx')
# for res_key, df in nan_tables.items():
#     df.to_excel(writer, res_key)
# writer.save()

# writer = pd.ExcelWriter('Overview.xlsx')
# for res_key, df in overviews.items():
#     df.to_excel(writer, res_key)
# writer.save()


# Save/Load the patched data sets

# In[ ]:


os.chdir(temp_path)
data_sets['15min'].to_pickle('patched_15.pickle')
data_sets['30min'].to_pickle('patched_30.pickle')
data_sets['60min'].to_pickle('patched_60.pickle')
entso_e['15min'].to_pickle('patched_entso_e_15.pickle')
entso_e['30min'].to_pickle('patched_entso_e_30.pickle')
entso_e['60min'].to_pickle('patched_entso_e_60.pickle')


# In[ ]:


os.chdir(temp_path)
data_sets = {}
data_sets['15min'] = pd.read_pickle('patched_15.pickle')
data_sets['30min'] = pd.read_pickle('patched_30.pickle')
data_sets['60min'] = pd.read_pickle('patched_60.pickle')
entso_e = {}
entso_e['15min'] = pd.read_pickle('patched_entso_e_15.pickle')
entso_e['30min'] = pd.read_pickle('patched_entso_e_30.pickle')
entso_e['60min'] = pd.read_pickle('patched_entso_e_60.pickle')


# ## Country specific calculations

# Some of the following operations require the Dataframes to be lexsorted in the columns

# In[ ]:


for res_key, df in data_sets.items():
    df.sort_index(axis='columns', inplace=True)


# ### Germany

# #### Aggregate German data from individual TSOs

# The wind and solar in-feed data for the 4 German control areas is summed up and stored in a new column. The column headers are created in the fashion introduced in the read script. Takes 5 seconds to run.

# In[ ]:


df = data_sets['15min']
control_areas_DE = ['DE_50hertz', 'DE_amprion', 'DE_tennet', 'DE_transnetbw']

for variable in ['solar', 'wind', 'wind_onshore', 'wind_offshore']:
    # we could also include 'generation_forecast'
    for attribute in ['generation_actual']:
        # Calculate aggregate German generation
        sum_frame = df.loc[:, (control_areas_DE, variable, attribute)]
        sum_frame.head()        
        sum_col = sum_frame.sum(axis='columns', skipna=False).to_frame().round(0)

        # Create a new MultiIndex
        new_col_header = {
            'region': 'DE',
            'variable': variable,
            'attribute': attribute,
            'source': 'own calculation based on German TSOs',
            'web': '',
            'unit': 'MW'
        }
        new_col_header = tuple(new_col_header[level] for level in headers)
        data_sets['15min'][new_col_header] = sum_col
        data_sets['15min'][new_col_header].describe()


# ### Italy

# Generation data for Italy come by region (North, Central North, Sicily, etc.) and separately for DSO and TSO, so they need to be agregated in order to get values for the whole country. In the next cell, we sum up the data by region and for each variable-attribute pair present in the Terna dataset header.

# In[ ]:


bidding_zones_IT = ['IT_CNOR', 'IT_CSUD', 'IT_NORD', 'IT_SARD', 'IT_SICI', 'IT_SUD']
attributes = ['generation_actual', 'generation_actual_dso', 'generation_actual_tso']

for variable in ['solar', 'wind_onshore']:
    sum_col = (
        data_sets['60min']
        .loc[:, (bidding_zones_IT, variable, attributes)]
        .sum(axis='columns', skipna=False))
    
    # Create a new MultiIndex
    new_col_header = {
        'region': 'IT',
        'variable': variable,
        'attribute': 'generation_actual',
        'source': 'own calculation based on Terna',
        'web': 'https://www.terna.it/SistemaElettrico/TransparencyReport/Generation/Forecastandactualgeneration.aspx',
        'unit': 'MW'
    }
    new_col_header = tuple(new_col_header[level] for level in headers)
    data_sets['60min'][new_col_header] = sum_col
    data_sets['60min'][new_col_header].describe()


# ### Great Britain / United Kingdom

# Data for Great Britain (without Northern Ireland) are disaggregated for DSO and TSO connected generators. We calculate aggregate values.

# In[ ]:


for variable in ['solar', 'wind']:
    sum_col = (data_sets['30min']
                .loc[:, ('GB_GBN', variable, ['generation_actual_dso', 'generation_actual_tso'])]
                .sum(axis='columns', skipna=False))
    
    # Create a new MultiIndex
    new_col_header = {
        'region' : 'GB_GBN',
        'variable' : variable,
        'attribute' : 'generation_actual',
        'source': 'own calculation based on Elexon and National Grid',
        'web': '',
        'unit': 'MW'
    }
    new_col_header = tuple(new_col_header[level] for level in headers)
    data_sets['30min'][new_col_header] = sum_col
    data_sets['30min'][new_col_header].describe()


# ## Calculate availabilities/profiles

# Calculate profiles, that is, the share of wind/solar capacity producing at a given time.

# In[ ]:


for res_key, df in data_sets.items():
    if res_key == '60min':
        continue
    for col_name, col in df.loc[:,(slice(None), slice(None), 'capacity')].iteritems():
        # Calculate the profile column
        kwargs = {'key': (col_name[0], col_name[1], 'generation_actual'),
            'level': ['region', 'variable', 'attribute'],
            'axis': 'columns', 'drop_level': False}
        generation_col = df.xs(**kwargs)
        # take ENTSO-E transparency data if there is none from TSO
        if generation_col.size == 0:
            try:
                generation_col = entso_e[res_key].xs(**kwargs)
            except KeyError:
                continue
            if generation_col.size == 0:
                continue
        profile_col = generation_col.divide(col, axis='index').round(4)

        # Create a new MultiIndex
        new_col_header = {
            'region': '{region}',
            'variable': '{variable}',
            'attribute': 'profile',
            'source': 'own calculation based on {source}',
            'web': '',
            'unit': 'fraction'
        }
        
        source_capacity = col_name[3]
        source_generation = generation_col.columns.get_level_values('source')[0]
        if source_capacity == source_generation:
            source = source_capacity
        else:
            source = (source_generation + ' and ' + source_capacity).replace('own calculation based on ', '')
        new_col_header = tuple(new_col_header[level].format(region=col_name[0], variable=col_name[1], source=source)
                                for level in headers)
        data_sets[res_key][new_col_header] = profile_col
        data_sets[res_key][new_col_header].describe()
        
        # Append profile to the dataset
        df = df.combine_first(profile_col)
        new_col_header


# Some of the following operations require the Dataframes to be lexsorted in the columns

# In[ ]:


for res_key, df in data_sets.items():
    df.sort_index(axis='columns', inplace=True)


# Another savepoint

# In[ ]:


os.chdir(temp_path)
data_sets['15min'].to_pickle('calc_15.pickle')
data_sets['30min'].to_pickle('calc_30.pickle')
data_sets['60min'].to_pickle('calc_60.pickle')


# In[ ]:


os.chdir(temp_path)
data_sets = {}
data_sets['15min'] = pd.read_pickle('calc_15.pickle')
data_sets['30min'] = pd.read_pickle('calc_30.pickle')
data_sets['60min'] = pd.read_pickle('calc_60.pickle')
entso_e = {}
entso_e['15min'] = pd.read_pickle('patched_entso_e_15.pickle')
entso_e['30min'] = pd.read_pickle('patched_entso_e_30.pickle')
entso_e['60min'] = pd.read_pickle('patched_entso_e_60.pickle')


# ## Resample higher frequencies to 60'

# Some data comes in 15 or 30-minute intervals (i.e. German or British renewable generation), other in 60-minutes (i.e. load data from ENTSO-E and Prices). We resample the 15 and 30-minute data to hourly resolution and append it to the 60-minutes dataset.
# 
# The `.resample('H').mean()` methods calculates the means from the values for 4 quarter hours [:00, :15, :30, :45] of an hour values, inserts that for :00 and drops the other 3 entries. Takes 15 seconds to run.

# In[ ]:


for ds in [data_sets, entso_e]:
    for res_key, df in ds.items():
        if res_key == '60min':
            continue
    #    # Resample first the marker column
    #    marker_resampled = df['interpolated_values'].groupby(
    #        pd.Grouper(freq='60Min', closed='left', label='left')
    #    ).agg(resample_markers, drop_region='DE_AT_LU')
    #    marker_resampled = marker_resampled.reindex(ds['60min'].index)

    #    # Glue condensed 15/30 min marker onto 60 min marker
    #    ds['60min'].loc[:, 'interpolated_values'] = glue_markers(
    #        ds['60min']['interpolated_values'],
    #        marker_resampled.reindex(ds['60min'].index))

    #    # Drop DE_AT_LU bidding zone data from the 15 minute resolution data to
    #    # be resampled since it is already provided in 60 min resolution by
    #    # ENTSO-E Transparency
    #    df = df.drop('DE_AT_LU', axis=1, errors='ignore')

        # Do the resampling
        resampled = df.resample('H').mean()
        resampled.columns = resampled.columns.map(mark_own_calc)
        resampled.columns.names = headers

        # filter out columns already represented in hourly data
        data_cols = ds['60min'].columns.droplevel(['source', 'web', 'unit'])
        tuples = [col for col in resampled.columns if not col[:3] in data_cols]
        add_cols = pd.MultiIndex.from_tuples(tuples, names=headers)
        resampled = resampled[add_cols]
        
        # Round the resampled columns
        for col in resampled.columns:
            if col[2] == 'profile':
                resampled.loc[:, col] = resampled.loc[:, col].round(4)
            else:
                resampled.loc[:, col] = resampled.loc[:, col].round(0)

        ds['60min'] = ds['60min'].combine_first(resampled)


# ## Fill columns not retrieved directly from TSO webites with  ENTSO-E Transparency data

# In[ ]:


data_cols = data_sets['60min'].columns.droplevel(['source', 'web', 'unit'])

for res_key, df in entso_e.items():
      # Combine with TSO data

#     # Copy entire 30min data from ENTSO-E if there is no data from TSO
    if data_sets[res_key].empty:
          data_sets[res_key] = df

    else:
        # Keep only region, variable, attribute in MultiIndex for comparison
        # Compare columns from ENTSO-E against TSO's, keep which we don't have yet
        cols = [col for col in df.columns if not col[:3] in data_cols]
        add_cols = pd.MultiIndex.from_tuples(cols, names=headers)
        data_sets[res_key] = data_sets[res_key].combine_first(df[add_cols])

#         # Add the ENTSO-E markers (but only for the columns actually copied)
#         add_cols = ['_'.join(col[:3]) for col in tuples]
#         # Spread marker column out over a DataFrame for easiser comparison
#         # Filter out everey second column, which contains the delimiter " | "
#         # from the marker
#         marker_table = (df['interpolated_values'].str.split(' | ', expand=True)
#                         .filter(regex='^\d*[02468]$', axis='columns'))
#         # Replace cells with markers marking columns not copied with NaNs
#         marker_table[~marker_table.isin(add_cols)] = np.nan

#         for col_name, col in marker_table.iteritems():
#             if col_name == 0:
#                 marker_entso_e = col
#             else:
#                 marker_entso_e = glue_markers(marker_entso_e, col)

#         # Glue ENTSO-E marker onto our old marker
#         marker = data_sets[res_key]['interpolated_values']
#         data_sets[res_key].loc[:, 'interpolated_values'] = glue_markers(
#             marker, df['interpolated_values'].reindex(marker.index))


# ## Insert a column with Central European (Summer-)time

# The index column of th data sets defines the start of the timeperiod represented by each row of that data set in **UTC** time. We include an additional column for the **CE(S)T** Central European (Summer-) Time, as this might help aligning the output data with other data sources.

# In[ ]:


info_cols = {'utc': 'utc_timestamp',
              'cet': 'cet_cest_timestamp'}


# In[ ]:


for ds in [data_sets, entso_e]:
    for res_key, df in ds.items():
        if df.empty:
            continue
        df.index.rename(info_cols['utc'], inplace=True)
        df.insert(0, info_cols['cet'],
                  df.index.tz_localize('UTC').tz_convert('CET'))


# # Create a final savepoint

# In[ ]:


data_sets['15min'].to_pickle('final_15.pickle')
data_sets['30min'].to_pickle('final_30.pickle')
data_sets['60min'].to_pickle('final_60.pickle')
#entso_e['15min'].to_pickle('final_entso_e_15.pickle')
#entso_e['30min'].to_pickle('final_entso_e_30.pickle')
#entso_e['60min'].to_pickle('final_entso_e_60.pickle')


# In[ ]:


os.chdir(temp_path)
data_sets = {}
data_sets['15min'] = pd.read_pickle('final_15.pickle')
data_sets['30min'] = pd.read_pickle('final_30.pickle')
data_sets['60min'] = pd.read_pickle('final_60.pickle')
#entso_e = {}
#entso_e['15min'] = pd.read_pickle('final_entso_e_15.pickle')
#entso_e['30min'] = pd.read_pickle('final_entso_e_30.pickle')
#entso_e['60min'] = pd.read_pickle('final_entso_e_60.pickle')


# In[ ]:


combined = data_sets


# Show the column names contained in the final DataFrame in a table

# In[ ]:


col_info = pd.DataFrame()
df = combined['60min']
for level in df.columns.names:
    col_info[level] = df.columns.get_level_values(level)

col_info


# # Write data to disk

# This section: Save as [Data Package](http://data.okfn.org/doc/tabular-data-package) (data in CSV, metadata in JSON file). All files are saved in the directory of this notebook. Alternative file formats (SQL, XLSX) are also exported. Takes about 1 hour to run.

# ## Limit time range
# Cut off the data outside of `[start_from_user:end_from_user]`

# In[ ]:


for res_key, df in combined.items():
    # In order to make sure that the respective time period is covered in both
    # UTC and CE(S)T, we set the start in CE(S)T, but the end in UTC
    if start_from_user:
        start_from_user = (
            pytz.timezone('Europe/Brussels')
            .localize(datetime.combine(start_from_user, time()))
            .astimezone(pytz.timezone('UTC')))
    if end_from_user:
        end_from_user = (
            pytz.timezone('UTC')
            .localize(datetime.combine(end_from_user, time()))
            # Appropriate offset to inlude the end of period
            + timedelta(days=1, minutes=-int(res_key[:2])))
    # Then cut off the data_set
    data_sets[res_key] = df.loc[start_from_user:end_from_user, :]


# ## Different shapes

# Data are provided in three different "shapes": 
# - SingleIndex (easy to read for humans, compatible with datapackage standard, small file size)
#   - Fileformat: CSV, SQLite
# - MultiIndex (easy to read into GAMS, not compatible with datapackage standard, small file size)
#   - Fileformat: CSV, Excel
# - Stacked (compatible with data package standard, large file size, many rows, too many for Excel) 
#   - Fileformat: CSV
# 
# The different shapes need to be created internally befor they can be saved to files. Takes about 1 minute to run.

# In[ ]:


combined_singleindex = {}
combined_multiindex = {}
combined_stacked = {}
for res_key, df in combined.items():
    if df.empty:
        continue

#    # Round floating point numbers to 2 digits
#    for col_name, col in df.iteritems():
#        if col_name[0] in info_cols.values():
#            pass
#        elif col_name[2] == 'profile':
#            df[col_name] = col.round(4)
#        else:
#            df[col_name] = col.round(3)

    # MultIndex
    combined_multiindex[res_key + '_multiindex'] = df

    # SingleIndex
    df_singleindex = df.copy()
    # use first 3 levels of multiindex to create singleindex
    df_singleindex.columns = [
        col_name[0] if col_name[0] in info_cols.values()
        else '_'.join([level for level in col_name[0:3] if not level == ''])
        for col_name in df.columns.values]

    combined_singleindex[res_key + '_singleindex'] = df_singleindex

    # Stacked
    stacked = df.copy().drop(columns=info_cols['cet'], level=0)
    stacked.columns = stacked.columns.droplevel(['source', 'web', 'unit'])
    # Concatenate all columns below each other (="stack").
    # df.transpose().stack() is faster than stacking all column levels
    # seperately
    stacked = stacked.transpose().stack(dropna=True).to_frame(name='data')
    combined_stacked[res_key + '_stacked'] = stacked


# ## Write to SQLite-database

# This file format is required for the filtering function on the OPSD website. This takes ~3 minutes to complete.

# In[ ]:


os.chdir(out_path)
for res_key, df in combined_singleindex.items():
    table = 'time_series_' + res_key
    df = df.copy()
    df.index = df.index.strftime('%Y-%m-%dT%H:%M:%SZ')
    cet_col_name = info_cols['cet']
    df[cet_col_name] = (df[cet_col_name].dt.strftime('%Y-%m-%dT%H:%M:%S%z'))
    df.to_sql(table, sqlite3.connect('time_series.sqlite'),
              if_exists='replace', index_label=info_cols['utc'])


# ## Write to Excel

# Writing the full tables to Excel takes extremely long. As a workaround, only the timestamp-columns are exported. The rest of the data can than be inserted manually from the `_multindex.csv` files.

# In[ ]:


os.chdir(out_path)
writer = pd.ExcelWriter('time_series.xlsx')
for res_key, df in data_sets.items():
    # Need to convert CE(S)T-timestamps to tz-naive, otherwise Excel converts
    # them back to UTC
    df.loc[:,(info_cols['cet'], '', '', '', '', '')].dt.tz_localize(None).to_excel(writer, res_key)
    filename = 'tsos_' + res_key + '.csv'
    df.to_csv(filename, float_format='%.4f', date_format='%Y-%m-%dT%H:%M:%SZ')
for res_key, df in entso_e.items():
    df.loc[:,(info_cols['cet'], '', '', '', '', '')].dt.tz_localize(None).to_excel(writer, res_key+ ' ENTSO-E')
    filename = 'entso_e_' + res_key + '.csv'
    df.to_csv(filename, float_format='%.4f', date_format='%Y-%m-%dT%H:%M:%SZ')
writer.save()


# ## Write to CSV

# This takes about 10 minutes to complete.

# In[ ]:


os.chdir(out_path)
# itertoools.chain() allows iterating over multiple dicts at once
for res_stacking_key, df in itertools.chain(
    combined_singleindex.items(),
    combined_multiindex.items(),
    combined_stacked.items()):

    df = df.copy()

    # convert the format of the cet_cest-timestamp to ISO-8601
    if not res_stacking_key.split('_')[1] == 'stacked':
        df.iloc[:, 0] = df.iloc[:, 0].dt.strftime('%Y-%m-%dT%H:%M:%S%z')  # https://frictionlessdata.io/specs/table-schema/#date
    filename = 'time_series_' + res_stacking_key + '.csv'
    df.to_csv(filename, float_format='%.4f',
              date_format='%Y-%m-%dT%H:%M:%SZ')


# ## Create metadata

# This section: create the metadata, both general and column-specific. All metadata we be stored as a JSON file. Takes 10s to run.

# In[ ]:


os.chdir(out_path)
make_json(combined, info_cols, version, changes, headers, areas,
          start_from_user, end_from_user)


# ## Write checksums.txt

# We publish SHA-checksums for the outputfiles on GitHub to allow verifying the integrity of outputfiles on the OPSD server.

# In[ ]:


os.chdir(out_path)
files = os.listdir(out_path)

# Create checksums.txt in the output directory
with open('checksums.txt', 'w') as f:
    for file_name in files:
        if file_name.split('.')[-1] in ['csv', 'sqlite', 'xlsx']:
            file_hash = get_sha_hash(file_name)
            f.write('{},{}\n'.format(file_name, file_hash))

# Copy the file to root directory from where it will be pushed to GitHub,
# leaving a copy in the version directory for reference
copyfile('checksums.txt', os.path.join(home_path, 'checksums.txt'))

