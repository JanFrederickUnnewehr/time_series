'''
Open Power System Data

Time series Datapackage

download.py : download time series files

'''

import argparse
from datetime import datetime, date, time, timedelta
import pytz
import logging
import os
import zipfile
import pandas as pd
import requests
import yaml
from ftplib import FTP
import math
import sys
from time import sleep
import pickle
from . import terna
import paramiko
from paramiko.ssh_exception import BadHostKeyException, AuthenticationException, SSHException

logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')


def download(
        sources,
        data_path,
        input_path,
        auth,
        archive_version=None,
        start_from_user=None,
        end_from_user=None,
        testmode=False):
    '''
    Load YAML file with sources from disk, and download all files for each
    source into the given data_path.

    Parameters
    ----------
    sources : dict
        Dict of download parameters specific to each source.
    data_path : str
        Base download directory in which to save all downloaded files.
    archive_version: str, default None
        OPSD Data Package Version to download original data from.
    start_from_user : datetime.date, default None
        Start of period for which to download the data.
    end_from_user : datetime.date, default None
        End of period for which to download the data.
    testmode: only download 1 file per source to check if the URLs still still
        work

    Returns
    ----------
    None

    '''

    for name, date in {'end_from_user': end_from_user,
                       'start_from_user': start_from_user}.items():
        if date and date > datetime.now().date():
            logger.warning('%s given was %s, must be smaller than %s, '
                           'we have no data for the future!',
                           name, date, datetime.today().date())
            return

    if archive_version:
        download_archive(archive_version, data_path)
    else:
        for source_name, source_dict in sources.items():
            # Skip download where it is not implemented
            no_download = ['Energinet.dk', 'ENTSO-E Power Statistics', 'CEPS']
            if source_name in no_download:
                continue

            if source_name in auth.keys():
                source_auth = auth[source_name]
            else:
                source_auth = None

            download_source(
                source_name,
                source_dict,
                data_path,
                input_path,
                source_auth,
                start_from_user,
                end_from_user,
                testmode=testmode)

    return


def download_archive(archive_version, data_path):
    '''
    Download archived data from the OPSD server. See download()
    for info on parameters.

    '''

    filepath = os.path.join(data_path, 'original_data.zip')

    if not os.path.exists(filepath):
        url = ('http://data.open-power-system-data.org/time_series/'
               '{}/original_data/original_data.zip'.format(archive_version))
        logger.info('Downloading and extracting archived data from %s', url)
        resp = requests.get(url)
        with open(filepath, 'wb') as output_file:
            for chunk in resp.iter_content(1024):
                output_file.write(chunk)

        myzipfile = zipfile.ZipFile(filepath)
        myzipfile.extractall(data_path)
        logger.info('Extracted data to {}'.format(data_path))

    else:
        logger.warning(
            '%s already exists. Delete it if you want to download again',
            filepath)

    return


def download_source(
        source_name,
        source_dict,
        data_path,
        input_path,
        source_auth,
        start_from_user=None,
        end_from_user=None,
        testmode=False):
    '''
    Download all files for source_name as specified by the given
    source_dict into data_path.

    Parameters
    ----------
    source_name : str
        Name of source dataset, e.g. ``TenneT``.
    source_dict : dict
        Dictionary of datasets and their parameters for the given source.
    data_path : str
        Base download directory in which to save all downloaded files.
    start_from_user : datetime.date, default None
        Start of period for which to download the data. 
    end_from_user : datetime.date, default None
        End of period for which to download the data

    Returns
    ----------
    None

    '''

    session = None

    for dataset_name, param_dict in source_dict.items():
        # Set up the filename structure
        if 'filename' in param_dict:
            filename = param_dict['filename']
        else:
            filename = None

        # Determine time range to be downloaded.
        # Start with everything from first to last datapoint on server
        start_server = param_dict['start']
        end_server = param_dict['end']
        if end_server == 'recent':
            end_server = datetime.now().date()
        # narrow down the time range if specified by user
        if start_from_user:
            if start_from_user <= start_server:
                pass  # do nothing
            elif start_server < start_from_user < end_server:
                start_server = start_from_user  # replace start_server
            else:
                continue
                # skip this dataset from the source dict, e.g. in Sweden

        if end_from_user:
            if end_from_user <= start_server:
                continue
                # skip this dataset from the source dict, e.g. in Sweden
            elif start_server < end_from_user < end_server:
                end_server = end_from_user  # replace  end_server
            else:
                pass  # do nothing

        if 'method' in param_dict and param_dict['method'] == 'scrape':
            # In this case, the data have to be scraped from the website in a
            # source-specific way.
            downloaded = download_with_driver(
                source_name,
                dataset_name,
                data_path,
                input_path,
                param_dict,
                start=start_server,
                end=end_server,
                filename=filename
            )

        elif param_dict['frequency'] == 'single file':
            # In these two cases, all data is housed in one file on the server
            downloaded, session = download_file(
                source_name,
                dataset_name,
                data_path,
                param_dict,
                start=start_server,
                end=end_server,
                url=param_dict['url_template'],
                url_params_template=param_dict['url_params_template'],
                filename=filename
            )

        elif param_dict['frequency'] == 'file list':
            # Log into the Elexon website to obtain cookies to download files
            jar = None
            if source_name == 'Elexon':
                from selenium import webdriver
                from selenium.webdriver.common.keys import Keys

                driver = webdriver.Chrome(
                    executable_path='./chromedriver/chromedriver')
                driver.get("https://www.elexonportal.co.uk")
                u = driver.find_element_by_id('pf_control_pf_username')
                p = driver.find_element_by_id('pf_control_pf_password')
                u.send_keys(source_auth['username'])
                p.send_keys(source_auth['password'])
                p.send_keys(Keys.RETURN)
                jar = requests.cookies.RequestsCookieJar()
                for cookie in driver.get_cookies():
                    jar.set(cookie['name'], cookie['value'],
                            domain=cookie['domain'], path=cookie['path'])
                driver.quit()

            for file in param_dict['files']:
                downloaded, session = download_file(
                    source_name,
                    dataset_name,
                    data_path,
                    param_dict,
                    start=file['start'],
                    end=file['end'],
                    url=file['url'],
                    filename=filename,
                    cookies=jar
                )
        else:
            # In all other cases, the files on the servers usually contain the
            # data for subperiods of some regular length (i.e. months or years)
            # Create lists of start- and enddates of periods represented in
            # individual files to be downloaded.

            # tranlate frequency to argument for pd.date_range()
            freq_start = {'yearly': 'AS', 'quarterly': 'QS',
                          'monthly': 'MS', 'daily': 'D'}
            freq_end = {'yearly': 'A', 'quarterly': 'Q',
                        'monthly': 'M', 'daily': 'D'}

            starts = pd.date_range(
                start=start_server, end=end_server,
                freq=freq_start[param_dict['frequency']]
            )

            ends = pd.date_range(
                start=start_server, end=end_server,
                freq=freq_end[param_dict['frequency']]
            )

            if len(starts) == 0:
                starts = pd.DatetimeIndex([start_server])
            if len(ends) == 0:
                ends = pd.DatetimeIndex([end_server])

            if starts[0].date() > start_server:
                # make sure to include full first period, i.e. if start_server
                # is 2014-12-14, set first start to 2014-01-01
                starts = starts.union([starts[0] - 1 * starts.freq])

            if ends[-1].date() < end_server:
                # make sure to include full last period, i.e. if end_server
                # is 2018-01-14, set last end to 2018-01-31
                ends = ends.union([ends[-1] + 1 * ends.freq])

            # else:
            #    # extend both by one period to load a little more data than the user asked for.
            #    # Reasoning: The last hour of the year in UTC is already the first hour of the new year in CET
            #    starts = starts.union([starts[-1] + 1])
            #    ends = ends.union([ends[-1] + 1])

            if source_name == 'ENTSO-E Transparency FTP':
                transport = paramiko.Transport(
                    param_dict['host'], param_dict['port'])
                transport.connect(username=source_auth['username'],
                                  password=source_auth['password'])
                sftp = paramiko.SFTPClient.from_transport(transport)
                param_dict['url_template'] = None
                param_dict['url_params_template'] = None

            else:
                sftp = None

            for s, e in zip(starts, ends):
                downloaded, session = download_file(
                    source_name,
                    dataset_name,
                    data_path,
                    param_dict,
                    start=s,
                    end=e,
                    url=param_dict['url_template'],
                    url_params_template=param_dict['url_params_template'],
                    filename=filename,
                    session=session,
                    sftp=sftp
                )
                if testmode:
                    break

            if source_name == 'ENTSO-E Transarency FTP':
                sftp.close()
    return


def download_with_driver(
        source_name,
        dataset_name,
        data_path,
        input_path,
        param_dict,
        start,
        end,
        filename=None):
    '''
    Decide which scraping function should download the data.


    Parameters
    ----------
    source_name : str
        Name of source dataset, e.g. ``Terna``
    dataset_name : str
        Name of dataset, e.g. ``solar``
    data_path : str
        Base download directory in which to save all downloaded files
    param_dict : dict
        Info required for download, e.g. url, url-parameter, filename. 
    start : datetime.date
        start of data in the file
    end : datetime.date
        end of data in the file
    filename : str, default None
        pattern of filename to use if it can not be retrieved from server

    Returns
    ----------
    downloaded: bool
        True if all the files were downloaded, False otherwise

    '''

    if source_name == 'Terna':
        return download_Terna(dataset_name, data_path, input_path, param_dict, start, end, filename)


def download_Terna(
        dataset_name,
        data_path,
        input_path,
        param_dict,
        start,
        end,
        filename):
    '''
    Download the files from the Tera page one by one

    Extract the links from the database of recorded links.
    If that does not cover the paeriod [start, end],
    if extract_new_terna_links is set to False in processing.ipynb, stop,
    but if it is set to True, scrape the links.

    Parameters
    ----------
    dataset_name : str
        Name of dataset, e.g. ``solar``
    data_path : str
        Base download directory in which to save all downloaded files
    param_dict : dict
        Info required for download, e.g. url, url-parameter, filename. 
    start : datetime.date
        start of data in the file
    end : datetime.date
        end of data in the file
    filename : str, default None
        pattern of filename to use if it can not be retrieved from server

    Returns
    ----------
    None

    '''

    # If extract_new_terna_urls was set to False or not set at all, treat it
    # as a False.
    try:
        extract_new = pickle.load(open('extract_new_terna_urls.pickle', 'rb'))
    except:
        extract_new = False

    # First consult the database
    recorded_path = os.path.join(input_path, 'recorded_terna_urls.csv')
    date_url_dictionary, start, end = terna.read_recorded(
        recorded_path, start, end)

    # If the user wants to add the links not covered in the database
    if extract_new:
        # and such links do exist
        if start <= end:
            # extract them from the Terna's web page
            extracted_date_url_dictionary = terna.extract_urls(start, end)
            # and add them to the dictionary of recorded links
            date_url_dictionary.update(extracted_date_url_dictionary)
            # update the file holding the recorded URLs
            dict1 = {date(*k): v for k, v in date_url_dictionary.items()}
            df = pd.DataFrame.from_dict(
                dict1, orient='index', columns=['url']).rename_axis('date')
            df.sort_index(inplace=True)
            no_gaps = pd.DatetimeIndex(start=df.index[0],
                                       end=df.index[-1],
                                       freq='D')
            df = df.reindex(index=no_gaps)
            df.index.rename('date', inplace=True)
            df.to_csv(os.path.join(input_path, 'recorded_terna_urls.csv'),
                      date_format='%Y-%m-%d')

    # Now, download the files from the links
    session = None
    for date_key in date_url_dictionary:
        url = date_url_dictionary[date_key]
        year, month, day = date_key
        the_date = date(year=year, month=month, day=day)
        downloaded, session = download_file(
            'Terna',
            dataset_name,
            data_path,
            param_dict,
            the_date,
            the_date,
            url=date_url_dictionary[date_key],
            filename=filename,
            session=session
        )

    return


def download_file(
        source_name,
        dataset_name,
        data_path,
        param_dict,
        start,
        end,
        url,
        url_params_template=None,
        filename=None,
        session=None,
        cookies=None,
        sftp=None):
    '''
    Prepare the Download of a single file.
    Make a directory to save the file to and check if it might have been
    downloaded already

    Parameters
    ----------
    source_name : str
        Name of source dataset, e.g. ``TenneT``
    dataset_name : str
        Name of dataset, e.g. ``solar``
    data_path : str
        Base download directory in which to save all downloaded files
    param_dict : dict
        Info required for download, e.g. url, url-parameter, filename. 
    start : datetime.date
        start of data in the file
    end : datetime.date
        end of data in the file
    filename : str, default None
        pattern of filename to use if it can not be retrieved from server
    session : requests.session, optional
        If not given, a new session is created.

    Returns
    ----------
    downloaded : bool
        True if download successful, False otherwise.
    session : requests.session

    '''
    if session is None:
        session = requests.session()

    message = '| {:20.20} | {:20.20} | {:%Y-%m-%d} | {:%Y-%m-%d} | '.format(
        source_name, dataset_name, start, end)

    # Each file will be saved in a folder of its own, this allows us to preserve
    # the original filename when saving to disk.
    container = os.path.join(data_path, source_name, dataset_name,
                             start.strftime('%Y-%m-%d') + '_' +
                             end.strftime('%Y-%m-%d'))
    os.makedirs(container, exist_ok=True)

    # Belgian TSO Elia requires start/end with time in UTC format
    if source_name == 'Elia':
        start = (pytz.timezone('Europe/Brussels')
                 .localize(datetime.combine(start, time()))
                 .astimezone(pytz.timezone('UTC')))

        end = (pytz.timezone('Europe/Brussels')
               .localize(datetime.combine(end + timedelta(days=1), time()))
               .astimezone(pytz.timezone('UTC')))

    # Attempt the download if there is no file yet.
    count_files = len(os.listdir(container))
    if count_files == 0:
        if source_name == 'ENTSO-E Transparency FTP':
            filename = filename.format(u_start=start, u_end=end)
            filepath = os.path.join(container, filename)
            try:
                sftp.get(param_dict['path'] + filename, filepath)
                downloaded = True
            except SSHException:
                donwloaded = False

        else:
            downloaded, session = download_request(
                source_name,
                start,
                end,
                session,
                filename,
                container,
                url_template=url,
                url_params_template=url_params_template,
                cookies=cookies)

        if downloaded:
            logger.info(message + 'download successful')
        else:
            logger.info(message + 'download failed')

    elif count_files == 1:
        downloaded = True
        logger.debug(message + 'download previously')

    else:
        downloaded = True
        logger.warning(
            'There must not be more than one file in: %s. Please check ',
            container)

    return downloaded, session


def download_request(
        source_name,
        start,
        end,
        session,
        filename,
        container,
        url_template,
        url_params_template=None,
        cookies=None):
    '''
    Download a single file via HTTP get.
    Build the url from parameters and save the file to disk under it's original
    filename 

    Parameters
    ----------
    container : str
        unique filepath for the file to be saved
    url_template : 
        stem of URL 
    url_params_template : dict
        dict of parameter names and values to paste into URL

    Returns
    ----------
    downloaded : bool
        True if download successful, False otherwise.
    session : requests.session

    '''
    url_params = {}  # A dict for URL-parameters

    # For most sources, we can use HTTP get method with parameters-dict
    if url_params_template:
        for key, value in url_params_template.items():
            url_params[key] = value.format(
                u_start=start,
                u_end=end,
            )
        url = url_template

    # For other sources that use urls without parameters (e.g. Svenska
    # Kraftnaet)
    else:
        url = url_template.format(
            u_start=start,
            u_end=end,
        )

    if source_name == 'PSE':
        attempts = 10
    else:
        attempts = 1

    # For polish TSO PSE, sometimes the first attempt to download does not work,
    # but later attempts will.
    for i in range(attempts):
        resp = session.get(url, params=url_params, cookies=cookies)
        if resp.status_code == 200:
            break
        elif i == attempts - 1:
            downloaded = False
            return downloaded, session
        elif source_name == 'PSE':
            logger.warning(
                'http status %s, attempt %s, trying again in 1:10 minutes...',
                resp.status_code, i + 1)
            sleep(70)

    # Get the original filename
    try:
        original_filename = (
            resp.headers['content-disposition']
            .split('filename=')[-1]
            .replace('"', '')
            .replace(';', '')
            .replace(':', '_')
        )
        logger.debug('Downloaded from URL: %s Original filename: %s',
                     resp.url, original_filename)

    # For cases where the original filename can not be retrieved,
    # I put the filename in the param_dict
    except KeyError:
        if filename:
            original_filename = filename.format(u_start=start, u_end=end)
        else:
            logger.warning(
                'original filename could neither be retrieved from server '
                'nor sources.yml'
            )
            original_filename = 'unknown_filename'

        logger.debug('Downloaded from URL: %s', resp.url)

    # Save file to disk
    filepath = os.path.join(container, original_filename)
    with open(filepath, 'wb') as output_file:
        for chunk in resp.iter_content(1024):
            output_file.write(chunk)
    downloaded = True
    return downloaded, session
