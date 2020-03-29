#!/usr/bin/python3

import pandas as pd

from urllib import request, parse
from io import StringIO

import requests
import json
import sys
import time


def notify_slack(text, hook_path):
    """
    This just sends the contents of text as a message to a slack channel of your choosing.
    You'll need to use the slack API and get the webhook URL for the destination channel.
    I just dump that URL into a text file and add the path to my .gitignore

    """
    post = {"text": "{0}".format(text)}

    with open(hook_path, 'r') as hook_file:
        hook = hook_file.read().replace('\n', '')

    try:
        json_data = json.dumps(post)
        req = request.Request(hook,
        data=json_data.encode('ascii'),
        headers={'Content-Type': 'application/json'})
        resp = request.urlopen(req)
    except Exception as em:
        print("EXCEPTION: " + str(em))


def fetch_cases():
    """
    Fetches county-level data on confirmed cases of COVID-19 from usafacts.org

    Uses requests to forge a header because usafacts.org rejects the 
    request from pd.read_csv

    Returns a pandas dataframe.
    """

    headers = {'User-agent':'Mozilla/5.0'}

    c = requests.get('https://static.usafacts.org/public/data/covid-19/covid_confirmed_usafacts.csv',
            headers = headers).text

    cases = pd.read_csv(StringIO(c))

    return cases


def fetch_deaths():
    """
    Fetches county-level data on COVID-19 deaths from usafacts.org

    Uses requests to forge a header because usafacts.org rejects the 
    request from pd.read_csv

    Returns a pandas dataframe.
    """

    headers = {'User-agent':'Mozilla/5.0'}

    d = requests.get('https://static.usafacts.org/public/data/covid-19/covid_deaths_usafacts.csv',
            headers = headers).text

    deaths = pd.read_csv(StringIO(d))

    return deaths


def fetch_tests():
    """
    Fetches California data on COVID-19 testing from covidtracking.com

    The covidtracking.com API is fine with the requests from pd.read_csv,
    but I formed this request the same as above for consistency. Also this
    takes advantage of the covidtracking.com API to pre-filter non-CA data

    Returns a pandas dataframe.
    """

    headers = {'User-agent':'Mozilla/5.0'}

    t = requests.get('http://covidtracking.com/api/states/daily.csv?state=CA',
            headers = headers).text

    tests = pd.read_csv(StringIO(t))

    return tests


def fetch_data(hook_path = None):
    """
    Calls all of the fetch methods within individual try/except blocks.

    If a hook_path is provided, will also send slack updates on fetch failures.

    Returns one pandas dataframe per fetch (currently three)
    """

    while True:
        try:
            cases = fetch_cases()
        except:
            msg = 'exception: retrying cases in 10s'
            if hook_path:
                notify_slack(msg, hook_path)
            print(msg)
            time.sleep(10)
            continue
        break

    while True:
        try:
            deaths = fetch_deaths()
        except:
            msg = 'exception: retrying deaths in 10s'
            if hook_path:
                notify_slack(msg, hook_path)
            print(msg)
            time.sleep(10)
            continue
        break

    while True:
        try:
            tests = fetch_tests()
        except:
            msg = 'exception: retrying tests in 10s'
            if hook_path:
                notify_slack(msg, hook_path)
            print(msg)
            time.sleep(10)
            continue
        break

    return cases, deaths, tests

def read_local_data():
    cal = pd.read_csv('data/california_agg.csv')
    bay = pd.read_csv('data/bay_area_agg.csv')
    scc = pd.read_csv('data/santa_clara_agg.csv')

    return cal, bay, scc

