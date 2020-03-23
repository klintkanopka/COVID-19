#!/usr/bin/python3

import pandas as pd

from urllib import request, parse
from io import StringIO

import requests
import json
import sys
import time


def notify_slack(text, hook_path):
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

    headers = {'User-agent':'Mozilla/5.0'}

    c = requests.get('https://static.usafacts.org/public/data/covid-19/covid_confirmed_usafacts.csv',
            headers = headers).text

    cases = pd.read_csv(StringIO(c))

    return cases


def fetch_deaths():

    headers = {'User-agent':'Mozilla/5.0'}

    d = requests.get('https://static.usafacts.org/public/data/covid-19/covid_deaths_usafacts.csv',
            headers = headers).text

    deaths = pd.read_csv(StringIO(d))

    return deaths


def fetch_tests():

    headers = {'User-agent':'Mozilla/5.0'}

    t = requests.get('http://covidtracking.com/api/states/daily.csv?state=CA',
            headers = headers).text

    tests = pd.read_csv(StringIO(t))

    return tests


def fetch_data(hook_path = None):

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
