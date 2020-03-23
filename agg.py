#!/user/bin/python3
import time
import requests
import pandas as pd
from io import StringIO
from util import fetch_data, notify_slack


def cal_agg(c, d, t, out_path):
    """
    Takes as inputs cases, deaths, and testing data and
    writes a csv of data aggregated for all of California to out_path

    Currently this is the only aggregation script that produces daily
    testing numbers. This data comes from The Covid Tracking Project
    and comes with its own caveats.
    """
    c = c[c['State'] == 'CA']
    c = c.drop(columns = c.columns[0:4])
    c = c.sum(axis = 0)

    d = d[d['State'] == 'CA']
    d = d.drop(columns = d.columns[0:4])
    d = d.sum(axis = 0)

    data1 = {'date': pd.to_datetime(c.index),
             'cases': c,
             'deaths': d}

    df1 = pd.DataFrame(data1)

    data2 = {'date': pd.to_datetime(t.date, format='%Y%m%d'),
             'positive': t.positive,
             'negative': t.negative,
             'tested': t.total}

    df2 = pd.DataFrame(data2)

    df = pd.merge(df1, df2, on='date', how='outer')
    df.to_csv(out_path, index = False)

def bay_agg(c, d, t, out_path):
    """
    Takes as inputs cases, deaths, and testing data
    writes a csv for only bay area counties to out_path

    Includes the nine counties from the Association of Bay Area Govts

    Note: currently county-level testing data is not available
    """

    bay_counties = [
            'Alameda County',
            'Contra Costa County',
            'Marin County',
            'Napa County',
            'San Mateo County',
            'Santa Clara County',
            'Solano County',
            'Sonoma County',
            'San Francisco County'
            ]

    c = c[c['State'] == 'CA']
    d = d[d['State'] == 'CA']

    c = c[c['County Name'].isin(bay_counties)]
    c = c.drop(columns = c.columns[0:4])
    c = c.sum(axis = 0)

    d = d[d['County Name'].isin(bay_counties)]
    d = d.drop(columns = d.columns[0:4])
    d = d.sum(axis = 0)

    data = {'date': pd.to_datetime(c.index),
            'cases': c,
            'deaths': d,
            'positive': ['' for _ in range(len(c))],
            'negative': ['' for _ in range(len(c))],
            'tested': ['' for _ in range(len(c))]}

    df = pd.DataFrame(data)

    df.to_csv(out_path, index = False)


def scc_agg(c, d, t, out_path):
    """
    Takes as inputs cases, deaths, and testing data
    writes a csv for only santa clara county to out_path

    Note: currently county-level testing data is not available
    """

    bay_counties = [
            'Alameda County',
            'Contra Costa County',
            'Marin County',
            'Napa County',
            'San Mateo County',
            'Santa Clara County',
            'Solano County',
            'Sonoma County',
            'San Francisco County'
            ]

    c = c[c['State'] == 'CA']
    d = d[d['State'] == 'CA']

    c = c[c['County Name'] == 'Santa Clara County']
    c = c.drop(columns = c.columns[0:4])
    c = c.sum(axis = 0)

    d = d[d['County Name'] == 'Santa Clara County']
    d = d.drop(columns = d.columns[0:4])
    d = d.sum(axis = 0)

    data = {'date': pd.to_datetime(c.index),
            'cases': c,
            'deaths': d,
            'positive': ['' for _ in range(len(c))],
            'negative': ['' for _ in range(len(c))],
            'tested': ['' for _ in range(len(c))]}

    df = pd.DataFrame(data)

    df.to_csv(out_path, index = False)


def main(cal_path, bay_path, scc_path, hook_path = None):
    """
    Fetch data and write a cleaned csv for each of California, 
    the Bay Area, and Santa Clara County. Send a slack notification if
    hook_path is provided.
    """

    cases, deaths, tests = fetch_data(hook_path)

    cal_agg(cases, deaths, tests, cal_path)
    bay_agg(cases, deaths, tests, bay_path)
    scc_agg(cases, deaths, tests, scc_path)

    if hook_path:
        notify_slack('updated COVID-19 data', hook_path)


if __name__ == '__main__':
    main('data/california_agg.csv',
            'data/bay_area_agg.csv',
            'data/santa_clara_agg.csv',
            'hook.txt')
