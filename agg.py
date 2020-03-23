#!/user/bin/python3
import time
import requests
import pandas as pd
from io import StringIO
from util import fetch_data, notify_slack


def cali_agg(c, d, t, out_path):
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


def main(cali_path, bay_path, hook_path):

    base_path = '/home/pi/COVID-19/'

    cases, deaths, tests = fetch_data(base_path + hook_path)

    cali_agg(cases, deaths, tests, base_path + cali_path)
    bay_agg(cases, deaths, tests, base_path + bay_path)

    notify_slack('updated COVID-19 data', base_path + hook_path)


if __name__ == '__main__':
    main('data/california_agg.csv',
            'data/bay_area_agg.csv',
            'hook.txt')
