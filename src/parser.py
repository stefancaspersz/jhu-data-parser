# vim: set tabstop=4 shiftwidth=4 expandtab:

import io
import csv
import json
import logging
from re import match
import boto3
from datetime import datetime
from urllib.request import Request, urlopen
from urllib.error import URLError

# Setup logger
logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

logger.info('Setup S3 Bucket')
s3bucket = "data-covid-19"
s3path = "partitioned/"
s3 = boto3.resource('s3')

def fix_date_format(input_date_string):
    date_format = '%m/%d/%y'
    output_date = datetime.strptime(input_date_string, date_format)
    return(output_date)

def fix_record(record_dict):
    fixed_record = {}
    fixed_record['time_series'] = []
    for key in record_dict.keys():
        if match("^\d{1,2}/\d{1,2}/\d{2}$", key):
            date_record = {}
            date_record['date'] = str(fix_date_format(key))
            date_record['value'] = int(record_dict[key])
            fixed_record['time_series'].append(date_record)
        elif key in ["Lat", "Long"]:
            fixed_record[key.lower()] = float(record_dict[key])
        else:
            fixed_record[key.lower()] = record_dict[key]
    return(fixed_record)

def fetch_data(url):
    req = Request(url)
    try: 
        logger.info('Fetch {}'.format(url))
        response = urlopen(req)
    except URLError as e:
        if hasattr(e, 'reason'):
            logger.info('We failed to reach a server.')
            logger.info('Reason: ', e.reason)
        elif hasattr(e, 'code'):
            logger.info('The server could not fulfill the request.')
            logger.info('Error code: ', e.code)
    return(response.read().decode('utf-8'))

def parse_and_store(category, input_csv):
    global s3bucket
    global s3
    numrecords = 0
    recordset = csv.DictReader(io.StringIO(input_csv))
    for row in recordset:
        record = fix_record(row)
        objectbody = json.dumps(record)
        if record['province/state'] == '':
            s3key = s3path + 'type=' + category + '/' + record['country/region'] + '.json'
        else:
            s3key = 'type=' + category + '/' + record['country/region'] + '-' + record['province/state'] + '.json'
        s3object = s3.Object(s3bucket, s3key)
        s3response = s3object.put(Body=objectbody)
        if s3response['ResponseMetadata']['HTTPStatusCode'] == 200:
            numrecords += 1
        else:
            logger.info('<--- ERROR --->')
            logger.info(json.dumps(s3response, indent=4))
    logger.info('Write to S3 complete: {} records of Category = {}'.format(numrecords,category))


def main_handler():

    #country_lookup_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/UID_ISO_FIPS_LookUp_Table.csv'
    confirmed_global_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv'
    deaths_global_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv'
    recovered_global_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv'

    #country_lookup_csv = fetch_data(country_lookup_url)

    logger.info('Start Category = confirmed')
    parse_and_store('confirmed',fetch_data(confirmed_global_url))

    logger.info('Start Category = deaths')
    parse_and_store('deaths',fetch_data(deaths_global_url))

    logger.info('Start Category = recovered')
    parse_and_store('recovered',fetch_data(recovered_global_url))

if __name__ == '__main__':
    logger.info('Running outside AWS Lambda')
    main_handler()