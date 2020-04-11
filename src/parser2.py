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

# ('<--- Setup Logger --->')

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

logger.info('<--- Setup S3 Bucket --->')
s3bucket = "data-covid-19"
s3 = boto3.resource('s3')

def fix_date_format(input_date_string):
    date_format = '%m/%d/%y'
    output_date = datetime.strptime(input_date_string, date_format)
    return(output_date)

def fetch_data(url):
    req = Request(url)
    try: 
        logger.info('<--- Fetch {} --->'.format(url))
        response = urlopen(req)
    except URLError as e:
        if hasattr(e, 'reason'):
            logger.info('We failed to reach a server.')
            logger.info('Reason: ', e.reason)
        elif hasattr(e, 'code'):
            logger.info('The server could not fulfill the request.')
            logger.info('Error code: ', e.code)
    return(response.read().decode('utf-8'))

def store(record):
    global s3bucket
    global s3
    objectbody = json.dumps(record)
    if record['Province/State'] == '':
        s3key = record['Country/Region'] + '.json'
    else:
        s3key = record['Country/Region'] + '-' + record['Province/State'] + '.json'
    s3object = s3.Object(s3bucket, s3key)
    s3response = s3object.put(Body=objectbody)
    if s3response['ResponseMetadata']['HTTPStatusCode'] != 200:
        logger.info('<--- ERROR --->')
        logger.info(json.dumps(s3response, indent=4))
    else:
        logger.info('<--- Write to S3 complete: {} {} --->'.format(record['Country/Region'],record['Province/State']))

def main():

    country_lookup_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/UID_ISO_FIPS_LookUp_Table.csv'
    confirmed_global_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv'
    deaths_global_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv'
    recovered_global_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv'


    country_reponse = fetch_data(country_lookup_url)
    confirmed_reponse = fetch_data(confirmed_global_url)
    deaths_reponse = fetch_data(deaths_global_url)
    recovered_reponse = fetch_data(recovered_global_url)

    confirmed_table = csv.DictReader(io.StringIO(confirmed_reponse))

    numrecords = 0

    for confirmed_row in confirmed_table:
        combined_record = {}
        combined_record['time_series'] = []

        found_deaths = False
        deaths_table = csv.DictReader(io.StringIO(deaths_reponse))

        for deaths_row in deaths_table:
        
            if deaths_row['Country/Region'] == confirmed_row['Country/Region'] and deaths_row['Province/State'] == confirmed_row['Province/State']:
                found_deaths = True

                found_recovered = False
                recovered_table = csv.DictReader(io.StringIO(recovered_reponse))
                for recovered_row in recovered_table:
                    if recovered_row['Country/Region'] == confirmed_row['Country/Region'] and recovered_row['Province/State'] == confirmed_row['Province/State']:
                        found_recovered = True

                        for key in confirmed_row.keys():
                            if match("^\d{1,2}/\d{1,2}/\d{2}$", key):
                                date_record = {}
                                date_record['date'] = str(fix_date_format(key))

                                try:
                                    date_record['confirmed'] = int(confirmed_row[key])
                                except KeyError:
                                    date_record['confirmed'] = 0

                                try:
                                    date_record['deaths'] = int(deaths_row[key])
                                except KeyError:
                                    date_record['deaths'] = 0

                                try:
                                    date_record['recovered'] = int(recovered_row[key])
                                except KeyError:
                                    date_record['recovered'] = 0

                                combined_record['time_series'].append(date_record)

                            elif key in ["lat", "long"]:
                                combined_record[key] = float(confirmed_row[key])

                            else:
                                combined_record[key] = confirmed_row[key]
                        break
                if not found_recovered:
                    logger.info('NO recovered for {} {}'.format(deaths_row['Country/Region'],deaths_row['Province/State']))         
                    for key in confirmed_row.keys():
                        if match("^\d{1,2}/\d{1,2}/\d{2}$", key):
                            date_record = {}
                            date_record['date'] = str(fix_date_format(key))

                            try:
                                date_record['confirmed'] = int(confirmed_row[key])
                            except KeyError:
                                date_record['confirmed'] = 0

                            try:
                                date_record['deaths'] = int(deaths_row[key])
                            except KeyError:
                                date_record['deaths'] = 0

                            date_record['recovered'] = 0

                            combined_record['time_series'].append(date_record)

                        elif key in ["lat", "long"]:
                            combined_record[key] = float(confirmed_row[key])

                        else:
                            combined_record[key] = confirmed_row[key]
                    break
                else:
                    break
        if not found_deaths:
            logger.info('NO deaths for {} {}'.format(deaths_row['Country/Region'],deaths_row['Province/State']))
            for key in confirmed_row.keys():
                if match("^\d{1,2}/\d{1,2}/\d{2}$", key):
                    date_record = {}
                    date_record['date'] = str(fix_date_format(key))

                    try:
                        date_record['confirmed'] = int(confirmed_row[key])
                    except KeyError:
                        date_record['confirmed'] = 0

                    date_record['deaths'] = 0

                    date_record['recovered'] = 0

                    combined_record['time_series'].append(date_record)

                elif key in ["lat", "long"]:
                    combined_record[key] = float(confirmed_row[key])

                else:
                    combined_record[key] = confirmed_row[key]
     
        country_table = csv.DictReader(io.StringIO(country_reponse))
        for country_row in country_table:
            found_country = False
            if country_row['Country_Region'] == confirmed_row['Country/Region']:
                found_country = True
                combined_record['iso2'] = country_row['iso2']
                break
        if not found_country:
            logger.info('No Country lookup for {}'.format(confirmed_row['Country/Region']))

        store(combined_record)
        numrecords += 1

    logger.info('<--- Completed: {} records --->'.format(numrecords))

if __name__ == '__main__':
    main()