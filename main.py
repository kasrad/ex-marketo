"__author__ = 'Radim Kasparek kasrad'"
"__credits__ = 'Keboola Drak"
"__component__ = 'Marketo Extractor'"

"""
Python 3 environment 
"""

import pip
pip.main(['install', '--disable-pip-version-check', '--no-cache-dir', 'logging_gelf'])
pip.main(['install', '--disable-pip-version-check', '--no-cache-dir', 'marketorestpython'])

import sys
import os
import logging
import csv
import json
import pandas as pd
from keboola import docker
from marketorestpython.client import MarketoClient


### Environment setup
abspath = os.path.abspath(__file__)
script_path = os.path.dirname(abspath)
os.chdir(script_path)

### Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt="%Y-%m-%d %H:%M:%S")

### Access the supplied rules
cfg = docker.Config('/data/')
params = cfg.get_parameters()
logging.info("params read")
client_id = cfg.get_parameters()["#client_id"] # enter Client ID from Admin > LaunchPoint > View Details
munchkin_id = cfg.get_parameters()["#munchkin_id"] # fill in Munchkin ID, typical format 000-AAA-000
client_secret = cfg.get_parameters()["#client_secret"] # enter Client ID and Secret from Admin > LaunchPoint > View Details
method = cfg.get_parameters()["method"]
desired_fields = cfg.get_parameters()["desired_fields"]
desired_fields = desired_fields.split()
logging.info("config successfuly read")

### Get proper list of tables
in_tables = cfg.get_input_tables()
out_tables = cfg.get_expected_output_tables()
out_files = cfg.get_expected_output_files()
logging.info("IN tables mapped: "+str(in_tables))
# logging.info("IN files mapped: "+str(in_files))
logging.info("OUT tables mapped: "+str(out_tables))
logging.info("OUT files mapped: "+str(out_files))


### destination to fetch and output files and tables
DEFAULT_TABLE_INPUT = "/data/in/tables/"
DEFAULT_FILE_INPUT = "/data/in/files/"

DEFAULT_FILE_DESTINATION = "/data/out/files/"
DEFAULT_TABLE_DESTINATION = "/data/out/tables/"


def get_tables(in_tables):
    """
    Evaluate input and output table names.
    Only taking the first one into consideration!
    """

    ### input file
    table = in_tables[0]
    in_name = table["full_path"]
    in_destination = table["destination"]
    logging.info("Data table: " + str(in_name))
    logging.info("Input table source: " + str(in_destination))
    
    return in_name

def get_output_tables(out_tables):
    """
    Evaluate output table names.
    Only taking the first one into consideration!
    """

    ### input file
    table = out_tables[0]
    in_name = table["full_path"]
    in_destination = table["source"]
    logging.info("Data table: " + str(in_name))
    logging.info("Input table source: " + str(in_destination))

    return in_name


def output_file(output_model, file_out="data.json"):
    """
    Save output data as CSV (pandas)
    """

    # with open(file_out, "w", encoding="utf-8") as csvfile:
    with open(file_out, 'w') as jsonfile:
        json.dump(output_model, jsonfile)
    
    jsonfile.close()
    
  
def extract_leads_by_ids(output_file, source_file,
                         fields = ['id', 'firstName', 'lastName', 'email',
                                   'updatedAt', 'createdAt', 'Do Not Call Reason']):
    """
    Extracts leads by lead_id. 
    The input file needs to contain a column with 
    """
    
    with open(source_file, mode='rt', encoding='utf-8') as in_file,\
         open(output_file, mode='w', encoding='utf-8') as out_file:

        leads = []
        lazy_lines = (line for line in in_file)
        reader = csv.DictReader(lazy_lines, lineterminator = '\n')
        
        mc = MarketoClient(munchkin_id, client_id, client_secret)
        for lead_record in reader:
            lead_detail = mc.execute(method='get_lead_by_id',
                                     id=lead_record["lead_id"])
            if len(lead_detail) > 0 :
                leads.append(lead_detail[0])

        keys = (fields)
        dict_writer = csv.DictWriter(out_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(leads)
        


def extract_leads_by_filter(output_file,
                            source_file,
                            filter_on,
                            filter_values_column,
                            fields = ['id', 'firstName', 'lastName', 'email',
                                      'updatedAt', 'createdAt', 'Do Not Call Reason']):
    """
    Extracts leads based on filters 
    source_file -  needs to contain a column with the values to input
    to the filter
    filter_values_column - the column in the source file that contains
    the values to input to the filter
    filter_on- specifies the field in API (e.g. "email")
    fields - the fields in API that will be returned
    """
    
    with open(source_file, mode='rt', encoding='utf-8') as in_file,\
         open(output_file, mode='w', encoding='utf-8') as out_file:
            

        leads = []
        lazy_lines = (line for line in in_file)
        reader = csv.DictReader(lazy_lines, lineterminator = '\n')
        
        filter_values_list = []
        for lead_record in reader:
            filter_values_list.append(lead_record[filter_values_column])
               
        leads = mc.execute(method = 'get_multiple_leads_by_filter_type',
                          filterType = filter_on,
                          filterValues = filter_values_list,
                          fields = fields,
                          batchSize=None)
        
        if len(leads) > 0:
            print('%i leads extracted', len(leads))
        else:
            print('No leads match the criteria!')
        
        keys = (fields)
        dict_writer = csv.DictWriter(out_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(leads)
        
    
def get_companies(output_file,
                  source_file,
                  filter_on,
                  filter_values_column,
                  fields = ['id', 'firstName', 'lastName', 'email', 'updatedAt',
                            'createdAt', 'Do Not Call Reason']):
    """
    filterType can be: externalCompanyId, id, externalSalesPersonId, company 
    
    Extracts companies based on filters 
    source_file -  needs to contain a column with the values to input
    to the filter
    filter_values_column - the column in the source file that contains
    the values to input to the filter
    filter_on- specifies the field in API (e.g. "company")
    fields - the fields in the API that will be returned
    """
        
    with open(source_file, mode='rt', encoding='utf-8') as in_file,\
         open(output_file, mode='w', encoding='utf-8') as out_file:
            

        companies = []
        lazy_lines = (line for line in in_file)
        reader = csv.DictReader(lazy_lines, lineterminator = '\n')
        
        filter_values_list = []
        for company_record in reader:
            filter_values_list.append(company_record[filter_values_column])
               
        companies = mc.execute(method = 'get_companies',
                          filterType = filter_on,
                          filterValues = filter_values_list,
                          fields = fields,
                          batchSize=None)
        
        if len(companies) > 0:
            print('%i companies extracted', len(companies))
        else:
            print('No companies match the criteria!')
        
        keys = (fields)
        dict_writer = csv.DictWriter(out_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(companies)
        
        
        
        
def get_lead_activities(output_file,
                        source_file,
                        since_date,
                        until_date):
    
    """
    source file: has to contain columns 'activity_type_ids' and 'lead_ids'. These
    must contain the values corresponding for the query.
    output file: will contain columns based on the fields in extracted responses - it can definitely happen that different runs
                    will produce different number of columns!!    
    since_date
    until_date
    """
        
    activity_type_ids = list(pd.read_csv(source_file,
                                         skipinitialspace=True,
                                         usecols=['activity_type_ids']).iloc[:,0])
    lead_ids = list(pd.read_csv(source_file,
                                 skipinitialspace=True,
                                 usecols=['lead_ids']).iloc[:,0])
    
    lead_ids = [str(int(i)) for i in lead_ids if str(i) != 'nan']
    activity_type_ids = [str(int(i)) for i in activity_type_ids if str(i) != 'nan']
    
    results = mc.execute(method='get_lead_activities',
                         activityTypeIds = activity_type_ids,
                         nextPageToken = None,
                         sinceDatetime = since_date,
                         untilDatetime = until_date,
                         batchSize = None,
                         listId = None,
                         leadIds = lead_ids)
    
    if len(results) == 0:
        print('No results!')
        return
    
    unique_keys = []
    for i in results:
        try:
            for j in i['attributes']:
                i[j['name']] = j['value']

            i.pop('attributes', None)

            unique_keys.extend(list(i.keys()))
        except KeyError:
            pass
            

    unique_keys = list(set(unique_keys))

    with open(output_file, mode='w', encoding='utf-8') as out_file:

            keys = (unique_keys)
            dict_writer = csv.DictWriter(out_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(results)
            
def get_lead_changes(output_file,
                     fields,
                     since_date,
                     until_date):
    
    """
    this function take very long to run
    output file: will contain columns based on the fields in extracted responses - it can definitely happen that different runs
                    will produce different number of columns!!    
    since_date
    until_date
    fields: list of field names to return changes for, field names can be retrieved with the Describe Lead API
    """
    
      
    results = mc.execute(method = 'get_lead_changes',
                         fields = fields,
                         nextPageToken = None, 
                         sinceDatetime = since_date,
                         untilDatetime = until_date,
                         batchSize = None,
                         listId = None)
   
    if len(results) == 0:
        print('No results!')
        return
    
    unique_keys = []
    for i in results:
        try:
            for j in i['attributes']:
                i[j['name']] = j['value']

            i.pop('attributes', None)
            
            for k in i['fields']:
                i[k['name']] = k['value']
                
            i.pop('fields', None)

            unique_keys.extend(list(i.keys()))
        except KeyError:
            pass
            

    unique_keys = list(set(unique_keys))

    with open(output_file, mode='w', encoding='utf-8') as out_file:

            keys = (unique_keys)
            dict_writer = csv.DictWriter(out_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(results)
        
        
    
def main():
    """
    Main execution script.
    """
    logging.info('starting the main')
    mc = MarketoClient(munchkin_id, client_id, client_secret)
    logging.info('mc read')
    if method == 'extract_leads_by_ids':
        extract_leads_by_ids(output_file = DEFAULT_TABLE_DESTINATION + 'leads_by_ids.csv',
                            source_file = DEFAULT_TABLE_INPUT + 'lead_ids_list_input.csv',
                            fields = desired_fields)
    elif method == 'extract_leads_by_filter':
        extract_leads_by_filter(output_file = DEFAULT_FILE_DESTINATION + 'leads_by_filter.csv',
                            source_file = DEFAULT_TABLE_INPUT + 'lead_filter_input.csv',
                            filter_on = 'email',
                            filter_values_column = 'e-mail',
                            fields = desired_fields)

    
    
    

    

    


if __name__ == "__main__":

    main()

    logging.info("Done.")
