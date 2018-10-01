"__author__ = 'Radim Kasparek kasrad'"
"__credits__ = 'Keboola Drak"
"__component__ = 'Marketo Extractor'"

"""
Python 3 environment 
"""

import sys
import os
import logging
from keboola import docker
import functions as fces
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
since_date = cfg.get_parameters()["since_date"] #YYYY-MM-DD
until_date = cfg.get_parameters()["until_date"] #YYYY-MM-DD
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


        
    
def main():
    """
    Main execution script.
    """
    logging.info('starting the main')
    mc = MarketoClient(munchkin_id, client_id, client_secret)
    logging.info('mc read')
    if method == 'extract_leads_by_ids':
        fces.extract_leads_by_ids(output_file = DEFAULT_TABLE_DESTINATION + 'leads_by_ids.csv',
                            source_file = DEFAULT_TABLE_INPUT + 'lead_ids_list_input.csv',
                            fields = desired_fields)

    elif method == 'extract_leads_by_filter':
        fces.extract_leads_by_filter(output_file = DEFAULT_TABLE_DESTINATION + 'leads_by_filter.csv',
                            source_file = DEFAULT_TABLE_INPUT + 'lead_filter_input.csv',
                            filter_on = 'email',
                            filter_values_column = 'e-mail',
                            fields = desired_fields)

    elif method == 'get_deleted_leads':
        fces.get_deleted_leads(output_file = DEFAULT_TABLE_DESTINATION + 'deleted_leads.csv',
                            since_date = since_date)

    elif method == 'get_lead_changes':
        fces.get_lead_changes(output_file = DEFAULT_TABLE_DESTINATION + 'lead_changes.csv',
                     fields = desired_fields,
                     since_date = since_date,
                     until_date = until_date)

    elif method == 'get_lead_activities':
        fces.get_lead_activities(output_file = DEFAULT_TABLE_DESTINATION + 'lead_activites.csv',
                        source_file = DEFAULT_TABLE_INPUT + 'lead_ids_act_ids.csv',
                        since_date = since_date,
                        until_date = until_date)

    elif method == 'get_companies':
        fces.get_companies(output_file = DEFAULT_TABLE_DESTINATION + 'companies.csv',
                  source_file = DEFAULT_TABLE_INPUT + 'lead_ids_act_ids.csv',
                  filter_on = 'email',
                  filter_values_column = 'e-mail',
                  fields = desired_fields)
        

    
    
    

    

    


if __name__ == "__main__":

    main()

    logging.info("Done.")
