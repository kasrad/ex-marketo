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
from marketorestpython.client import MarketoClient
import functions as fces

# Environment setup
abspath = os.path.abspath(__file__)
script_path = os.path.dirname(abspath)
os.chdir(script_path)

sys.tracebacklimit = None

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt="%Y-%m-%d %H:%M:%S")

# Access the supplied rules
cfg = docker.Config('/data/')
params = cfg.get_parameters()
logging.info("params read")

# enter Client ID from Admin > LaunchPoint > View Details
# fill in Munchkin ID, typical format 000-AAA-000
# enter Client ID and Secret from Admin > LaunchPoint > View Details

client_id = cfg.get_parameters()["#client_id"] 
munchkin_id = cfg.get_parameters()["#munchkin_id"]
client_secret = cfg.get_parameters()["#client_secret"]
method = cfg.get_parameters()["method"]
desired_fields = cfg.get_parameters()["desired_fields"]
since_date = cfg.get_parameters()["since_date"]  # YYYY-MM-DD
until_date = cfg.get_parameters()["until_date"]  # YYYY-MM-DD
filter_column = cfg.get_parameters()["filter_column"]
filter_field = cfg.get_parameters()["filter_field"]
desired_fields = [i.strip() for i in desired_fields.split(",")]
logging.info("config successfuly read")

# Get proper list of tables
in_tables = cfg.get_input_tables()
out_tables = cfg.get_expected_output_tables()
out_files = cfg.get_expected_output_files()
logging.info("IN tables mapped: " + str(in_tables))
# logging.info("IN files mapped: "+str(in_files))
logging.info("OUT tables mapped: " + str(out_tables))
logging.info("OUT files mapped: " + str(out_files))
logging.info(filter_column)

if len(in_tables) > 1:
    logging.error("Please don't use more than one table as input table.")
    sys.exit(1)
else:
    pass


# Destination to fetch and output files and tables
DEFAULT_TABLE_INPUT = "/data/in/tables/"
DEFAULT_FILE_INPUT = "/data/in/files/"

DEFAULT_FILE_DESTINATION = "/data/out/files/"
DEFAULT_TABLE_DESTINATION = "/data/out/tables/"

# main


def main():
    """
    Main execution script.
    """
    logging.info('starting the main')
    mc = MarketoClient(munchkin_id, client_id, client_secret)
    logging.info('mc read')
    if method == 'extract_leads_by_ids':
        fces.extract_leads_by_ids(output_file = DEFAULT_TABLE_DESTINATION + 'leads_by_ids.csv',
                                  source_file = DEFAULT_TABLE_INPUT + in_tables[0]['destination'],
                                  fields = desired_fields,
                                  mc_object = mc)

    elif method == 'extract_leads_by_filter':
        fces.extract_leads_by_filter(output_file = DEFAULT_TABLE_DESTINATION + 'leads_by_filter.csv',
                                     source_file=DEFAULT_TABLE_INPUT +
                                     in_tables[0]['destination'],
                                     filter_on = filter_field,
                                     filter_values_column = filter_column,
                                     fields = desired_fields,
                                     mc_object = mc)

    elif method == 'get_deleted_leads':
        fces.get_deleted_leads(output_file = DEFAULT_TABLE_DESTINATION + 'deleted_leads.csv',
                               since_date = since_date,
                               mc_object = mc)

    elif method == 'get_lead_changes':
        fces.get_lead_changes(output_file = DEFAULT_TABLE_DESTINATION + 'lead_changes.csv',
                              fields = desired_fields,
                              since_date = since_date,
                              until_date = until_date,
                              mc_object = mc)

    elif method == 'get_lead_activities':
        fces.get_lead_activities(output_file = DEFAULT_TABLE_DESTINATION + 'lead_activites.csv',
                                 source_file=DEFAULT_TABLE_INPUT +
                                 in_tables[0]['destination'],
                                 since_date = since_date,
                                 until_date = until_date,
                                 mc_object = mc)

    elif method == 'get_companies':
        fces.get_companies(output_file = DEFAULT_TABLE_DESTINATION + 'companies.csv',
                           source_file=DEFAULT_TABLE_INPUT +
                           in_tables[0]['destination'],
                           filter_on = filter_field,
                           filter_values_column = filter_column,
                           fields = desired_fields,
                           mc_object = mc)

    elif method == 'get_campaigns':
        fces.get_campaigns(
            output_file=DEFAULT_TABLE_DESTINATION + 'campaigns.csv',
            mc_object = mc)


if __name__ == "__main__":

    main()

    logging.info("Done.")
