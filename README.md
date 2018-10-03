# ex-marketo

Keboola Connection docker app for extracting data from specific endpoints of Marketo REST API. Available under `radim-kasparek.ex-marketo`


## Functionality
This component allows KBC to get data from a few endpoints of the Marketo REST API (http://developers.marketo.com/rest-api/). 
It relies heavily on package marketorestpython (https://github.com/jepcastelein/marketo-rest-python).

As of now, the component can be used for extracting data from 5 endpoints, but it can be easily extended:
- extract_leads_by_ids
- extract_leads_by_filter
- get_lead_activities
- get_lead_changes
- get_deleted_leads

## Parameters
There are 10 options in the UI:
- Munchkin ID
- Client ID
- Client Secret
- Input/Output tables. *The tables need to mapped to specific names (names can be found in main.py)*
- Desired Fields. *write down the column names you want to extract and separate them by white space*
- Method. *denotes the endpoint*
- Until Date
- Since Date
- Filter Values Column. *Denotes the column in the input file that contains the values you want to filter for*




