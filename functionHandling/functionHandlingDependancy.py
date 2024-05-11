import json
import dataHandling
import pandas as pd
#Here we will attempt to create a sort of a router that will be able to carry certain information that will allow all functions to execute.
# Currently im unsure if its better to make a fucntion that creates a function or to simply create a function that can respresent any function.

payload ={
    "data_source":{
    "source":"sheet",
        "source_metadata":{
            "file_name":"contacts.csv",
            "columns":"Email, Was Reached Out To"
        }
    },
    "Action-sequence":[
        {
            "action_type":"Update",
            "filter_by":{
                "column":"Was Reached Out To",
                "value":"True"
            },
            "perform_on":{
                "column":"Email"
                ,"value":"True"
            }
        }
    ]
}

print(payload["Action-sequence"][0]["action_type"])
print(payload["Action-sequence"][0]["filter_by"]["column"])
print(payload["Action-sequence"][0]["filter_by"]["value"])
print(payload["Action-sequence"][0]["perform_on"]["column"])
print(payload["Action-sequence"][0]["perform_on"]["value"])
print(payload["Action-sequence"][0])



def build_function(dict):

    return dataHandling.change_value(match_column=dict["filter_by"]["column"], match_value=dict["filter_by"]["value"],change_column=dict["perform_on"]["column"], change_value=dict["perform_on"]["value"])

build_function(payload["Action-sequence"][0])

"""enable the user to request: read the contacts on this file, for the contacts that we 
have yet not reached out to send them an email reaching out greeting and introducting our company"""

#Requirements


#Users prompt will be as follows:
# From the sheet contacts, email all of users whos Was Reached Out To status is false and after they have been reached out to change the Was Reached Out to True

# Tasks
# The System must confirm the column names
    # Option 1 - Premature function call - Return to the system
    # Option 2 - Premature function call
# The System has to Filter according to user request
    #
# The System needs to perform the action according to the users request
# The System needs to update the data according to users request


# At this stage we are supposed to have the listing of columns and selection of sheet working
# Now we are required to filter