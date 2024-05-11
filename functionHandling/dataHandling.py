import pandas as pd
import numpy as np



def open_df(filename):
    df = pd.read_csv(filename)
    return df

def single_schema_creator(df, title):
    schema = {
                  "columns":[
    ]
    }
    columns = df.columns.tolist()
    for column in columns:
        schema["columns"].append({"column":{"name":column, "type":str(df[column].dtype).replace('dtype(', '').replace(')', '')}})
    return(schema)

def schema_creator(filenames):
    schema = {}
    for name in filenames:
        df = pd.read_csv(name)
        schema[name] = single_schema_creator(df=df, title=name)

    return schema



if __name__ == "__main__":
    schema = schema_creator(["contacts.csv", "job_postings.csv", "shakespeare_plays.csv"])
    print(schema)


def get_columns():
    columns = df.columns.tolist()
    return columns

def filter_by_value(column, value):
    return df[df[column]==value]

def extract_values(column):
    return df[column].tolist()

def change_value(match_column, match_value, change_column,change_value, df):
    df.loc[df[match_column]==match_value, change_column]=change_value
    print(df)
    return df
#change_value(match_column="Was Reached Out To", match_value=True, change_column="Email", change_value=True)

#Mock data: [{"source":{"storage_type":"gsheet", "name":"contacts.csv", "properties":['Unnamed: 0', 'Email', 'First Name', 'Last Name', 'Company', 'Was Reached Out To'}]
