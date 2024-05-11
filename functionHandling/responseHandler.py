import json
import dataHandling
import operator

response ='{"file_name":"contacts.csv","actions":{"crud_opertaion":"CREATE","change_on":"Name","operations":[{"operation":"Concatenation","operands":[{"operand":{"source_from_data":true,"the_operand":"First Name","operand_type":"column"}},{"operand":{"the_operand":" ","operand_type":"arbitrary"}},{"operand":{"source_from_data":true,"the_operand":"Last Name","operand_type":"column"}}]}]}}'

mapping_opperators = {
    "==": operator.eq,
    "!=": operator.ne,
    "<": operator.lt,
    ">": operator.gt,
    "Concatenation": operator.concat,
    "Addition": operator.add,
    "Division": operator.truediv,
    "Subtraction": operator.sub
}

converted_response = json.loads(response)
print(converted_response["actions"]["change_on"])

def delete(df):
    return ~df
def read(df):
    return df

mapping_crud = {
    "DELETE":delete,
    "READ":read
}
def type_catch_and_conver(df , column, variable):
    column_select = df[column]
    colum_type = column_select.dtypes
    return colum_type.type(variable)

def handle_response_condition(response, df):


    conditions = response["conditions"]

    if conditions:
        first_condition = conditions[0]
        print(first_condition)
        combined_mask = mapping_opperators[first_condition["comparator"]](df[first_condition["axis_name"]], type_catch_and_conver(df=df, column=first_condition["axis_name"], variable= first_condition["value"]))
        for condition in conditions[1:]:
            mask = mapping_opperators[condition["comparator"]](df[condition["axis_name"]],type_catch_and_conver(df=df, column=condition["axis_name"], variable= condition["value"]))
            print(mask)
            if conditions[conditions.index(condition)-1].get("logic"):
                if conditions[conditions.index(condition)-1]["logic"] == "&":
                    combined_mask&=mask

                else:
                    combined_mask |= mask



    action = response["actions"]["crud_operation"]
    crud_mask = mapping_crud[action](combined_mask)
    result = df[crud_mask]
    return result.to_dict()

def handle_response_operations(response, df):
        change_on = response["actions"]["change_on"]
        operations = response["actions"]["operations"]
        first_operand = operations[0]["operands"][0]["operand"]
        if first_operand.get("source_from_data"):
            result = df[first_operand["the_operand"]] if first_operand["source_from_data"] else first_operand["the_operand"]
        else:
            result = first_operand["the_operand"]
            #HERE WE CHECK INCASE THE source_from_data WAS MISSED, THIS WILL ALSO BE DONE LATER DOWN THE LINE
        for operation in operations:
            operation_function = mapping_opperators[operation["operation"]]
            operands = operation["operands"]
            for operand in operands[1:]:
                # HERE WE ALSO CHECK INCASE THE source_from_data DATA WAS MISSED
                if operand["operand"].get("source_from_data"):
                    operand_data = df[operand["operand"]["the_operand"]] if operand["operand"]["source_from_data"] else \
                        operand["operand"]["the_operand"]
                else:
                    operand_data = operand["operand"]["the_operand"]


                result = operation_function(result, operand_data)
        df[change_on] = result
        print(df)
        return result
        # operand = operations[0]["operands"][0]["operand"]
        # result = df[operand["the_operand"]] if operand["source_from_data"] else operand["the_operand"]
        #
        # for operand_instance in operations[:]:
        #     print("hehe")
        #     print(operand_instance)
        #     if operand["source_from_data"] == True:
        #         mapping_opperators[operations["operation"]](result, df[operand["operand"]["the_operand"]])
        #     else:
        #         mapping_opperators[operations["operation"]](result, operand["operand"]["the_operand"])
        # return result

print(handle_response_operations(response=converted_response, df= dataHandling.open_df("contacts.csv")))

#print(handle_response(converted_response))
def initial_router(response):
    converted_response = json.loads(response)

    if converted_response["actions"].get("operations") and converted_response.get("conditions"):
        #cal function that handles both
        file_name = converted_response["file_name"]
        df = dataHandling.open_df(file_name)
        filtered_df = handle_response_condition(converted_response, file_name)
        print(handle_response_operations(response=converted_response,df=filtered_df ))
        print("Janes Romanes")

    elif converted_response.get("conditions"):
        file_name = converted_response["file_name"]
        df = dataHandling.open_df(file_name)
        print(handle_response_condition(converted_response, df=df))

    else:
        print(handle_response_condition())


