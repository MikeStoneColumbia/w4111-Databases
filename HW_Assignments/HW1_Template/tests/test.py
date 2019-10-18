from HW_Assignments.HW1_Template.src.CSVDataTable import CSVDataTable
import logging
import os
import json
# The logging level to use should be an environment variable, not hard coded.
logging.basicConfig(level=logging.DEBUG)

# Also, the 'name' of the logger to use should be an environment variable.
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# This should also be an environment variable.
# Also not the using '/' is OS dependent, and windows might need `\\`
data_dir = os.path.abspath("../Data/Baseball")
connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

def test_init():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, None)
    print("CSV data", csv_tbl)


def test_match():
    row = {"cool": "yes", "db": "no"}
    t = {"cool": "yes"}
    result = CSVDataTable.matches_template(row, t)
    print(result)

def test_k():
    connect_info = {"directory": data_dir,
                    "file_name": "People.csv"}

    csv_table = CSVDataTable("people", connect_info, key_columns=['playerID'])
    k = csv_table.key_to_template("willite01")
    csv_table.delete_by_key("willite01")
    print(k)



def test_delete_by_Key():
    print("--------------------------------------------------------------------")

    print("Now testing delete by key")

    csv_table = CSVDataTable("people", connect_info, key_columns=['playerID'])

    templateCSV =  csv_table.key_to_template(['accarje01'])

    result = csv_table.find_by_template(templateCSV)
    print(json.dumps(result,indent=2))

    print('Now deleting')
    k = csv_table.delete_by_key(["accarje01"])
    print(k)
    print("After")
    result = csv_table.find_by_template(templateCSV)
    print(json.dumps(result, indent=2))

    print("--------------------------------------------------------------------")


def t_find_by_template():
    print("--------------------------------------------------------------------")
    print("Now testing find by template")

    key_cols = ["playerID"]
    fields = ['playerID']
    template = {'playerID': "willite01"}
    csv_table = CSVDataTable("people", connect_info, key_columns=key_cols)
    result = csv_table.find_by_template(template=template)
    print("Results = \n", json.dumps(result,indent=2))

    print("--------------------------------------------------------------------")

def test_insert():
    print("--------------------------------------------------------------------")

    print("Now testing insert:")

    print("We Are inserting: playerID': 'Mike', 'nameLast': 'Stone', 'birthCity': 'Long Island \n")

    template = {'playerID': 'Mike'}

    csv_table = CSVDataTable("people", connect_info, key_columns=['playerID'])

    csv_table.insert(template)

    print("Now we look to see if the insert worked:")

    print(csv_table.find_by_template(template))

    print("--------------------------------------------------------------------")





def t_find_by_primary_key():

    print("--------------------------------------------------------------------")
    print("testing find by primary key")

    key_cols = ["playerID"]
    keys = ["willite01"]
    print("Using keys: Willite01")
    csv_table = CSVDataTable("people", connect_info, key_columns=key_cols)
    result = csv_table.find_by_primary_key(keys)
    print("Results = \n", json.dumps(result,indent=2))

    print("--------------------------------------------------------------------")


def test_update_by_template():
    print("--------------------------------------------------------------------")

    key_cols = ["playerID"]
    fields = ['playerID']
    template = {'playerID': "boonera01"}
    csv_table = CSVDataTable("people", connect_info, key_columns=key_cols)
    result = csv_table.find_by_template(template=template)

    print("Now testing update by template:")

    print("Changing abadfe01 to something else")

    print("I am going to change the player too: playerID' : STONESTONE\n")

    change = {'playerID': "STONESTONE"}
    print("Proof that the player exists:")

    print(result)

    print("Changes made: " + str(csv_table.update_by_template(template, change)))

    print("--------------------------------------------------------------------")

def test_update_by_key():
    print("--------------------------------------------------------------------")
    print("Testing update by key")

    key_cols = ["playerID"]
    fields = ['playerID']
    template = {'playerID': "boonera01"}

    print("We are going to try changing player with id: boonera01")

    csv_table = CSVDataTable("people", connect_info, key_columns=key_cols)
    change = {'playerID': "Stone"}

    result = csv_table.update_by_key(['boonera01'],change)

    print("Changes that have occurred: " + str(result))

    print("Now we try to find him again with new ID:")

    print(csv_table.find_by_primary_key(['Stone']))




    print("--------------------------------------------------------------------")


def test_delete_by_template():
    print("--------------------------------------------------------------------")

    print("Now testing delete_by_template")

    print("We are going to try and delete: boonera01")


    print("Showing that willite does exist: ")

    key_cols = ["playerID"]
    fields = ['playerID']
    template = {'playerID': "boonera01"}
    csv_table = CSVDataTable("people", connect_info, key_columns=key_cols)

    print(csv_table.find_by_template(template))

    print()

    print("Now we are going to delete him")

    result = csv_table.delete_by_template(template)

    print("The number of deletes occurred: " + str(result))

    print("Lets try to find boonera01 again: ... if nothing prints then it worked")

    print(csv_table.find_by_template(template))

    print("--------------------------------------------------------------------")

def test_matches():

    r = {"playerID": "webstra01",
         "teamID": 'BOS',
         'yearID': '1960',
         'AB': '3',
         'H': '0',
         'HR': 0,
         'RBI': '1'}

    temp = {"playerID": "webstra01"}

    test = CSVDataTable.matches_template(r,temp)
    print("Matches = ", test)

def t_load():

    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_table = CSVDataTable("people", connect_info, key_columns=['playerID'])
    print('Created Table = ' + str(csv_table))

test_insert()

#t_find_by_primary_key()

#test_update_by_template()

#test_delete_by_Key()

#t_find_by_template()

#test_update_by_key()

#test_delete_by_template()