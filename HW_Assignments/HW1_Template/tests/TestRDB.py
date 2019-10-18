from HW_Assignments.HW1_Template.src.RDBDataTable import RDBDataTable
import logging
import os
import json
import pymysql

serverInfo = {'host' : '127.0.0.1',
              'user' : 'root',
              'password' : 'Seas0320!',
              'db' : 'database',
              'charset' : 'utf8mb4',
              'cursorclass': pymysql.cursors.DictCursor}

def test_find_by_primary_key():

    print("--------------------------------------------------------------------")
    print("Now testing Find by Primary Key:")

    findkey = ['abercda01']
    rdb_table = RDBDataTable(table_name='people', connect_info=serverInfo , key_columns=['playerID'])

    print("trying to find person with id abercda01")

    ans = rdb_table.find_by_primary_key(findkey)

    print(ans)

    print("--------------------------------------------------------------------")


def test_find_by_template():
    print("--------------------------------------------------------------------")

    print("Now testing: Find by Primary Key")

    rdb_table = RDBDataTable(table_name="people", connect_info=serverInfo, key_columns=['playerID'])

    print("trying to find person with lastName: Williams, and birthCity San Diego")

    template = {'nameLast': 'Williams', 'birthCity': 'San Diego'}

    result = rdb_table.find_by_template(template, field_list=None)

    print(result)

    print("--------------------------------------------------------------------")

def test_delete_by_key():
    print("--------------------------------------------------------------------")

    print("Now testing delete by key:")

    print("We are going to delete abercda01\n")
    findkey = ['abercda01']
    rdb_table = RDBDataTable(table_name='people', connect_info=serverInfo, key_columns=['playerID'])

    print("make sure this person still exists in the database: ")
    ans = rdb_table.find_by_primary_key(findkey)

    print(str(ans))

    print("Now we delete this player and return the number of deletes that occurred: ")
    print(rdb_table.delete_by_key(findkey))

    print()

    print("Now we try to find them after deletion: ... if empty then they have been deleted")
    rdb_table.find_by_template(ans)

    print("Running delete by key again, Expected count to be 0, actual count: " + str(
    rdb_table.delete_by_template(ans)))

    print("--------------------------------------------------------------------")

def test_delete_by_template():
    print("--------------------------------------------------------------------")

    print("Now Testing Delete by template:")

    rdb_table = RDBDataTable(table_name="people", connect_info=serverInfo, key_columns=['playerID'])

    template = {'nameLast': 'Stone', 'birthCity': 'Long Island'}

    print("Inserted someone manually with lastName: Stone, and birthCity: LongIsland")

    result = rdb_table.delete_by_template(template)

    print("Number of deletes that occurred: " + str(result))

    rdb_table.find_by_template(template)
    print("Running delete by template again, Expected count to be 0, actual count: " +str(rdb_table.delete_by_template(template)))
    print("--------------------------------------------------------------------")

def test_update_by_template():
    print("--------------------------------------------------------------------")

    print("Now testing uodate by template:")

    rdb_table = RDBDataTable(table_name="people", connect_info=serverInfo, key_columns=['playerID'])

    print("I made player: playerID': 'Stoner', 'nameLast': 'Stone', 'birthCity': 'Long Island\n")

    template = {'playerID': 'Stoner', 'nameLast': 'Stone', 'birthCity': 'Long Island'}

    print("I am going to change the player too: playerID' : STONESTONE, nameLast: StonerStiner, birthCity: New York City\n")

    change = {'playerID' : "STONESTONE", 'nameLast': 'StonerStiner', 'birthCity': 'New York City'}

    print("Proof that the Stoner Stone player exists:")

    print(rdb_table.find_by_template(template))

    print("Changes made: "+ str(rdb_table.update_by_template(template,change)))

    print("--------------------------------------------------------------------")



def test_update_by_key():
    print("--------------------------------------------------------------------")
    print("Now testing update by key:\n")


    findkey = ['aardsda01']
    print("We are changing player with ID: " + findkey[0] + "\n")

    rdb_table = RDBDataTable(table_name='people', connect_info=serverInfo, key_columns=['playerID'])

    print("We are changing it to playerID: STONESTONE, nameLast: StonerStiner, birthCity: New York City \n")
    change = {'playerID': "STONESTONE", 'nameLast': 'StonerStiner', 'birthCity': 'New York City'}

    ans = rdb_table.update_by_key(findkey, change)

    temp = rdb_table.find_by_primary_key(['aardsda01'])
    print("Now we look for aardsda01 again: " +str(temp) + "     ... if nothing is printed then it doesnt exist")

    print("Now printing number of updates: "+ str(ans))

    print("--------------------------------------------------------------------")


def test_insert():
    print("--------------------------------------------------------------------")

    print("Now testing insert:")

    rdb_table = RDBDataTable(table_name="people", connect_info=serverInfo, key_columns=['playerID'])

    template = {'playerID': 'Stoners', 'nameLast': 'Stone', 'birthCity': 'Long Island'}

    print("We Are inserting: playerID': 'Stoners', 'nameLast': 'Stone', 'birthCity': 'Long Island \n")



    rdb_table.insert(template)
    print("Now we are trying to find the newly inserted player")
    print(rdb_table.find_by_template(template))

    print("--------------------------------------------------------------------")
test_delete_by_template()
test_find_by_template()
test_find_by_primary_key()

test_delete_by_key()

test_insert()
test_update_by_template()
test_delete_by_template()
test_update_by_key()