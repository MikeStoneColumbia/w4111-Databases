from HW_Assignments.HW1_Template.src.BaseDataTable import BaseDataTable
import copy
import csv
import logging
import json
import os
import pandas as pd
import pymysql



class RDBDataTable(BaseDataTable):


    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    def __init__(self, table_name, connect_info, key_columns):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """

        self.table_name = table_name
        self.key_columns = key_columns

        self.connection = pymysql.connect(**connect_info)
        self.cursor = self.connection.cursor()




    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """

        temp = dict(zip(self.key_columns,key_fields))

        ans = self.find_by_template(temp=temp, field_list=field_list)

        if ans is not None and len(ans) > 0:
            ans = ans[0]

        else:
            ans = None


        return ans



    def create_select(self,table_name, template, fields, order_by=None, limit=None, offset=None):
        """
        Produce a select statement: sql string and args.

        :param table_name: Table name: May be fully qualified dbname.tablename or just tablename.
        :param fields: Columns to select (an array of column name)
        :param template: One of Don Ferguson's weird JSON/python dictionary templates.
        :param order_by: Ignore for now.
        :param limit: Ignore for now.
        :param offset: Ignore for now.
        :return: A tuple of the form (sql string, args), where the sql string is a template.
        """

        if fields is None:
            field_list = " * "
        else:
            field_list = " " + ",".join(fields) + " "

        w_clause= self.template_to_where_clause(template)

        sql = "select " + field_list + " from " + table_name + " " + w_clause

        return sql

    def template_to_where_clause(self,template):
        """

        :param template: One of those weird templates
        :return: WHERE clause corresponding to the template.
        """

        if template is None or template == {}:
            result = (None, None)
        else:
            args = []
            terms = []

            for k, v in template.items():
                terms.append(" " + k + "=%s ")
                args.append(v)

            w_clause = "AND".join(terms)
            w_clause = " WHERE " + w_clause

            result = (w_clause, args)

            count = 0

            for string in result[1]:
                result[1][count] = "\'" + str(result[1][count]) + "\'"
                count += 1

            result = result[0] % tuple(result[1])

        return result

    def find_by_template(self, temp, field_list=None, limit=None, offset=None, order_by=None):
        """

        :param template: A dictionary of the form { "field1" : value1, "field2": value2, ...}
        :param field_list: A list of request fields of the form, ['fielda', 'fieldb', ...]
        :param limit: Do not worry about this for now.
        :param offset: Do not worry about this for now.
        :param order_by: Do not worry about this for now.
        :return: A list containing dictionaries. A dictionary is in the list representing each record
            that matches the template. The dictionary only contains the requested fields.
        """
        my_sql = self.create_select(table_name=self.table_name, template = temp, fields=field_list)
        self.cursor.execute(my_sql)
        retrieve = self.cursor.fetchall()
        return retrieve

    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param template: A template.
        :return: A count of the rows deleted.
        """

        ans = self.find_by_primary_key(key_fields)
        ans = self.delete_by_template(ans)

        return ans


    def delete_by_template(self, template):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """

        my_sql = self.create_select(table_name=self.table_name, template=template, fields=None)
        my_sql_get_count = my_sql.replace("*", "count(*)")
        self.cursor.execute(my_sql_get_count)
        retrieve = self.cursor.fetchall()

        my_sql_get_count = my_sql_get_count.replace("count(*)", "")
        my_sql_get_count = my_sql_get_count.replace("select", "delete")

        self.cursor.execute(my_sql_get_count)

        return retrieve[0]['count(*)']

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """

        ans = self.find_by_primary_key(key_fields)
        #ans = self.find_by_template(ans)
        print(ans)
        ans = self.update_by_template(ans,new_values)

        return ans


    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """


        org = []
        keys = []
        s = ' and '
        a = ','

        for key in template.keys():
            org.append(str(key) + ' = ' +  "\"" +str(template[key]) + "\" ")

        for key in new_values.keys():
            keys.append(key)

        org = s.join(org)

        count = 0

        for x in keys:
            keys[count] = keys[count] + " = " + "\"" + new_values[x] + "\""
            count += 1

        keys = a.join(keys)

        string = "update " + str(self.table_name) + " set " + keys + "where " + org

        my_sql = self.create_select(table_name=self.table_name, template=template, fields=None)

        my_sql_get_count = my_sql.replace("*", "count(*)")
        self.cursor.execute(my_sql_get_count)
        retrieve = self.cursor.fetchall()

        self.cursor.execute(string)

        print(string)



        return retrieve[0]['count(*)']


    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        keys = []
        vals = []
        for key in new_record.keys():
            keys.append(key)
            vals.append("\"" + new_record[key] + "\"")



        s = ','
        s = s.join(keys)
        v = ','
        v = v.join(vals)

        ans = "insert into " + str(self.table_name) +" " + "(" + s +")" + " value " + "(" + v  +")"

        self.cursor.execute(ans)



    def get_rows(self):
        return self._rows
