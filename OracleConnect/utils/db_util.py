#% % writefile
#sql_editor_proj / db_crud55.py
import os, sys
import cx_Oracle as cx
import getpass
import pandas as pd
import numpy as np
from IPython.core.display_functions import display
from cryptography.fernet import Fernet
import ipywidgets as widgets


class db_util:

    def __init__(self):
        self.key = Fernet.generate_key()
        # self.cols_to_mask = []
        # print("Key:", key.decode())
        # pass#, hostname, dbname, dbuser, dbpass):
        # self.hostname = hostname

    # self.dbname = dbname
    # self.dbuser = dbuser
    # self.dbpass = dbpass

    def set_dbhost(self, db_hostname):
        self.dbhost = db_hostname

    def get_dbhost(self):
        return self.dbhost

    def set_dbuser(self, dbuser):
        self.dbuser = dbuser

    def get_dbuser(self):
        return self.username

    def set_dbname(self, dbname):
        self.dbname = dbname

    def get_dbname(self):
        return self.dbname

    def set_dbpass(self, dbpass):
        self.dbpass = dbpass

    def get_dbpass(self):
        return self.dbpass

    def get_col_mask(self):
        print(f'cols to mask {self.cols_to_mask}')
        # create getters and setters for all other inputs as well - col & row filtering

    def connect_to_db(self):
        try:
            conn = cx.connect(self.dbuser, Fernet(self.key).decrypt(self.dbpass),
                              self.dbhost + '/' + self.dbname)  # ('hr', p, 'pederast1.fyre.ibm.com/pdb1')
            self.cursor = conn.cursor()
            print('Connected to the database')
        except Exception as e:
            print(e)
            # return
        # return cursor

    def apply_mask(self):
        print(self.df.head(10))
        print(self.cols_to_mask)
        df_masked = self.df
        df_masked[self.cols_to_mask] = 'xxxx'
        print(df_masked.head(10))
        # return self.df

    def read_data_from_db(self, sqlquery, col=None, rowid=None):
        try:
            rows = self.cursor.execute(sqlquery)
            self.df = pd.DataFrame(rows, columns=[col[0] for col in self.cursor.description])
            #if col or rowid:
            #   self.df = self.apply_mask(self.df, col)
        except Exception as e:
            print(e)
            return
            # return df

    def get_columns_to_mask(self, columns):
        print('Select the columns you want to mask: ')
        data = columns  # ["data1", "data2", "data3", "data4"]
        data.insert(0, 'None')
        self.checkboxes = [widgets.Checkbox(value=False, description=col) for col in data]
        output = widgets.VBox(children=self.checkboxes)
        display(output)


    def selected_cols(self):
        self.cols_to_mask = [self.checkboxes[i].description for i in range(0, len(self.checkboxes)) if
                             self.checkboxes[i].value]
        print(f'cols to mask {self.cols_to_mask}')


    def selection(self, columns):
        names = []
        print('Select the columns you want to mask: ')
        data = columns
        # data.insert(0,'None')
        checkbox_objects = []
        for key in data:
            checkbox_objects.append(widgets.Checkbox(value=False, description=key))
            names.append(key)

        arg_dict = {names[i]: checkbox for i, checkbox in enumerate(checkbox_objects)}
        # sel = [data[i] for i, checkbox in enumerate(checkbox_objects) if checkbox]
        # print(f'selected ones: {sel}')

        ui = widgets.VBox(children=checkbox_objects)

        selected_data = []
        self.cols_to_mask = []

        def select_data(**kwargs):
            selected_data.clear()
            self.cols_to_mask.clear()
            for key in kwargs:
                if kwargs[key] is True:
                    selected_data.append(key)
                    self.cols_to_mask.append(key)
            print(selected_data)
            print(self.cols_to_mask)
            # return selected_data

        # self.cols_to_mask = selected_data

        out = widgets.interactive_output(select_data, arg_dict)
        display(ui, out)


    def get_row_filter(self):
        col_dict = self.df.columns
        self.filter_col = widgets.Dropdown(
            options=col_dict,
            value=col_dict[0],
            description='Column:',
            disabled=False,
            button_style=''
        )
        display(self.filter_col)

        cond_dict = ['=', '<>', '>', '<', '<=', '>=', 'BETWEEN', 'LIKE', 'IN']
        self.filter_condition = widgets.Dropdown(
            options=cond_dict,
            value=cond_dict[0],
            description='Condition:',
            disabled=False,
            button_style=''
        )

        display(self.filter_condition)

        self.filter_val_txt = widgets.Text(
            placeholder='Type in the value',
            description='Value:',
            disabled=False
        )
        display(self.filter_val_txt)
        # print(val_txt.value)

    def apply_row_filter(self):
        df_masked = self.df
        print(self.filter_col.value)
        print(self.filter_condition.value)
        print(self.filter_val_txt.value)
        df_masked[df_masked[self.filter_col.value]] = 'xxxx'
        print(df_masked.head(10))

    def read_input_from_user(self):
        self.set_dbhost(input('Hostname: '))
        self.set_dbname(input('Database/SID name: '))
        self.set_dbuser(input('Username: '))
        self.set_dbpass(Fernet(self.key).encrypt(getpass.getpass('Password: ').encode()))
        self.connect_to_db()



    def read_sql(self):
        self.sql = widgets.Textarea(
            value='SQl Query',
            placeholder='Type your query here',
            description='String:',
            disabled=False
        )
        display(self.sql)
        self.read_data_from_db(self.sql.value)
        # sql = input('SQL query: ')
        # self.read_data_frwidgets.interactive_output(self.select_data, arg_dict)
        #         #display(ui,out)
        #         #self.rows_to_mask = input('Provide the condition for row masking: ')
        #         #print(f'selected : {selected}')
        #         #print(f'cols oto mask {self.cols_to_mask}')
        #         #print(f'Masking the columns {self.cols_to_mask} matching the condition {self.rows_to_mask}')
        #         #get_col_mask()
        #
        #     def get_row_mask_condition(self):
        #         self.rows_to_mask = input('Provide the condition for row masking: ')
        #         self.apply_mask()
        #