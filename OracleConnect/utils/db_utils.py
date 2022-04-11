import os, sys
import cx_Oracle as cx
import getpass
import pandas as pd
import numpy as np
from cryptography.fernet import Fernet
import ipywidgets as widgets
import itc_utils.flight_service as itcfs
from IPython.display import display, clear_output
from IPython.core.magics.display import cell_magic


class db_util:

    def __init__(self):
        self.key = Fernet.generate_key()
        self.read_input_from_user()

        # self.btn_click('self.btn_db_conn', 'Connect', self.connect_to_db())
        self.btn_db_conn = widgets.Button(description="Connect")
        output = widgets.Output()

        display(self.btn_db_conn, output)

        @output.capture(clear_output=False)
        def on_button_clicked(b):
            with output:
                clear_output()
                self.connect_to_db()

        self.btn_db_conn.on_click(on_button_clicked)

    def connect_to_db(self):
        try:
            conn = cx.connect(self.dbuser, Fernet(self.key).decrypt(self.dbpass),
                              self.dbhost + '/' + self.dbname)  # ('hr', p, 'pederast1.fyre.ibm.com/pdb1')
            self.cursor = conn.cursor()
            print('Connected to the database!')
        except Exception as e:
            print(e)
            # return
        # return cursor

    def read_data_from_db(self, sqlquery, col=None, rowid=None):
        try:
            rows = self.cursor.execute(sqlquery)
            self.df = pd.DataFrame(rows, columns=[col[0] for col in self.cursor.description])
            cell_magic(clear)
            self.get_columns_to_mask(self.df.columns)
        except Exception as e:
            print(e)
            return
            # return df

    def get_columns_to_mask(self, columns):
        cell_magic(clear)
        clear_output(wait=False)
        print('Select the columns you want to mask: ')
        data = columns
        # data.insert(0,'None')
        self.checkboxes = [widgets.Checkbox(value=False, description=col) for col in data]
        output = widgets.VBox(children=self.checkboxes)
        display(output)
        # selected_data = [checkboxes[i].description for i in range(0,len(checkboxes)) if checkboxes[i].value==True]
        # return checkboxes

        self.btn_col_mask = widgets.Button(description="Mask")
        output = widgets.Output()

        display(self.btn_col_mask, output)

        @output.capture(clear_output=False)
        def on_button_clicked(b):
            with output:
                cell_magic(clear)
                clear_output(wait=False)
                self.selected_cols()

        self.btn_col_mask.on_click(on_button_clicked)

    def selected_cols(self):
        self.cols_to_mask = [self.checkboxes[i].description for i in range(0, len(self.checkboxes)) if
                             self.checkboxes[i].value]

        self.btn_row_mask = widgets.Button(description="Apply row masking")
        output = widgets.Output()

        display(self.btn_row_mask, output)

        @output.capture(clear_output=False)
        def on_button_clicked(b):
            with output:
                clear_output(wait=False)
                self.get_row_filter()

        self.btn_row_mask.on_click(on_button_clicked)

        self.btn_show_data = widgets.Button(description="No row masking...Show data as is")
        output = widgets.Output()

        display(self.btn_show_data, output)

        @output.capture(clear_output=False)
        def on_button_clicked(b):
            with output:
                clear_output()
                self.show_df_data(self.df)

        self.btn_show_data.on_click(on_button_clicked)

    def show_df_data(self, df1):
        print(df1)

    def get_row_filter(self):
        clear_output()
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

        self.btn_row_mask_apply = widgets.Button(description="Apply")
        output = widgets.Output()

        display(self.btn_row_mask_apply, output)

        @output.capture(clear_output=False)
        def on_button_clicked(b):
            with output:
                clear_output()
                self.apply_mask()

        self.btn_row_mask_apply.on_click(on_button_clicked)
        # print(val_txt.value)

    def apply_row_filter(self):
        df_masked = self.df
        # df1[df1['REGION_ID']<2]
        # filter_cond = 'df_masked[self.filter_col.value]' + self.filter_condition.value +  self.filter_val_txt.value
        print(self.filter_col.value + ' ' + self.filter_condition.value + ' ' + self.filter_val_txt.value)

        # df_masked[filter_cond] = 'xxxx'
        # print(df_masked.head(10))

    def apply_mask(self):
        #print(self.df.head(10))
        #print(self.cols_to_mask)
        df_masked = self.df
        df_masked[self.cols_to_mask] = 'xxxx'
        self.show_df_data(df_masked)
        # print(df_masked.head(10))
        # return self.df

    def read_input_from_user(self):
        self.dbhost = input('Hostname: ')
        self.dbname = input('Database/SID name: ')
        self.dbuser = input('Username: ')
        self.dbpass = Fernet(self.key).encrypt(getpass.getpass('Password: ').encode())
        '''self.set_dbhost(input('Hostname: '))
        self.set_dbname(input('Database/SID name: '))
        self.set_dbuser(input('Username: '))
        self.set_dbpass(Fernet(self.key).encrypt(getpass.getpass('Password: ').encode()))
        #self.connect_to_db()'''

    def run_sql_query(self):
        cell_magic(%clear)
        self.sql = widgets.Textarea(
            # value='',
            placeholder='Type your query here',
            description='String:',
            disabled=False
        )
        display(self.sql)

        self.btn_run_query = widgets.Button(description="Run query")
        output = widgets.Output()

        display(self.btn_run_query, output)

        def on_button_clicked(b):
            with output:
                cell_magic(%clear)
                clear_output(wait=False)
                self.read_data_from_db(self.sql.value)

        self.btn_run_query.on_click(on_button_clicked)

    def get_col_mask(self):
        print(f'cols to mask {self.cols_to_mask}')

    def get_row_mask_condition(self):
        self.rows_to_mask = input('Provide the condition for row masking: ')
        self.apply_mask()

    '''def read_from_platform_conn(sql):
        readClient = itcfs.get_flight_client()

        ora_amalthea_data_request = {
            'connection_name': """ora-amalthea""",
            'select_statement': sql
        }

        flightInfo = itcfs.get_flight_info(readClient, nb_data_request=ora_amalthea_data_request)

        data_df_1 = itcfs.read_pandas_and_concat(readClient, flightInfo)

        # print(data_df_1.head(10))

    read_from_platform_conn('select * from emp')'''
