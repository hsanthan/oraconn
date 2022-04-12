% % writefile
sql_editor_proj / db_crud170.py
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
from project_lib import Project
from ibm_watson_studio_lib import access_project_or_space
import operator


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

            self.btn_list_tables = widgets.Button(description="List user tables")
            output = widgets.Output()

            display(self.btn_list_tables, output)

            @output.capture(clear_output=False)
            def on_button_clicked(b):
                with output:
                    # cell_magic('%clear')
                    # clear_output()
                    rows = self.cursor.execute('select TABLE_NAME,TABLESPACE_NAME from user_tables')
                    self.user_tables = pd.DataFrame(rows, columns=[col[0] for col in self.cursor.description])
                    print(self.user_tables)

            self.btn_list_tables.on_click(on_button_clicked)

            self.btn_sql_editor = widgets.Button(description="Open SQL editor")
            output = widgets.Output()

            display(self.btn_sql_editor, output)

            @output.capture(clear_output=False)
            def on_button_clicked(b):
                with output:
                    # cell_magic('%clear')
                    # clear_output()
                    self.run_sql_query()

            self.btn_sql_editor.on_click(on_button_clicked)
        except Exception as e:
            print(e)
            # return
        # return cursor

    def read_data_from_db(self, sqlquery, col=None, rowid=None):
        try:
            rows = self.cursor.execute(sqlquery)
            self.df = pd.DataFrame(rows, columns=[col[0] for col in self.cursor.description])
            self.choose_mask()  # ****mask choices
            # self.get_columns_to_mask(self.df.columns)
        except Exception as e:
            print(e)
            return
            # return df

    def col_mask(self):
        print('Do you want to apply masking?')
        # data = ['Mask Columns', 'Mask Rows', 'Show Data As Is']
        '''self.mask_btns = [widgets.Button(description=col) for col in data]
        output = widgets.HBox(children=self.mask_btns)
        display(output)
        print(self.mask_btns)
        print(self.mask_btns[0])'''

        # self.btn_col_mask = widgets.Button(description="Mask")
        '''output = widgets.Output()

        display(self.btn_col_mask, output)

        @output.capture(clear_output=False)
        def on_button_clicked(b):
            with output:
                #cell_magic('%clear')
                clear_output(wait=False)
                self.selected_cols()

        self.btn_col_mask.on_click(on_button_clicked)'''

        self.btn_mask_cols = widgets.Button(description="Mask Columns")
        btn_mask_cols_out = widgets.Output()

        def on_button_clicked(b):
            with btn_mask_cols_out:
                clear_output(wait=False)
                self.get_columns_to_mask(self.df.columns)

        self.btn_mask_cols.on_click(on_button_clicked)

        self.btn_mask_rows = widgets.Button(description="Mask Rows")
        btn_mask_rows_out = widgets.Output()

        def on_button_clicked(b):
            with btn_mask_rows_out:
                clear_output(wait=False)
                self.get_row_filter()

        self.btn_mask_rows.on_click(on_button_clicked)

        self.btn_no_mask = widgets.Button(description="Show Data As Is")
        btn_no_mask_out = widgets.Output()

        def on_button_clicked(b):
            with btn_no_mask_out:
                clear_output()
                self.show_df_data(self.df)

        self.btn_no_mask.on_click(on_button_clicked)

        output = widgets.HBox(children=[self.btn_mask_cols, self.btn_mask_rows, self.btn_no_mask])
        display(output)

    def get_columns_to_mask1(self, columns):
        # cell_magic('%clear')
        # clear_output(wait=False)
        print('Select the columns you want to mask: ')
        data = columns
        data = data.insert(0, 'None')
        self.checkboxes = [widgets.Checkbox(value=False, description=col) for col in data]
        output = widgets.VBox(children=self.checkboxes)
        display(output)
        # selected_data = [checkboxes[i].description for i in range(0,len(checkboxes)) if checkboxes[i].value==True]
        # return checkboxes

        self.btn_col_mask = widgets.Button(description="Mask")
        btn_col_mask_output = widgets.Output()

        # display(self.btn_col_mask, output)

        self.btn_no_mask = widgets.Button(description="Show Data As Is")
        btn_no_mask_output = widgets.Output()

        output = widgets.HBox(children=[self.btn_col_mask, self.btn_no_mask])
        display(output)

        # @output.capture(clear_output=False)
        def on_button_clicked(b):
            with btn_col_mask_output:
                # cell_magic('%clear')
                clear_output(wait=False)
                self.selected_cols()

        self.btn_col_mask.on_click(on_button_clicked)

        # display(self.btn_no_mask, output)

        # @output.capture(clear_output=False)
        def on_button_clicked(b):
            with btn_no_mask_output:
                # cell_magic('%clear')
                clear_output(wait=False)
                self.show_df_data(self.df)

        self.btn_no_mask.on_click(on_button_clicked)

        output = widgets.HBox(children=[self.btn_col_mask, self.btn_no_mask])
        display(output)

    def get_columns_to_mask(self, columns):
        print('Select the columns you want to mask: ')
        data = columns
        self.checkboxes = [widgets.Checkbox(value=False, description=col) for col in data]
        output = widgets.VBox(children=self.checkboxes)
        display(output)
        # self.choose_mask()
        self.btn_col_mask = widgets.Button(description="Mask")
        output = widgets.Output()

        display(self.btn_col_mask, output)

        @output.capture(clear_output=False)
        def on_button_clicked(b):
            with output:
                # cell_magic('%clear')
                clear_output(wait=False)
                self.selected_cols()

        self.btn_col_mask.on_click(on_button_clicked)
        # self.selected_cols()

    def choose_mask(self):
        print('Do you want to mask columns?')
        self.btn_col_mask_yes = widgets.Button(description="Yes")
        btn_col_mask_output = widgets.Output()

        # display(self.btn_row_mask_yes, output)

        self.btn_no_mask = widgets.Button(description="No,show data as is")
        btn_no_mask_output = widgets.Output()

        # display(self.btn_row_mask_no, output)
        output_hbox = widgets.HBox(children=[self.btn_col_mask_yes, self.btn_no_mask])
        out = widgets.Output()
        display(output_hbox, out)

        # @output.capture(clear_output=False)
        def on_mask_button_clicked(b):
            with out:
                clear_output(wait=False)
                self.get_columns_to_mask(self.df.columns)
            # self.selected_cols()

        self.btn_col_mask_yes.on_click(on_mask_button_clicked)

        # @output.capture(clear_output=False)
        def on_nomask_button_clicked(b):
            with out:
                clear_output(wait=False)
                self.show_df_data(self.df)

        self.btn_no_mask.on_click(on_nomask_button_clicked)

    def selected_cols(self):
        self.cols_to_mask = [self.checkboxes[i].description for i in range(0, len(self.checkboxes)) if
                             self.checkboxes[i].value]

        print('Do you want to apply row masking?')

        self.btn_row_mask_yes = widgets.Button(description="Yes")
        btn_row_mask_yes_output = widgets.Output()

        # display(self.btn_row_mask_yes, output)

        self.btn_row_mask_no = widgets.Button(description="No")
        btn_row_mask_no_output = widgets.Output()

        # display(self.btn_row_mask_no, output)
        output_hbox = widgets.HBox(children=[self.btn_row_mask_yes, self.btn_row_mask_no])
        out = widgets.Output()
        display(output_hbox, out)

        # @output.capture(clear_output=False)
        def on_yes_button_clicked(b):
            with out:
                clear_output(wait=False)
                self.get_row_filter()

        self.btn_row_mask_yes.on_click(on_yes_button_clicked)

        # @output.capture(clear_output=False)
        def on_no_button_clicked(b):
            with out:
                clear_output(wait=False)

                df_masked = self.apply_mask(self.df, self.cols_to_mask)
                self.show_df_data(df_masked)

        self.btn_row_mask_no.on_click(on_no_button_clicked)

        '''self.btn_row_mask = widgets.Button(description="Apply row masking")
        output = widgets.Output()

        display(self.btn_row_mask, output)

        @output.capture(clear_output=False)
        def on_button_clicked(b):
            with output:
                clear_output(wait=False)
                self.get_row_filter()

        self.btn_row_mask.on_click(on_button_clicked)

        self.btn_show_data = widgets.Button(description="No row masking")
        output = widgets.Output()

        display(self.btn_show_data, output)

        @output.capture(clear_output=False)
        def on_button_clicked(b):
            with output:
                clear_output()
                self.apply_mask(self.df)

        self.btn_show_data.on_click(on_button_clicked)'''

    def get_row_filter(self):
        # clear_output()
        col_dict = self.df.columns
        self.filter_col = widgets.Dropdown(
            options=col_dict,
            value=col_dict[0],
            description='Column:',
            disabled=False,
            button_style=''
        )
        # display(self.filter_col)

        cond_dict = ['==', '<>', '>', '<', '<=', '>=']  # , 'BETWEEN', 'LIKE', 'IN']
        self.filter_condition = widgets.Dropdown(
            options=cond_dict,
            value=cond_dict[0],
            description='Condition:',
            disabled=False,
            button_style=''
        )

        # display(self.filter_condition)

        self.filter_val_txt = widgets.Text(
            placeholder='Type in the value',
            description='Value:',
            disabled=False
        )
        # display(self.filter_val_txt)
        output = widgets.HBox(children=[self.filter_col, self.filter_condition, self.filter_val_txt])
        display(output)
        self.display_data()

    def display_data(self):
        self.btn_row_mask_apply = widgets.Button(description="Display data")
        output = widgets.Output()

        display(self.btn_row_mask_apply, output)

        @output.capture(clear_output=False)
        def on_button_clicked(b):
            with output:
                clear_output()
                df_masked = self.apply_mask(self.df, self.cols_to_mask, self.filter_col.value,
                                            int(self.filter_val_txt.value), self.filter_condition.value)
                self.show_df_data(df_masked)

        self.btn_row_mask_apply.on_click(on_button_clicked)

        '''self.btn_row_mask_apply = widgets.Button(description="Display data")
        output = widgets.Output()
        display(self.btn_row_mask_apply)


        @output.capture(clear_output=False)
        def on_button_clicked(b):
            with output:
                clear_output()
                #check for data type of filter col value, then set dtype of txt
                df_masked = self.apply_mask(self.df, self.cols_to_mask, self.filter_col.value, int(self.filter_val_txt.value), self.filter_condition.value)
                self.show_df_data(df_masked)
        self.btn_row_mask_apply.on_click(on_button_clicked)'''

    def apply_row_filter(self, df1):
        df_masked = df1
        # df1[df1['REGION_ID']<2]
        # filter_cond = 'df_masked[self.filter_col.value]' + self.filter_condition.value +  self.filter_val_txt.value
        print(self.filter_col.value + ' ' + self.filter_condition.value + ' ' + self.filter_val_txt.value)

        # df_masked[filter_cond] = 'xxxx'
        # print(df_masked.head(10))

    def apply_mask_hardcoded(self, df1):
        # print(self.df.head(10))
        # print(self.cols_to_mask)
        # self.filter_col.value + self.filter_condition.value +  self.filter_val_txt.value
        df_masked = df1
        df_masked[self.cols_to_mask] = 'xxxx'
        self.show_df_data(df_masked)
        # print(df_masked.head(10))
        # return self.df

    def apply_mask(self, df1, maskcolumn, column=None, value=None, relate=None):
        """
        Function to mask data in a cell based upon values in another columnn

        Args:
            table_name (string): representing the name of csv file
            maskcolumn (string): representing the column requiring masked data
            column (string): representing the name of the basecolumn used to mask data in another column
            value (float/string): representing the value from base column used to mask data in another column
            relate (operator): representing the operator used to compare values in column

        Returns:
            df: new dataframe

        """

        ops = {'>': operator.gt,
               '<': operator.lt,
               '>=': operator.ge,
               '<=': operator.le,
               '==': operator.eq}
        # '<>': operator.ne}

        # data = pd.read_csv(table_name)
        # df = pd.DataFrame(data)
        if (column and value and relate):
            df1.loc[ops[relate](df1[column], value), maskcolumn] = 'XXXXX'
        else:
            df1.loc[:, maskcolumn] = 'XXXXX'
            # df.head()

        return df1

    def show_df_data(self, df1):
        print(df1)
        self.btn_save_data = widgets.Button(description="Save data to Project")
        output = widgets.Output()

        display(self.btn_save_data, output)

        @output.capture(clear_output=False)
        def on_button_clicked(b):
            with output:
                clear_output()
                # filename = get_filename_from_user() #'test.csv' #input('Save data as : ')
                self.save_data_to_project(df1)

        self.btn_save_data.on_click(on_button_clicked)

    '''def get_filename_from_user(self):
        filename = input('Save data as: ')
        return filename'''

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
        # cell_magic('%clear')
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

        @output.capture(clear_output=False)
        def on_button_clicked(b):
            with output:
                # cell_magic('%clear')
                clear_output()
                self.read_data_from_db(self.sql.value)

        self.btn_run_query.on_click(on_button_clicked)

    def save_data_to_project_wslib(self, df1):
        try:
            wslib = access_project_or_space()
            filename = 'masked_data.csv'
            # wslib.save_data(filename, data=df1.to_csv(index=False).encode(), overwrite=True)
            wslib.storage.store_data(filename, data=df1.to_csv(index=False).encode(), overwrite=True)
            wslib.storage.register_asset(filename)
            print('Data saved to the project')
            # on save show js alert to take the user to the project assets page
            # call df.to based on the filename extension
        except Exception as e:
            print(e)

    def save_data_to_project(self, df1):
        try:
            project = Project.access()
            filename = 'masked_data.csv'  # input('Save data as: ')
            project.save_data(filename, df1.to_csv(index=False))
            print('Data saved to the project')

            # on save show js alert to take the user to the project assets page
            # call df.to based on the filename extension
        except Exception as e:
            print(e)

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

    def get_platform_conn(self):
        wslib = access_project_or_space()
        self.conns = wslib.list_connections()
        self.conns_names = self.conns['name']
        wslib.show(self.conns['name'])
