from utils.db_util import db_util

if __name__ == '__main__':
    dbc = db_util()
    dbc.read_input_from_user()
    dbc.read_sql()
    dbc.get_col_mask()