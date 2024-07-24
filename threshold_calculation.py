import pyodbc
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import time
from IPython.display import clear_output
from datetime import datetime
import json
import os
import requests
import warnings
warnings.filterwarnings('ignore')


def setup_db():
    SERVER = "SERVER NAME"
    DATABASE = ""
    USERNAME = "USERNAME"
    PASSWORD = "PASSWORD"

    connectionString = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes'
    conn = pyodbc.connect(connectionString)
    return conn


class MachiningCenter:

    def __init__(self, name, config_file):
        self.name = name
        self.config = self.load_config(config_file)
        self.n_bar_1 = 50
        self.n_bar_2 = 100
        self.n_bar_3 = 150
        self.n_bar_4 = 300

    def load_config(self, config_file):
        with open(config_file, 'r') as f:
            config = json.load(f)
        return config

    def fetch_new_data(self, table_name):
        table = self.config['machining_centers'][self.name]['tables'][table_name]
        cols = self.config['columns']['tables'][table_name][1:]
        filter = ''
        n_cols = len(cols)
        for i in range(n_cols):
            if i != n_cols - 1:
                filter = filter + cols[i] + ' > 100 and '
            else:
                filter = filter + cols[i] + ' > 100'
        SQL_QUERY = f"""
        SELECT *
        FROM {table}
        WHERE {filter}
        ORDER BY [STAMP_DATETIME] DESC
        """
        # print(SQL_QUERY)
        df = pd.read_sql(SQL_QUERY, conn)
        return df.head(2 * self.n_bar_4 - 1)

    def fetch_threshold(self, threshold_table_name):
        threshold_table = self.config['machining_centers'][self.name]['tables'][threshold_table_name]
        SQL_QUERY = f"""
        SELECT *
        FROM {threshold_table}
        ORDER BY [STAMP_DATETIME] DESC
        """

        df = pd.read_sql(SQL_QUERY, conn)
        return df

    def format_insert(self, threshold_table_name, time_stamp, insert_list):
        threshold_table = self.config['machining_centers'][self.name]['tables'][threshold_table_name]
        column_names = ', '.join(self.config['columns']['tables'][threshold_table_name])
        s = f"""
        INSERT INTO {threshold_table} ({column_names})
        VALUES ('{str(time_stamp)[:-3]}', {', '.join(map(str, insert_list))})
        """
        return s

    def insert_data(self, sql_query):
        try:
            cursor = conn.cursor()
            cursor.execute(sql_query)
            conn.commit()
            #print(f"{self.name}: Insert successful at {datetime.now()}")

        except Exception as e:
            print(f"{self.name}: Error occurred - {e} at {datetime.now()}")

    def get_threshold_matrix(self, n_bar, df, columns_cycle_time):
        threshold_matrix = []
        for column in columns_cycle_time[1:]:
            threshold_values = df[column].iloc[self.n_bar_4-1::-1].ewm(span=n_bar, adjust=False).mean().tolist()
            threshold_values = [round(e, 2) for e in threshold_values]
            threshold_matrix.append(threshold_values)
        return threshold_matrix
    
    def line_notify(self, msg):
        url = 'https://notify-api.line.me/api/notify'
        token = 'TOKEN'
        headers = {'content-type':'application/x-www-form-urlencoded','Authorization':'Bearer ' + token}
        resp = requests.post(url, headers=headers, data = {'message':msg})
        print(resp.text)
    
    def update_db(self, threshold_table_name):

        print('\n' + "UPDATING :", self.name)

        df_cycle_time = self.fetch_new_data(threshold_table_name[10:]) # ex. 'clamp' from 'threshold_clamp'
        df_threshold = self.fetch_threshold(threshold_table_name).head(2 * self.n_bar_4 - 1)

        # CASE 1 : ALREADY UP-TO-DATE DATABASE
        
        condition = df_cycle_time['STAMP_DATETIME'] > df_threshold['STAMP_DATETIME'].iloc[0]
        if not condition.any():
            print('\n' + "ALREADY UP-TO-DATE")
            print(datetime.now())
            return 0

        idx = df_cycle_time[condition].index[-1]

        # CASE 2 : MONITOR UPDATE DATABASE

        if idx == 0:
            table_name = threshold_table_name[10:]          # ex. 'clamp' from 'threshold_clamp'
            df_cycle_time = self.fetch_new_data(table_name)
            time_stamp = df_cycle_time.iloc[0]['STAMP_DATETIME']
            n_cols = len(self.config['columns']['tables'][table_name]) - 1
            last_ema_vals = df_threshold.iloc[0][2 : -n_cols].to_numpy()
            last_ema_vals = np.array(np.array_split(last_ema_vals, 4))
            last_avg_vals = df_threshold.iloc[0][-n_cols:].to_numpy()
            new_vals = df_cycle_time.iloc[0][2:].to_numpy()
            n_bars = [self.n_bar_1, self.n_bar_2, self.n_bar_3, self.n_bar_4]

            insert_list = []

            # EMA
            for i in range(len(last_ema_vals)):
                each_list = (new_vals * 2 / (n_bars[i] + 1) + last_ema_vals[i] * (1 - 2 / (n_bars[i] + 1))).tolist()
                #print("each_list :", each_list)
                insert_list.extend([round(val, 2) for val in each_list])

            # AVERAGE * 5
            n = len(df_threshold)
            #print("n :", n)
            each_list = ((new_vals + last_avg_vals/5 * (n - 1)) / n * 5).tolist()
            # each_list = ((new_vals + last_avg_vals) / 2).tolist()
            insert_list.extend([round(val, 2) for val in each_list])

            #print("insert_list :", insert_list)

            # Line Notify
            new_cycle_times = np.array(df_cycle_time.iloc[0][2:])
            line_notify_condition = new_cycle_times >= each_list
            if line_notify_condition.any():
                msg = self.name + ' ' + str(table_name) + ' : ' + 'Too long cycle time detected'
                self.line_notify(msg)

            SQL_INSERT = self.format_insert(threshold_table_name, time_stamp, insert_list)
            #print("SQL_INSERT :", SQL_INSERT)
            self.insert_data(SQL_INSERT)
            print('\n' + "*****INSERTED BY MONITOR*****")
            print(datetime.now())
            return 0
        
        # CASE 3 : INITIAL UPDATE DATABASE

        df = df_cycle_time.iloc[:idx + 1].iloc[:self.n_bar_4]         # get the less value
        #print("df :", df)
        time_stamp_list = df['STAMP_DATETIME'].iloc[:idx + 1].astype(str)
        time_stamp_list = [ts + '000' for ts in time_stamp_list]

        columns_cycle_time = self.config['columns']['tables'][threshold_table_name[10:]]        # ex. 'clamp' from 'threshold_clamp'

        threshold_matrix_1 = self.get_threshold_matrix(self.n_bar_1, df, columns_cycle_time)    # ex.clamping EMA150 and unclamping EMA150
        threshold_matrix_2 = self.get_threshold_matrix(self.n_bar_2, df, columns_cycle_time)    # ex.clamping EMA300 and unclamping EMA300
        threshold_matrix_3 = self.get_threshold_matrix(self.n_bar_3, df, columns_cycle_time)    # ex.clamping EMA600 and unclamping EMA600
        threshold_matrix_4 = self.get_threshold_matrix(self.n_bar_4, df, columns_cycle_time)    # ex.clamping EMA1000 and unclamping EMA1000

        avg_matrix = []
        for column in columns_cycle_time[1:]:
            cumulative_sum = np.cumsum(df[column])
            avgs = cumulative_sum / np.arange(1, len(df[column]) + 1)
            avgs = avgs.to_list()
            #avg_matrix.append([round(e, 2) for e in avgs])
            avg_matrix.append([round(e * 5, 2) for e in avgs])

        insert_lists = np.array([threshold_matrix_1, threshold_matrix_2, threshold_matrix_3, threshold_matrix_4, avg_matrix])
        insert_lists = insert_lists.transpose(2, 0, 1).reshape(insert_lists.shape[2], -1)
        insert_lists = np.flip(insert_lists, axis=0)
        # print("len(insert_lists) :", len(insert_lists))

        for i in range(len(insert_lists)):
            SQL_INSERT = self.format_insert(threshold_table_name, time_stamp_list[i], insert_lists[i])
            #print(i, "SQL_INSERT :", SQL_INSERT)
            self.insert_data(SQL_INSERT)

        print('\n' + "*****INSERTED BY INITIAL*****")
        print(datetime.now())


conn = setup_db()
os.system('cls' if os.name == 'nt' else 'clear')

mc32 = MachiningCenter("MC32", "config.json")
mc32.update_db('threshold_clamp')
mc32.update_db('threshold_door')
mc32.update_db('threshold_pot')
mc32.update_db('threshold_shutter')

mc33 = MachiningCenter("MC33", "config.json")
mc33.update_db('threshold_clamp')
mc33.update_db('threshold_door')
mc33.update_db('threshold_pot')
mc33.update_db('threshold_shutter')

mc34 = MachiningCenter("MC34", "config.json")
mc34.update_db('threshold_clamp')
mc34.update_db('threshold_door')
mc34.update_db('threshold_pot')
mc34.update_db('threshold_shutter')

mc35 = MachiningCenter("MC35", "config.json")
mc35.update_db('threshold_clamp')
mc35.update_db('threshold_door')
mc35.update_db('threshold_pot')
mc35.update_db('threshold_shutter')

mc36 = MachiningCenter("MC36", "config.json")
mc36.update_db('threshold_clamp')
mc36.update_db('threshold_door')
mc36.update_db('threshold_pot')
mc36.update_db('threshold_shutter')

mc37 = MachiningCenter("MC37", "config.json")
mc37.update_db('threshold_clamp')
mc37.update_db('threshold_door')
mc37.update_db('threshold_pot')
mc37.update_db('threshold_shutter')


# print('\n' + '********** Current Database Log **********' + '\n')
# threshold_table_name = 'threshold_clamp'
# print(mc33.fetch_threshold(threshold_table_name).head())
