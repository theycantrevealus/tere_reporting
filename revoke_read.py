# import csv
# import bson
# import pytz
# import re
# import time
# import sys
# import psutil
import math
import os
from tabulate import tabulate
import pandas as pd


def load_log_file(file_path, directory):
    """Load"""
    try:
        log_data = pd.concat([pd.read_csv(f"{directory}/{file}", sep='|').assign(match=False) for file in file_path], ignore_index=False)
        log_data['source_msisdn'] = log_data['source_msisdn'].astype(str)
        return log_data[['source_msisdn', 'source_total_poin', 'revoked_poin', 'remaining', 'match']]
    except (ValueError, TypeError) as e:
        print(f"Error loading log file: {e}")

# Tabulate log data
def tabulate_log_data(df):
    """Load"""
    try:
        indices_to_remove = []
        for row in df.itertuples():
            if(row.remaining == 0):
                indices_to_remove.append(row.Index)

            if(math.isnan(row.revoked_poin)):
                df.loc[row.Index, 'revoked_poin'] = 0

            # if(row.source_msisdn != "6281262590816"):
            #     indices_to_remove.append(row.Index)

        for row in df.itertuples():
            check = row.remaining + row.revoked_poin
            if(check == row.source_total_poin):
                df.loc[row.Index, 'match'] = True
                indices_to_remove.append(row.Index)

        df.drop(indices_to_remove, inplace=True)
        # df['revoked_poin'] = df['revoked_poin'].astype('float64')
        tabulated_data = tabulate(df, headers='keys', tablefmt='psql')
        return tabulated_data
    except (ValueError, TypeError) as e:
        print(f"Error tabulating log data: {e}")

# Main function
def main():
    """Load"""
    directory = "remaining"
    file_list = [f.name for f in os.scandir(directory)]
    log_data = load_log_file(file_list, directory)
    
    if log_data is not None:
        tabulated_data = tabulate_log_data(log_data)
        print(tabulated_data)

if __name__ == "__main__":
    main()
