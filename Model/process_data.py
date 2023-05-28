import sys
sys.path.append('../')

import pandas as pd
import os
import string
import re
from Data import reddit_data

def process_data(df: pd.DataFrame):
    df = df.dropna()
    df.loc[:, 'comments'] = df['comments'].str.replace('-', '---')
    df.loc[:, 'comments'] = '---' + df['comments']
    df.loc[:, 'title'] = '-----' + df['title']
    chrs_to_keep = string.ascii_lowercase + string.digits + ' ' + '-'
    
    combined_string = df['title'].str.cat(df['comments'], sep='')
    result_string = ''.join(combined_string)
    
    result_string = result_string.lower()
    result_string = ''.join([c for c in result_string if c in chrs_to_keep])
    result_string = re.sub(r"\s+", " ", result_string).strip()
    result_string = re.sub(r'\s*-\s*', '-', result_string)
    result_string += '-----'
    
    with open('processed_data.txt', 'w') as f:
        f.write(result_string)
        
    return 'Success'

if __name__ == '__main__':
    
    dfu = reddit_data.DFUtils()
    df = dfu.get_df()
    process_data(df)