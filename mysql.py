import pymysql.cursors
import json, os, sys, traceback, re
from functools import reduce

def get_str_length (s):
    if s is None:
        return 0
    chinese_len = len([1 for c in str(s) if re.match(r'[\u4e00-\u9fff]', c)])
    non_chinese_len = len(str(s)) - chinese_len
    return chinese_len * 2 + non_chinese_len

def padding_row (row, lengths):
    result = ''
    for i,data  in enumerate(row):
        data = str(data) if data is not None else ''
        padding = [' '] * (lengths[i] - get_str_length(data))
        result = result + data + ''.join(padding) + ' | '
    return '|' + result

def get_middleline (lengths):
    dashes = []
    for length in lengths:
        dashes = dashes + ['-'] * length + ['-', '|', '-']
    del dashes[-1]
    return '|' + ''.join(dashes)

def get_topline (lengths):
    dashes = []
    for length in lengths:
        dashes = dashes + ['-'] * length + ['-', '+', '-']
    dashes.pop()
    dashes.pop()
    return '+' + ''.join(dashes) + '+'

def get_bottomline (lengths):
    dashes = []
    for length in lengths:
        dashes = dashes + ['-'] * length + ['-', '+', '-']
    dashes.pop()
    dashes.pop()
    return '+' + ''.join(dashes) + '+'

def print_rows (rows):
    if len(rows) == 0:
        return
    headers = list(rows[0].keys())
    lengths = [len(x) for x in headers]
    values = []
    for row in rows:
        value = list(row.values())
        value_lens = [get_str_length(x) if x is not None else 0 for x in value]
        lengths = [ x if x > lengths[i] else lengths[i] for i, x in enumerate(value_lens)]
        values.append(value)
    header_str = padding_row(headers, lengths)
    table_line = get_middleline(lengths)
    print('Headers: ' + ','.join(headers))
    print(get_topline(lengths))
    print(header_str)
    print(table_line)
    for value in values:
        valueStr = padding_row(value, lengths)
        # print(table_line)
        print(valueStr)
    print(get_bottomline(lengths))

def print_show_table(result):
    if len(result) < 1:
        return
    create_table = result[0].get('Create Table', '')
    print(create_table)

#support list
def load_config ():
    try:
        dir_path = os.path.dirname(os.path.realpath(__file__))
        config_path = os.path.join(dir_path, 'config.json')
        with open(config_path) as config_file:
            data = json.load(config_file)
            if isinstance(data, list):
                enableData = [x for x in data if x.get('enable', None) == True]
                data = enableData[0] if len(enableData) > 0 else data[0]
                data.pop('enable', None)
            return data
    except Exception as e:
        print(traceback.print_exc())
        exit(1)

def run_sql_query(sql):
    config = load_config()
    connection = pymysql.connect(**config, cursorclass=pymysql.cursors.DictCursor)
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()
            if (re.match(r'^SHOW CREATE TABLE', sql, re.IGNORECASE)):
                print_show_table(result)
            else:
                print_rows(result)
    except Exception as e:
        print(traceback.print_exc())
    finally:
        connection.close()

# parse args
if len(sys.argv) < 2:
    print("need sql to continue")
    exit(1)

sql = sys.argv[1]
run_sql_query(sql)

#TODO: odbc flavor
#TODO: vim refactor vscode mssql
