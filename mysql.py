import pymysql.cursors
import json, os, sys, traceback
from functools import reduce

def padding_row (row, lengths):
    result = ''
    for i,data  in enumerate(row):
        data = str(data) if data is not None else ''
        padding = [' '] * (lengths[i] - len(data))
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
        value_lens = [len(str(x)) if x is not None else 0 for x in value]
        lengths = [ x if x > lengths[i] else lengths[i] for i, x in enumerate(value_lens)]
        values.append(value)
    header_str = padding_row(headers, lengths)
    table_line = get_middleline(lengths)
    print(get_topline(lengths))
    print(header_str)
    print(table_line)
    for value in values:
        valueStr = padding_row(value, lengths)
        # print(table_line)
        print(valueStr)
    print(get_bottomline(lengths))

# parse args
if len(sys.argv) < 2:
    print("need sql to continue")
    exit(1)

sql = sys.argv[1]

dir_path = os.path.dirname(os.path.realpath(__file__))
config_path = os.path.join(dir_path, 'config.json')
with open(config_path) as config_file:
    data = json.load(config_file)

connection = pymysql.connect(**data, cursorclass=pymysql.cursors.DictCursor)
try:
    with connection.cursor() as cursor:
        # sql = "select id, username, phone from users order by created_at limit 10;"
        cursor.execute(sql)
        result = cursor.fetchall()
        print_rows(result)
except Exception as e:
    print(traceback.print_exc())
finally:
    connection.close()
