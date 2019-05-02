import pymysql.cursors
import json, os, sys, traceback, re
from functools import reduce
from mysql_lib import Config

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

def run_sql_query(sql, conf):
    config = conf.load()
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

def get_all_tables(conf):
    config = conf.load()
    connection = pymysql.connect(**config, cursorclass=pymysql.cursors.DictCursor)
    sql = "SELECT table_name FROM information_schema.tables WHERE table_type = 'base table' AND table_schema='{}'".format(config['db'])
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()
            [print(x['table_name']) for x in result]
    except Exception as e:
        print(traceback.print_exc())
    finally:
        connection.close()

def main():
    # parse args
    if len(sys.argv) < 2:
        print("need sql to continue")
        exit(1)

    dir_path = os.path.dirname(os.path.realpath(__file__))
    config_path = os.path.join(dir_path, 'config.json')
    conf = Config(config_path)

    arg = sys.argv[1]
    if arg == '--table':
        get_all_tables(conf)
    else:
        run_sql_query(arg, conf)

#TODO: odbc flavor
#TODO: vim refactor vscode mssql
#TODO: mysql connector load config file
if __name__ == '__main__':
    main()
