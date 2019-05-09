from mysqlpy import connect, cursors
import json, os, sys, traceback, re
from functools import reduce
from mysql_lib import Config, Connection, DBConfig
from multiprocessing.pool import ThreadPool

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

def run_sql_query(sql, config):
    connection = connect(**config, cursorclass=cursors.DictCursor)
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
    connection = connect(**config, cursorclass=cursors.DictCursor)
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

def load_config():
    dir_path = os.path.dirname(os.path.realpath(__file__))
    config_path = os.path.join(dir_path, 'config.json')
    config = DBConfig.load(config_path)
    return config


#TODO: with
class MySQLExecutor(object):

    def __init__(self, config=None):
        self.config = config if config is not None else load_config()

    def execute_query(self, sql, print_table=True):
        connection = Connection(self.config)
        try:
                result = connection.run_sql(sql)
                if (re.match(r'^SHOW CREATE TABLE', sql, re.IGNORECASE)):
                    [print(row[1]) for row in result.rows if print_table]
                    return result.rows[0][1]
                return result
        except Exception as e:
            print(traceback.print_exc())
        finally:
            connection.close()

    def get_all_tables(self, print_table=True):
        connection = Connection(self.config)
        sql = "SELECT table_name FROM information_schema.tables WHERE table_type = 'base table' AND table_schema='{}'".format(self.config['db'])
        try:
                result = connection.run_sql(sql)
                [print(x[0]) for x in result.rows if print_table]
                return [x[0] for x in result.rows]
        except Exception as e:
            print(traceback.print_exc())
        finally:
            connection.close()

    def synchronize_database_columns(self, data_path):
        tables = self.get_all_tables(print_table=False)
        pool = ThreadPool(processes=12)
        columns = pool.map(self.get_table_column, tables, chunksize=12)
        with open('columns.csv', 'w+') as f:
            for column in columns:
                f.write('{}\r\n'.format(','.join(column)))
        # print(tables)

    def get_table_column(self, table):
        result = [table, '0']
        try:
            sql = 'describe {}'.format(table)
            table_describe = self.execute_query(sql, print_table=False)
            columns = [row[0] for row in table_describe.rows]
            result =  [table, str(len(columns))] + columns
        except:
            pass
        return result

def main():
    # parse args
    if len(sys.argv) < 2:
        print("need sql to continue")
        exit(1)

    dir_path = os.path.dirname(os.path.realpath(__file__))
    config_path = os.path.join(dir_path, 'config.json')
    config = Config(config_path).load()

    executor = MySQLExecutor()

    arg1 = sys.argv[1]
    if arg1 == '--table':
        executor.get_all_tables()
    elif arg1 == '--sync':
        # data_path = os.path.join(dir_path, '.data', '{}_columns'.format(config['db']))
        data_path = sys.argv[2] if len(sys.argv) > 2 else None
        executor.synchronize_database_columns(data_path)
    elif (re.match(r'^SHOW CREATE TABLE', arg1, re.IGNORECASE)):
        executor.execute_query(arg1)
    else:
        run_sql_query(arg1, config)

#TODO: odbc flavor
#TODO: vim refactor vscode mssql
#TODO: mysql connector load config file
if __name__ == '__main__':
    main()
