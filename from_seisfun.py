"""
Функции из библиотеки seisfun
find_file_upper()
"""

import os
import configparser
import psycopg2


def find_file_upper(filename: str = None, basepath: str = None, **kwargs):
    """
    Поиск файла от текущей директории и вверх
    :param filename:
    :param basepath: директория с которой начинать искать файл
    :return:
    """
    message_show = kwargs.get('message_show', False)
    #
    if basepath is not None:
        pardir = basepath
    else:
        pardir = os.getcwd()
    #
    while os.path.exists(pardir):
        if os.path.exists(os.path.abspath(os.path.join(pardir, filename))):
            return os.path.abspath(os.path.join(pardir, filename))
        if pardir == os.path.dirname(pardir):
            break
        pardir = os.path.dirname(pardir)
    #
    if message_show:
        print(f"Файл {filename} не найден")
    return None
# --------------------------------------------------------------

def config_read(filename: str = 'config.ini', encoding: str = 'utf-8', **kwargs):
    """

    :param filename:
    :param encoding:
    :return:
    """
    message_show = kwargs.get('message_show', False)
    filename = find_file_upper(filename, message_show=message_show)
    if filename is not None:
        config = configparser.ConfigParser()
        config.read(filename, encoding=encoding)
    else:
        config = None
        if message_show:
            print(f"Конфигурационный файл не загружен")
    return config
# --------------------------------------------------------------

def query_postgresql(query: str = '', options: dict = None):
    """
    Загрузка данных из базы данных (PostGreSQL) ЕИССД
    query for SDIS
    :param query:
    :param options:
    :return:
    """
    # result type of list
    if options is None:
        options = {}
    if 'access' not in options or options['access'] is None:
        access = "config.ini"
    else:
        access = options['access']
    #
    results = None
    field_names = None
    if type(access) is str:
        access = config_read(access)  # ищем в корневой папке, в папке на (один/два) уровня выше
        if access is None:
            msg = f"File {access} not found"
            print(msg)
            return None
        if access.has_section('pg_read'):
            access = {j[0]: j[1] for j in access.items('pg_read')}
        else:
            print("the file with database access settings does not contain the required parameters")

    if type(access) is dict and 'dbname' in access and 'user' in access and 'password' in access \
            and 'host' in access and 'port' in access:
        connection = False
        cursor = False
        try:
            connection = psycopg2.connect(database=access['dbname'],
                                          user=access['user'],
                                          password=access['password'],
                                          host=access['host'],
                                          port=access['port'])
            cursor = connection.cursor()
            cursor.execute(query)
            # изменения фиксируются и сразу же сохраняются в базе данных (вроде по умолчанию и так сохраняет)
            connection.commit()
            results = cursor.fetchall()
            field_names = [desc[0] for desc in cursor.description]
        except (Exception, psycopg2.DatabaseError) as error:
            msg = f"Error while connecting to PostGreSQL. {error}"
            print(msg)
            if connection:
                #  отменить изменения в базе данных (вроде close() неявно вызывает эту функцию)
                connection.rollback()
        finally:
            # closing database connection.
            if connection:
                cursor.close()
                connection.close()
                # print("PostGreSQL connection is closed")
    else:
        print("missing parameters for database access")

    if 'typeOut' in options:
        if type(results) is list:
            if options['typeOut'] == 'dictionaryFromSElECT' or options['typeOut'] == 'dictionary':
                # переформируем результат запроса в список со словарями с именами полей из выборки
                results_dict = []
                for line in results:
                    j = 0
                    line_dict = {}
                    for value in line:
                        line_dict[field_names[j]] = value
                        j += 1
                    results_dict.append(line_dict)
                results = results_dict
            elif options['typeOut'] == 'first_field':
                # переформируем результат запроса в список со словарями с именами полей из выборки
                results_list = []
                for line in results:
                    results_list.append(line[0])
                results = results_list
            elif options['typeOut'] == 'key_value':
                # переформируем результат запроса в словарь имя значение
                results_list = {}
                for line in results:
                    results_list[line[0]] = line[1]
                results = results_list
            elif options['typeOut'] == 'first_value':
                if len(results) > 0 and len(results[0]) > 0:
                    results = results[0][0]
                else:
                    results = None
            elif options['typeOut'] == 'SELECT':
                print(query)
            else:
                print(f'query_postgresql: unknown typeOut = {options["typeOut"]}')
    return results
# --------------------------------------------------------------
