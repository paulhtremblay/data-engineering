import os
import sys
import datetime

class SqlError(Exception):
    pass

def _fix_date_format(date_format = '%Y-%m-%d', **kwargs):
    """
    Transform any Date format to String with %Y-%m-%d format

    Parameters
    ----------

    **kwargs is a dictonary

    Return
    ------

     dictionary, with date as string
    """
    final = {}
    for key in kwargs.keys():
        if isinstance(kwargs[key], datetime.datetime) or \
                isinstance(kwargs[key], datetime.date):
            final[key] = kwargs[key].strftime(date_format)
        else:
            final[key] = kwargs[key]
    return final

def get_sql_string(path, verbose = False,
                   extra_args = {}):
    """
    Read sql from the given file.

    Parameters
    ----------

    dir_name: String, name of directory / dataset name
    table_name: String, name of table
    verbose: Optional Boolean, true if you would like to display the sql.
    sql_string: str, SQL passed directly to function, rather than gotten from a file
    extra_args: dictionary, values for formatting the SQL string

    Return
    ------

    sql_string: str, SQL formatted {x} converted
    """
    if not os.path.isfile(path):
        raise FileNotFoundError(f'no sql for  in path {path}')

    with open(path) as read_obj:
        sql_string = ''.join(read_obj.readlines())

    try:
        sql_string = sql_string.format(**extra_args)
    except KeyError as st:
        raise SqlError('key {k} does not exist'.format(k = st))

    if verbose:
        print('sql is:')
        print(sql_string)

    return sql_string


def get_sql_string_(dir_name = None, table_name = None, verbose = False,
                   extra_args = {}, sql_string = None):
    """
    Read sql from the given file.

    Parameters
    ----------

    dir_name: String, name of directory / dataset name
    table_name: String, name of table
    verbose: Optional Boolean, true if you would like to display the sql.
    sql_string: str, SQL passed directly to function, rather than gotten from a file
    extra_args: dictionary, values for formatting the SQL string

    Return
    ------

    sql_string: str, SQL formatted {x} converted
    """
    if not isinstance(sql_string, str) and (not dir_name or not table_name):
        raise SqlError('must pass sql_string, or (table_name and dir_anme)')

    if not sql_string:
        ext = '.sql'
        if os.path.splitext(table_name)[1] == '.sql':
            ext = ''
        path = '{p}{ext}'.format(p=os.path.join(os.path.dirname(__file__), 'sql', dir_name, table_name),
                                 ext = ext)

        if not os.path.isfile(path):
            raise FileNotFoundError('no sql for {t} in path {p}'.format(t=table_name, p = path))

        with open(path) as read_obj:
            sql_string = ''.join(read_obj.readlines())

    all_args = get_table_variables()
    all_args.update(_fix_date_format(**extra_args))
    try:
        sql_string = sql_string.format(**all_args)
    except KeyError as st:
        raise SqlError('key {k} does not exist'.format(k = st))

    if verbose:
        print('sql is:')
        print(sql_string)

    return sql_string

