#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on 010/03/17 at 10:19 PM

@author: neil

Program description here

Version 0.0.0
"""

import numpy as np
from astropy.table import Table
import MySQLdb
import pandas
from collections import OrderedDict
from sqlalchemy import create_engine
from sqlalchemy.types import VARCHAR
from sqlalchemy.types import String


# =============================================================================
# Define variables
# =============================================================================
WORKSPACE = '/Astro/Projects/David_Work/'
# test data 2
DATAPATH = WORKSPACE + 'Data/Catalogues/NJCM_full_V1.vo'
# Name the sql table the catalogue should create
CAT_NAME = 'NJCM_full_V1'
# -----------------------------------------------------------------------------
# set database settings
HOSTNAME = 'localhost'
USERNAME = 'root'
PASSWORD = '1234'
DATABASE = 'astro'
# -----------------------------------------------------------------------------

# =============================================================================
# Define functions
# =============================================================================
def load_db(db_name, host, uname, pword, flavor='mysql+mysqldb'):
    """
    Loads the database using create_engine from sqlalchemy

    :param db_name: string, database name
    :param host: string, database host
    :param uname: string, username
    :param pword: string, password
    :param flavor: string, dialect[+driver]

    ``dialect`` is a database name such as ``mysql``, ``oracle``,
    ``postgresql``, etc.,

    ``driver`` the name of a DBAPI, such as ``mysqldb``, ``psycopg2``,
                                            ``pyodbc``, ``cx_oracle``
    :return:
    """
    # conn1 = MySQLdb.connect(host=host, user=uname, db=db_name,
    #                         connect_timeout=100000, passwd=pword)
    # c1 = conn1.cursor()
    # return c1, conn1
    args = [uname, pword, host, db_name, flavor]
    connstring = "{4}://{0}:{1}@{2}/{3}".format(*args)
    engine = create_engine(connstring, encoding='utf8',convert_unicode=True)
    return engine


def create_data_dictionaries(d):
    """
    Takes an astropy table "d" and extract out the data and meta data
    (units, description and format)

    :param d: astropy table

    :return dictdata: dictionary, keys are column names, with data stored as
                      values for each key (column)

    :return metadata: dictionary, keys are "columns, formats, units, description
                      values are the corresponding data from the astropy table

    """
    formats = dict(f='FLOAT', b='BOOLEAN', i='INTEGER', s='STRING')
    dictdata = OrderedDict()
    col_descs = OrderedDict()
    col_formats = OrderedDict()
    col_units = OrderedDict()
    # iterate around columns to scrape data
    for col in d.colnames:
        dictdata[col] = np.array(d[col].data)
        col_descs[col] = d[col].description
        col_units[col] = d[col].unit

        rawformat = d[col].descr[1]
        for f in formats:
            if f in rawformat:
                 col_formats[col] = formats[f]
        if col not in col_formats:
            col_formats[col] = "STRING"
    # convert to metadata dictionary
    metadata = OrderedDict()
    metadata['columns'] = list(d.colnames)
    metadata['formats'] = list(col_formats.values())
    metadata['units'] = list(col_units.values())
    metadata['description'] = list(col_descs.values())
    # return dictionary of data and metadata
    return dictdata, metadata

# =============================================================================
# Start of code
# =============================================================================
# Main code here
if __name__ == "__main__":
    # -------------------------------------------------------------------------
    # Load data with Astropy table
    print("\n Loading table with astropy (slow)...")
    data = Table.read(DATAPATH)
    # -------------------------------------------------------------------------
    # Convert to pandas dataframe via ordered dictionary
    print("\n Loading data into pandas data frame...")
    ddata, dmeta = create_data_dictionaries(data)
    pandas_data = pandas.DataFrame(ddata)
    pandas_data = pandas_data.replace([np.inf, -np.inf], np.nan)
    # -------------------------------------------------------------------------
    # Create meta data pandas dataframe
    print("\n Loading meta data into pandas data frame...")
    pandas_meta = pandas.DataFrame(dmeta)
    pandas_meta = pandas_meta.replace([np.inf, -np.inf], np.nan)
    # -------------------------------------------------------------------------
    # load database
    # Must have database running
    # mysql -u root -p
    print("\n Connecting to database...")
    engine = load_db(DATABASE, HOSTNAME, USERNAME, PASSWORD)
    # -------------------------------------------------------------------------
    # Convert to pandas data frames via ordered dictionary
    print("\n Loading main catalogue into database...")
    pandas_data.to_sql(CAT_NAME, engine)
    print("\n Loading meta catalogue into database...")
    pandas_meta.to_sql(CAT_NAME + '_meta', engine)

# =============================================================================
# End of code
# =============================================================================
