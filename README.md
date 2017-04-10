# Cook et al. 2017 - Astropy.Table to SQL (MySQL) database converter

  Takes "any" table readable by Table (from Astropy.table import Table)
  and via pandas converts it to a database (tested with MySQL only)
  
  Uses two functions:
  
  ## load_db(db_name, host, uname, pword, flavor='mysql+mysqldb')
  
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
    :return engine: create_engine instance
    
 ## create_data_dictionaries(d)
 
     Takes an astropy table "d" and extract out the data and meta data
    (units, description and format)
    :param d: astropy table
    :return dictdata: dictionary, keys are column names, with data stored as
                      values for each key (column)
    :return metadata: dictionary, keys are "columns, formats, units, description
                      values are the corresponding data from the astropy table
                      
## Example of use

    import numpy as np
    from astropy.table import Table
    import pandas
    from astrotable_to_sql.py import load_db, create_data_dictionaries
    
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
    DATABASE = 'mydatebasename'
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
