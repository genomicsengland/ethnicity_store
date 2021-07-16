"""
module to provide functions for creating and accessing the databases
"""

import logging
import os
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import sqlparse
from classes import diversity_db

LOGGER = logging.getLogger(__name__)

DIVERSITY_DB_CREATION_SCRIPT_FP = \
    os.path.abspath('resources/sql_scripts/ethnicity_store.sql')


def assemble_binds(config):
    """
    assemble binds for the different databases of interest
    :returns: dictionary that is passed to session.configure
    """

    binds = {}

    binds[diversity_db.BASE] = get_engine(config.div_db_conn_str)
            
    return binds


def make_session(config):
    """
    get SQLAlchemy session bound to all required DBs
    """

    session = sessionmaker(autoflush=False)
    session.configure(
        binds=assemble_binds(config)
    )

    return session()


def get_engine(conn_str):
    """
    Get an engine for a db
    :param conn_str: the connection string for teh db
    """

    return create_engine(conn_str, echo=False)


def run_sql_query(e, sql):
    """
    run a sql query on an engine
    :params e: the db's engine
    :params sql: the sql query string
    """

    clean_lines = sqlparse.format(sql, strip_comments=True)

    # create connection to db engine
    with e.connect() as db_con:

        # execute then close down
        res = db_con.execute(clean_lines)
        db_con.close()

    return res


def run_sql_file(e, fp):
    """
    run a sql file on an engine
    :params e: the db's engine
    :params fp: the filepath of the sql file
    """

    with open(fp, "r") as f:
        sql = f.read()
        d = run_sql_query(e, sql)

    return d


def create_diversity_db(c):
    """
    create the diversity_db
    :params c: a Config class instance
    """

    LOGGER.info("creating diversity db")

    e = get_engine(c.div_db_conn_str)

    run_sql_file(e, DIVERSITY_DB_CREATION_SCRIPT_FP)


def drop_diversity_db(c):
    """
    drop the diversity_db
    :params c: a Config class instance
    """

    LOGGER.info("dropping the diversity db")

    e = get_engine(c.div_db_conn_str)

    sql = """drop schema ethnicity_store cascade;"""
    run_sql_query(e, sql)

