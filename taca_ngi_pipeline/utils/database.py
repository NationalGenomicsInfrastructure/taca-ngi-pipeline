__author__ = 'Pontus'

from ngi_pipeline.database import classes as db
from datetime import datetime

class DatabaseError(Exception):
    pass


def _wrap_database_query(query_fn, *query_args, **query_kwargs):
    """ Wrapper calling the supplied method with the supplied arguments
        :param query_fn: function reference in the CharonSession class that
            will be called
        :returns: the result of the function call
        :raises DatabaseError:
            if an error occurred when communicating with the database
    """
    try:
        return query_fn(*query_args, **query_kwargs)
    except db.CharonError as ce:
        raise DatabaseError(ce)


def dbcon():
    """ Establish a CharonSession
        :returns: a ngi_pipeline.database.classes.CharonSession instance
    """
    return db.CharonSession()


def project_entry(dbc, projectid):
    """ Fetch a database entry representing the instance's project
        :returns: a json-formatted database entry
        :raises DatabaseError:
            if an error occurred when communicating with the database
    """
    return _wrap_database_query(dbc.project_get, projectid)


def project_sample_entries(dbc, projectid):
    """ Fetch the database sample entries representing the instance's project
        :returns: a list of json-formatted database sample entries
        :raises DatabaseError:
            if an error occurred when communicating with the database
    """
    return _wrap_database_query(dbc.project_get_samples, projectid)


def sample_entry(dbc, projectid, sampleid):
    """ Fetch a database entry representing the instance's project
        :returns: a json-formatted database entry
        :raises DatabaseError:
            if an error occurred when communicating with the database
    """
    return _wrap_database_query(dbc.sample_get, projectid, sampleid)


def update_project(dbc, projectid, **kwargs):
    """
    :param dbc: a valid database session
    :param projectid: the id of the project to update
    :param kwargs: the database fields to update are specified as keyword arguments
    :return: the result from the underlying API call
    :raises DatabaseError: if an error occurred when communicating with the database
    """
    return _wrap_database_query(dbc.project_update, projectid, **kwargs)


def update_sample(dbc, projectid, sampleid, **kwargs):
    """
    :param dbc: a valid database session
    :param projectid: the id of the project to update
    :param sampleid: the id of the sample to update
    :param kwargs: the database fields to update are specified as keyword arguments
    :return: the result from the underlying API call
    :raises DatabaseError: if an error occurred when communicating with the database
    """
    return _wrap_database_query(dbc.sample_update, projectid, sampleid, **kwargs)