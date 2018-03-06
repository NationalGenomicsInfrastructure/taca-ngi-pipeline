__author__ = 'Pontus'

from ngi_pipeline.database import classes as db


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
        raise DatabaseError(ce.message)


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

class statusdb_session(object):
    """Small wrapper class for couchdb utils"""
    def __init__(self, config, db=None):
        import couchdb #importing here just incase not to break other methods
        duser = config.get("username")
        dpwrd = config.get("password")
        dport = config.get("port")
        durl = config.get("url")
        durl_string = "http://{}:{}@{}:{}".format(duser, dpwrd, durl, dport)
        display_url_string = "http://{}:{}@{}:{}".format(duser, "*********", durl, dport)
        self.connection = couchdb.Server(url=durl_string)
        if not self.connection:
            raise("Couchdb connection failed for url {}".format(display_url_string))
        if db:
            self._set_db_connection(db)
    
    def get_project(self, project):
        try:
            proj_db = self.connection["projects"]
            return [proj_db.get(k.id) for k in proj_db.view("project/project_name", reduce=False) if k.key == project][0]
        except Exception as e:
            raise Exception("Failed getting project due to {}".format(e))
    
    def save_db_doc(self, ddoc, db=None):
        try:
            db = db or getattr(self, "db_connection")
            db.save(ddoc)
        except Exception as e:
            raise Exception("Failed saving document due to {}".format(e))
    
    def _set_db_connection(self, db):
        self.db_connection = self.connection[db]

