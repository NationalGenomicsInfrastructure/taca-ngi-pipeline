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
    """Small wrapper class for couchdb utils. Made it as class to allow room for expansion"""
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
            raise Exception("Couchdb connection failed for url {}".format(display_url_string))
        if db:
            self.db_connection = self.connection[db]

    def get_entry(self, name, use_id_view=False, log=None):
        """Retrieve entry from given db for a given name.

        :param name: unique name identifier (primary key, not the uuid)
        :param db: name of db to fetch data from
        """
        if use_id_view:
            view = self.id_view
        else:
            view = self.name_view
        if not view.get(name, None):
            if log:
                log.warn("no entry '{}' in {}".format(name, self.db))
            return None
        return self.db.get(view.get(name))

    def save_db_doc(self, ddoc, db=None):
        try:
            db = db or self.db
            db.save(ddoc)
        except Exception as e:
            raise Exception("Failed saving document due to {}".format(e))

    def get_project_flowcell(self, project_id, open_date="2015-01-01", date_format="%Y-%m-%d"):
        """From information available in flowcell db connection collect the flowcell this project was sequenced

        :param project_id: NGI project ID to get the flowcells
        :param open_date: Open date of project to skip the check for all flowcells
        :param date_format: The format of specified open_date
        """
        try:
            open_date = datetime.strptime(open_date, date_format)
        except:
            open_date = datetime.strptime("2015-01-01", "%Y-%m-%d")

        project_flowcells = {}
        date_sorted_fcs = sorted(self.proj_list.keys(), key=lambda k: datetime.strptime(k.split('_')[0], "%y%m%d"), reverse=True)
        for fc in date_sorted_fcs:
            fc_date, fc_name = fc.split('_')
            if datetime.strptime(fc_date,'%y%m%d') < open_date:
                break
            if project_id in self.proj_list[fc] and fc_name not in project_flowcells.keys():
                project_flowcells[fc_name] = {'name':fc_name,'run_name':fc, 'date':fc_date, 'db':self.db.name}

        return project_flowcells

class ProjectSummaryConnection(statusdb_session):
    def __init__(self, config, dbname="projects"):
        super(ProjectSummaryConnection, self).__init__(config)
        self.db = self.connection[dbname]
        self.name_view = {k.key:k.id for k in self.db.view("project/project_name", reduce=False)}
        self.id_view = {k.key:k.id for k in self.db.view("project/project_id", reduce=False)}

class SampleRunMetricsConnection(statusdb_session):
    def __init__(self, config, dbname="samples"):
        super(SampleRunMetricsConnection, self).__init__(config)
        self.db = self.connection[dbname]

class FlowcellRunMetricsConnection(statusdb_session):
    def __init__(self, config, dbname="flowcells"):
        super(FlowcellRunMetricsConnection, self).__init__(config)
        self.db = self.connection[dbname]
        self.name_view = {k.key:k.id for k in self.db.view("names/name", reduce=False)}
        self.proj_list = {k.key:k.value for k in self.db.view("names/project_ids_list", reduce=False) if k.key}

class X_FlowcellRunMetricsConnection(statusdb_session):
    def __init__(self, config, dbname="x_flowcells"):
        super(X_FlowcellRunMetricsConnection, self).__init__(config)
        self.db = self.connection[dbname]
        self.name_view = {k.key:k.id for k in self.db.view("names/name", reduce=False)}
        self.proj_list = {k.key:k.value for k in self.db.view("names/project_ids_list", reduce=False) if k.key}
