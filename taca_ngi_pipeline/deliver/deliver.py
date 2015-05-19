"""
    Module for controlling deliveries of samples and projects
"""
import datetime
import glob
import logging
import os
import re

from ngi_pipeline.database import classes as db
from ngi_pipeline.utils.classes import memoized
    
from taca.utils.config import CONFIG
from taca.utils.filesystem import create_folder
from taca.utils.misc import hashfile
from taca.utils import transfer

from taca_ngi_pipeline.utils import getLogger

logger = getLogger(__name__)

class DelivererError(Exception): pass
class DelivererDatabaseError(DelivererError): pass
class DelivererReplaceError(DelivererError): pass
class DelivererRsyncError(DelivererError): pass

class Deliverer(object):
    """ 
        A (abstract) superclass with functionality for handling deliveries
    """
    def __init__(self, projectid, sampleid, **kwargs):
        """
            :param string projectid: id of project to deliver
            :param string sampleid: id of sample to deliver
            :param bool no_checksum: if True, skip the checksum computation
            :param string hash_algorithm: algorithm to use for calculating 
                file checksums, defaults to sha1
        """
        # override configuration options with options given on the command line
        self.config = CONFIG.get('deliver',{})
        self.config.update(kwargs)
        # set items in the configuration as attributes
        for k, v in self.config.items():
            setattr(self,k,v)
        self.projectid = projectid
        self.sampleid = sampleid
        self.hash_algorithm = getattr(
            self,'hash_algorithm','sha1')
        self.no_checksum = getattr(
            self,'no_checksum',False)
        # only set an attribute for uppnexid if it's actually given or in the db
        try:
            self.uppnexid = getattr(
                self,'uppnexid',self.project_entry()['uppnex_id'])
        except KeyError:
            pass

    def __str__(self):
        return "{}:{}".format(
            self.projectid,self.sampleid) \
            if self.sampleid is not None else self.projectid

    @memoized
    def dbcon(self):
        """ Establish a CharonSession
            :returns: a ngi_pipeline.database.classes.CharonSession instance
        """
        return db.CharonSession()

    @memoized
    def project_entry(self):
        """ Fetch a database entry representing the instance's project
            :returns: a json-formatted database entry
            :raises DelivererDatabaseError: 
                if an error occurred when communicating with the database
        """
        return self.wrap_database_query(
            self.dbcon().project_get,self.projectid)

    @memoized
    def project_sample_entries(self):
        """ Fetch the database sample entries representing the instance's project
            :returns: a list of json-formatted database sample entries
            :raises DelivererDatabaseError: 
                if an error occurred when communicating with the database
        """
        return self.wrap_database_query(
            self.dbcon().project_get_samples,self.projectid)

    @memoized
    def sample_entry(self):
        """ Fetch a database entry representing the instance's project and sample
            :returns: a json-formatted database entry
            :raises DelivererDatabaseError: 
                if an error occurred when communicating with the database
        """
        return self.wrap_database_query(
            self.dbcon().sample_get,self.projectid,self.sampleid)
        
    def update_delivery_status(self, *args, **kwargs):
        """ Abstract method, should be implemented by subclasses """
        raise NotImplementedError("This method should be implemented by "\
        "subclass")
    
    def wrap_database_query(self,query_fn,*query_args,**query_kwargs):
        """ Wrapper calling the supplied method with the supplied arguments
            :param query_fn: function reference in the CharonSession class that
                will be called
            :returns: the result of the function call
            :raises DelivererDatabaseError: 
                if an error occurred when communicating with the database
        """
        try:
            return query_fn(*query_args,**query_kwargs)
        except db.CharonError as ce:
            raise DelivererDatabaseError(ce.message)
            
    def gather_files(self):
        """ This method will locate files matching the patterns specified in 
            the config and compute the checksum and construct the staging path
            according to the config.
            
            The config should contain the key 'files_to_deliver', which should
            be a list of tuples with source path patterns and destination path
            patterns. The source path can be a file glob and can refer to a 
            folder or file. File globs will be expanded and folders will be
            traversed to include everything beneath.
             
            :returns: A generator of tuples with source path, 
                destination path and the checksum of the source file 
                (or None if source is a folder)
        """
        def get_hash(sourcepath,destpath):
            hash = None
            checksumpath = "{}.{}".format(sourcepath,self.hash_algorithm)
            if not os.path.exists(checksumpath):
                if not self.no_checksum:
                    hash = hashfile(sourcepath,hasher=self.hash_algorithm)
                    with open(checksumpath,'w') as fh:
                        fh.write(hash)
            else:
                with open(checksumpath,'r') as fh:
                    hash = fh.next()
            return (sourcepath,destpath,hash)
                    
        for sfile, dfile in getattr(self,'files_to_deliver',[]):
            dest_path = self.expand_path(dfile)
            for f in glob.iglob(self.expand_path(sfile)):
                if (os.path.isdir(f)):
                    fparent = os.path.dirname(f)
                    # walk over all folders and files below
                    for pdir,_,files in os.walk(f):
                        for current in files:
                            fpath = os.path.join(pdir,current)
                            # use the relative path for the destination path
                            fname = os.path.relpath(fpath,fparent)
                            yield get_hash(fpath,
                                os.path.join(dest_path,fname))
                else:
                    yield get_hash(f,
                        os.path.join(dest_path,os.path.basename(f)))
    
    def stage_delivery(self):
        """ Stage a delivery by symlinking source paths to destination paths 
            according to the returned tuples from the gather_files function. 
            Checksums will be written to a digest file in the staging path. 
            Failure to stage individual files will be logged as warnings but will
            not terminate the staging. 
            
            :raises DelivererError: if an unexpected error occurred
        """
        digestpath = self.staging_digestfile()
        create_folder(os.path.dirname(digestpath))
        try: 
            with open(digestpath,'w') as dh:
                agent = transfer.SymlinkAgent(None, None, relative=True)
                for src, dst, digest in self.gather_files():
                    agent.src_path = src
                    agent.dest_path = dst
                    try:
                        agent.transfer()
                    except (transfer.TransferError, transfer.SymlinkError) as e:
                        logger.warning("failed to stage file '{}' when "\
                            "delivering {} - reason: {}".format(
                                src,str(self),e))
                    if digest is not None:
                        dh.write("{}  {}\n".format(
                            digest,
                            os.path.relpath(
                                dst,self.expand_path(self.stagingpath))))
        except IOError as e:
            raise DelivererError(
                "failed to stage delivery - reason: {}".format(e))
        return True

    def delivered_digestfile(self):
        """
            :returns: path to the file with checksums after delivery
        """
        return self.expand_path(
            os.path.join(
                self.deliverypath,
                self.projectid,
                os.path.basename(self.staging_digestfile())))

    def staging_digestfile(self):
        """
            :returns: path to the file with checksums after staging
        """
        return self.expand_path(
            os.path.join(
                self.stagingpath,
                "{}.{}".format(self.sampleid,self.hash_algorithm)))

    def transfer_log(self):
        """
            :returns: path prefix to the transfer log files. The suffixes will
                be created by the transfer command
        """
        return self.expand_path(
            os.path.join(
                self.stagingpath,
                "{}_{}".format(self.sampleid,
                    datetime.datetime.now().strftime("%Y%m%dT%H%M%S"))))
                
    @memoized
    def expand_path(self,path):
        """ Will expand a path by replacing placeholders with correspondingly 
            named attributes belonging to this Deliverer instance. Placeholders
            are specified according to the pattern '_[A-Z]_' and the 
            corresponding attribute that will replace the placeholder should be
            identically named but with all lowercase letters.
            
            For example, "this/is/a/path/to/_PROJECTID_/and/_SAMPLEID_" will
            expand by substituting _PROJECTID_ with self.projectid and 
            _SAMPLEID_ with self.sampleid
            
            If the supplied path does not contain any placeholders or is None,
            it will be returned unchanged.
            
            :params string path: the path to expand
            :returns: the supplied path will all placeholders substituted with
                the corresponding instance attributes
            :raises DelivererError: if a corresponding attribute for a 
                placeholder could not be found
        """
        try:
            m = re.search(r'(_[A-Z]+_)',path)
        except TypeError:
            return path
        else:
            if m is None:
                return path
            try:
                expr = m.group(0)
                return self.expand_path(
                    path.replace(expr,getattr(self,expr[1:-1].lower())))
            except AttributeError as e:
                raise DelivererError(
                    "the path '{}' could not be expanded - reason: {}".format(
                        path,e))
    
class ProjectDeliverer(Deliverer):
    
    def __init__(self, projectid=None, sampleid=None, **kwargs):
        super(ProjectDeliverer,self).__init__(
            projectid,
            sampleid,
            **kwargs)
    
    def deliver_project(self):
        """ Deliver all samples in a project to the destination specified by 
            deliverypath
            
            :returns: True if all samples were delivered successfully, False if
                any sample was not properly delivered or ready to be delivered
        """
        logger.info("Delivering {} to {}".format(str(self),self.deliverypath))
        try:
            sampleentries = self.project_sample_entries()
        except DelivererDatabaseError as e:
            logger.error("error '{}' occurred during delivery of {}"
                         .format(str(e), str(self)))
            raise
        # right now, don't catch any errors since we're assuming any thrown 
        # errors needs to be handled by manual intervention
        status = True
        for sampleentry in sampleentries.get('samples',[]):
            st = SampleDeliverer(
                self.projectid,sampleentry.get('sampleid')
            ).deliver_sample(sampleentry)
            status = (status and st)
            
        if status:
            self.update_delivery_status()
        return status

    def update_delivery_status(self, status="DELIVERED"):
        """ Update the delivery_status field in the database to the supplied 
            status for the project specified by this instance
            :returns: the result from the underlying api call
            :raises DelivererDatabaseError: 
                if an error occurred when communicating with the database
        """
        return self.wrap_database_query(
            self.dbcon().project_update,
            self.projectid,
            delivery_status=status)
            
class SampleDeliverer(Deliverer):
    """
        A class for handling sample deliveries
    """
    def __init__(self, projectid=None, sampleid=None, **kwargs):
        super(SampleDeliverer,self).__init__(
            projectid,
            sampleid,
            **kwargs)
        
    def deliver_sample(self, sampleentry=None):
        """ Deliver a sample to the destination specified by the config.
            Will check if the sample has already been delivered and should not 
            be delivered again or if the sample is not yet ready to be delivered.
            
            :params sampleentry: a database sample entry to use for delivery
                but not sent to the receiver
            :returns: True if sample was successfully delivered or was previously 
                delivered, False if sample was not yet ready to be delivered
            :raises DelivererDatabaseError: if an entry corresponding to this
                sample could not be found in the database
            :raises DelivererReplaceError: if a previous delivery of this sample
                has taken place but should be replaced
            :raises DelivererError: if the delivery failed
        """
        logger.info("Delivering {} to {}".format(str(self), self.deliverypath))
        try:
            sampleentry = sampleentry or self.sample_entry()
        except DelivererDatabaseError as e:
            logger.error(
                "error '{}' occurred during delivery of {}".format(
                    str(e),str(self)))
            raise
        if sampleentry.get('delivery_status') == 'DELIVERED' and not self.force:
            logger.info("{} has already been delivered".format(str(self)))
            return True
        elif sampleentry.get('delivery_status') == 'DELIVERING' \
            and not self.force:
            logger.info("delivery of {} is already in progress".format(str(self)))
            return False
        elif sampleentry.get('analysis_status') != 'ANALYZED' and not self.force:
            logger.info("{} has not finished analysis and will not be "\
                "delivered".format(str(self)))
            return False
        else:
            # Propagate raised errors upwards, they should trigger 
            # notification to operator
            if not self.stage_delivery():
                raise DelivererError("sample was not properly staged")
            logger.info("{} successfully staged".format(str(self)))
            if not self.stage_only:
                if not self.do_delivery():
                    raise DelivererError("sample was not properly delivered")
                logger.info("{} successfully delivered".format(str(self)))
                self.update_delivery_status()
            return True
    
    def do_delivery(self):
        """ Deliver the staged delivery folder using rsync
            :returns: True if delivery was successful, False if unsuccessful
            :raises DelivererRsyncError: if an exception occurred during
                transfer
        """
        agent = transfer.RsyncAgent(
            self.expand_path(self.stagingpath),
            dest_path=self.expand_path(self.deliverypath),
            digestfile=self.delivered_digestfile(),
            remote_host=getattr(self,'remote_host', None), 
            remote_user=getattr(self,'remote_user', None), 
            log=logger,
            opts={
                '--copy-links': None,
                '--recursive': None,
                '--perms': None,
                '--chmod': 'u+rwX,og-rwx',
                '--verbose': None,
                '--exclude': ["*rsync.out","*rsync.err"]
            })
        try:
            return agent.transfer(transfer_log=self.transfer_log())
        except transfer.TransferError as e:
            raise DelivererRsyncError(e)
    
    def update_delivery_status(self, status="DELIVERED"):
        """ Update the delivery_status field in the database to the supplied 
            status for the project and sample specified by this instance
            :returns: the result from the underlying api call
            :raises DelivererDatabaseError: 
                if an error occurred when communicating with the database
        """
        return self.wrap_database_query(
            self.dbcon().sample_update,
            self.projectid,
            self.sampleid,
            delivery_status=status)
            
            
