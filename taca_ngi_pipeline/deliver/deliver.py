"""
    Module for controlling deliveries of samples and projects
"""
import couchdb
import datetime
import glob
import json
import logging
import os
import re
import signal
import shutil
import yaml

from taca.utils.config import CONFIG
from taca.utils.filesystem import create_folder, chdir
from taca.utils.misc import call_external_command
from taca.utils import transfer
from ..utils import database as db
from ..utils import filesystem as fs

logger = logging.getLogger(__name__)


class DelivererError(Exception):
    pass


class DelivererInterruptedError(DelivererError):
    pass


class DelivererReplaceError(DelivererError):
    pass


class DelivererRsyncError(DelivererError):
    pass


def _signal_handler(sgnal, frame):
    """ A custom signal handler which will raise a DelivererInterruptedError
        :raises DelivererInterruptedError: 
            this exception will be raised
    """
    raise DelivererInterruptedError(
        "interrupt signal {} received while delivering".format(sgnal))


def _timestamp(days=None):
    """Current date and time (UTC) in ISO format, with millisecond precision.
    Add the specified offset in days, if given.
    Stolen from https://github.com/NationalGenomicsInfrastructure/charon/blob/master/charon/utils.py
    """
    instant = datetime.datetime.utcnow()
    if days:
        instant += datetime.timedelta(days=days)
    instant = instant.isoformat()
    return instant[:-9] + "%06.3f" % float(instant[-9:]) + "Z"


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
        self.config = CONFIG.get('deliver', {})
        self.config.update(kwargs)
        # set items in the configuration as attributes
        for k, v in self.config.items():
            setattr(self, k, v)
        self.projectid = projectid
        self.sampleid = sampleid
        self.hash_algorithm = getattr(self, 'hash_algorithm', 'sha1')
        self.no_checksum = getattr(self, 'no_checksum', False)
        self.files_to_deliver = getattr(self, 'files_to_deliver', None)
        self.deliverystatuspath = getattr(self, 'deliverystatuspath', None)
        self.stagingpath = getattr(self, 'stagingpath', None)
        self.deliverypath = getattr(self, 'deliverypath', None)
        self.logpath = getattr(self, 'logpath', None)
        self.reportpath = getattr(self, 'reportpath', None)
        self.force = getattr(self, 'force', False)
        self.stage_only = getattr(self, 'stage_only', False)
        self.ignore_analysis_status = getattr(self, 'ignore_analysis_status', False)
        #Fetches a project name, should always be availble; but is not a requirement
        try:
            self.projectname = db.project_entry(db.dbcon(), projectid)['name']
        except KeyError:
            pass
        # only set an attribute for uppnexid if it's actually given or in the db
        try:
            getattr(self, 'uppnexid')
        except AttributeError:
            try:
                self.uppnexid = db.project_entry(db.dbcon(), projectid)['uppnex_id']
            except KeyError:
                pass
        # set a custom signal handler to intercept interruptions
        signal.signal(signal.SIGINT, _signal_handler)
        signal.signal(signal.SIGTERM, _signal_handler)

    def __str__(self):
        return "{}:{}".format(
            self.projectid, self.sampleid) \
            if self.sampleid is not None else self.projectid

    def acknowledge_delivery(self, tstamp=_timestamp()):
        try:
            ackfile = self.expand_path(
                os.path.join(self.deliverystatuspath, "{}_delivered.ack".format(
                    self.sampleid or self.projectid)))
            create_folder(os.path.dirname(ackfile))
            with open(ackfile, 'w') as fh:
                fh.write("{}\n".format(tstamp))
        except (AttributeError, IOError) as e:
            logger.warning(
                "could not write delivery acknowledgement, reason: {}".format(
                    e))

    def db_entry(self):
        """ Abstract method, should be implemented by subclasses """
        raise NotImplementedError("This method should be implemented by subclass")

    def get_sample_status(self, dbentry=None):
        """ Returns the analysis status for this sample. If a sampleentry
            dict is supplied, it will be used instead of fethcing from database

            :params sampleentry: a database sample entry to use instead of
                fetching from db
            :returns: the analysis status of this sample as a string
        """
        dbentry = dbentry or self.db_entry()
        return dbentry.get('status', 'FRESH')

    def update_delivery_status(self, *args, **kwargs):
        """ Abstract method, should be implemented by subclasses """
        raise NotImplementedError("This method should be implemented by subclass")

    def get_analysis_status(self, dbentry=None):
        """ Returns the analysis status for this sample. If a sampleentry
            dict is supplied, it will be used instead of fethcing from database

            :params sampleentry: a database sample entry to use instead of
                fetching from db
            :returns: the analysis status of this sample as a string
        """
        dbentry = dbentry or self.db_entry()
        return dbentry.get('analysis_status', 'TO_ANALYZE')

    def get_delivery_status(self, dbentry=None):
        """ Returns the delivery status for this sample. If a sampleentry
            dict is supplied, it will be used instead of fethcing from database

            :params sampleentry: a database sample entry to use instead of
                fetching from db
            :returns: the delivery status of this sample as a string
        """
        dbentry = dbentry or self.db_entry()
        return dbentry.get('delivery_status', 'NOT_DELIVERED')

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
        return fs.gather_files([map(self.expand_path, file_pattern) for file_pattern in self.files_to_deliver],
                               no_checksum=self.no_checksum,
                               hash_algorithm=self.hash_algorithm)

    def stage_delivery(self):
        """ Stage a delivery by symlinking source paths to destination paths 
            according to the returned tuples from the gather_files function. 
            Checksums will be written to a digest file in the staging path. 
            Failure to stage individual files will be logged as warnings but will
            not terminate the staging. 
            
            :raises DelivererError: if an unexpected error occurred
        """
        digestpath = self.staging_digestfile()
        filelistpath = self.staging_filelist()
        create_folder(os.path.dirname(digestpath))
        try:
            with open(digestpath, 'w') as dh, open(filelistpath, 'w') as fh:
                agent = transfer.SymlinkAgent(None, None, relative=True)
                for src, dst, digest in self.gather_files():
                    agent.src_path = src
                    agent.dest_path = dst
                    try:
                        agent.transfer()
                    except (transfer.TransferError, transfer.SymlinkError) as e:
                        logger.warning("failed to stage file '{}' when "
                                       "delivering {} - reason: {}".format(src, str(self), e))

                    fpath = os.path.relpath(dst, self.expand_path(self.stagingpath))
                    fh.write("{}\n".format(fpath))
                    if digest is not None:
                        dh.write("{}  {}\n".format(digest, fpath))
                # finally, include the digestfile in the list of files to deliver
                fh.write("{}\n".format(os.path.basename(digestpath)))
        except (IOError, fs.FileNotFoundException, fs.PatternNotMatchedException) as e:
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
                os.path.basename(self.staging_digestfile())))

    def staging_digestfile(self):
        """
            :returns: path to the file with checksums after staging
        """
        return self.expand_path(
            os.path.join(
                self.stagingpath,
                "{}.{}".format(self.sampleid, self.hash_algorithm)))

    def staging_filelist(self):
        """
            :returns: path to the file with a list of files to transfer
                after staging
        """
        return self.expand_path(
            os.path.join(
                self.stagingpath,
                "{}.lst".format(self.sampleid)))
 
    def transfer_log(self):
        """
            :returns: path prefix to the transfer log files. The suffixes will
                be created by the transfer command
        """
        return self.expand_path(
            os.path.join(
                self.logpath,
                "{}_{}".format(self.sampleid,
                               datetime.datetime.now().strftime("%Y%m%dT%H%M%S"))))

    def expand_path(self, path):
        """ Will expand a path by replacing placeholders with correspondingly 
            named attributes belonging to this Deliverer instance. Placeholders
            are specified according to the pattern '<[A-Z]>' and the
            corresponding attribute that will replace the placeholder should be
            identically named but with all lowercase letters.
            
            For example, "this/is/a/path/to/<PROJECTID>/and/<SAMPLEID>" will
            expand by substituting <PROJECTID> with self.projectid and
            <SAMPLEID> with self.sampleid
            
            If the supplied path does not contain any placeholders or is None,
            it will be returned unchanged.
            
            :params string path: the path to expand
            :returns: the supplied path will all placeholders substituted with
                the corresponding instance attributes
            :raises DelivererError: if a corresponding attribute for a 
                placeholder could not be found
        """
        try:
            m = re.search(r'(<[A-Z]+>)', path)
        except TypeError:
            return path
        else:
            if m is None:
                return path
            try:
                expr = m.group(0)
                return self.expand_path(
                    path.replace(expr, getattr(self, str(expr[1:-1]).lower())))
            except AttributeError as e:
                raise DelivererError(
                    "the path '{}' could not be expanded - reason: {}".format(
                        path, e))

    def aggregate_meta_info(self):
        """ A method to collect meta info about delivered files (like size, md5 value)
            Which files are interested (by default only 'fastq' and 'bam' files) can be
            controlled by setting 'files_interested' in 'aggregate_meta_info' section.
            It needs a database credentials file to put the aggregated info.
        """
        control_dict = getattr(self, 'save_meta_info', {})
        # No appropriate section found, move on silently
        if not control_dict:
            return

        def _parse_hash_file(hfile, interested_files):
            """Parse the hash file and return dict with hash value and file size
               Files are grouped based on parent directory relative to stage
               Only saves the meta info for 'interested_files'
            """
            mdict = {}
            with open(hfile, 'r') as hfl:
                for fl in iter(hfl):
                    fl = fl.strip()
                    if not any(map(lambda pat: pat in fl, interested_files)):
                        continue
                    hval, fnm = fl.split()
                    fkey = fnm.split(os.sep)[0] if len(fnm.split(os.sep)) > 1 else os.path.splitext(fnm)[0]
                    if fkey not in mdict:
                        mdict[fkey] = {}
                    mdict[fkey][fnm] = {'{}_val'.format(self.hash_algorithm): hval,
                                        'size_in_bytes': os.path.getsize(os.path.join(self.expand_path(self.stagingpath), fnm))}
            return mdict
        
        def _merge_dicts(mdict, sdict):
            """Merge the 2 given dictioneries, if a key already exists it is
               replaced/udated with new values depending upon data types
            """
            for k, v in sdict.iteritems():
                if k not in mdict:
                    mdict[k] = v
                elif isinstance(v, dict) and isinstance(mdict[k], dict):
                    mdict[k] = _merge_dicts(mdict[k], v)
                elif isinstance(v, list) and isinstance(mdict[k], list):
                    mdict[k] = list(set(mdict[k] + v))
                else:
                    mdict[k] = v
            return mdict
        
        try:
            with open(control_dict.get('status_db_credentials'), 'r') as db_cred_file:
                db_conf = yaml.load(db_cred_file)['statusdb']
        except Exception as e:
            logger.warning("Reading DB config failed due to {}. Meta info will not be saved".format(e))
            return
        # Build meta info dict to put in the database. This method in intended to be
        # called after staging. So if everything goes well there should always be
        # calculated hash files with specific format in the staged directory
        meta_info_dict = {}
        hash_files = glob.glob(os.path.join(self.expand_path(self.stagingpath), "*.{}".format(self.hash_algorithm)))
        for hash_file in hash_files:
            meta_info_dict = _merge_dicts(meta_info_dict, _parse_hash_file(hash_file, control_dict.get('files_interested', ['.fastq', '.bam'])))
        # Now fetch the document from database for the project
        try:
            duser = db_conf.get("username")
            dpwrd = db_conf.get("password")
            dport = db_conf.get("port")
            durl = db_conf.get("url")
            durl_string = "http://{}:{}@{}:{}".format(duser, dpwrd, durl, dport)
            display_url_string = "http://{}:{}@{}:{}".format(duser, "*********", durl, dport)
            couch_connection = couchdb.Server(url=durl_string)
            if not couch_connection:
                logger.warning("Connection failed for url {}, will skip savinf meta info to database".format(display_url_string))
                return
            proj_db = couch_connection['projects']
            proj_doc = [proj_db.get(k.id) for k in proj_db.view("project/project_name", reduce=False) if k.key == self.projectname][0]
            old_meta_info = proj_doc.get("delivered_files", {})
            proj_doc["delivered_files"] = _merge_dicts(old_meta_info, meta_info_dict)
            proj_db.save(proj_doc)
        except Exception as e:
            logger.warning("Attempt to put meta info in database have failed due to {}".format(e))
            return
        else:
            logger.info("Successfully updated meta info")


class ProjectDeliverer(Deliverer):
    def __init__(self, projectid=None, sampleid=None, **kwargs):
        super(ProjectDeliverer, self).__init__(
            projectid,
            sampleid,
            **kwargs)

    def all_samples_delivered(
            self,
            sampleentries=None):
        """ Checks the delivery status of all project samples

            :params sampleentries: a list of sample entry dicts to use instead
                of fetching from database
            :returns: True if all samples in this project has been successfully
                delivered, False otherwise
        """
        sampleentries = sampleentries or db.project_sample_entries(db.dbcon(), self.projectid).get('samples', [])
        return all([self.get_delivery_status(sentry) == 'DELIVERED' for sentry in sampleentries if self.get_sample_status(sentry) != "ABORTED" ])

    def create_report(self):
        """ Create a final aggregate report via a system call """
        logprefix = os.path.abspath(
            self.expand_path(os.path.join(self.logpath, self.projectid)))
        try:
            if not create_folder(os.path.dirname(logprefix)):
                logprefix = None
        except AttributeError:
            logprefix = None
        with chdir(self.expand_path(self.reportpath)):
            cl = self.report_aggregate.split(' ')
            call_external_command(
                cl,
                with_log_files=(logprefix is not None),
                prefix="{}_aggregate".format(logprefix))

    def copy_report(self):
        """ Copies the aggregate report and version reports files to a specified outbox directory.
            :returns: list of the paths to the files it has successfully copied (i.e. the targets)
        """

        def find_from_files_to_deliver(pattern):
            """ Searches the nested list of `files_to_deliver` for files matching the provided pattern
                :param pattern: the regex pattern to search for
                :returns: single matching file
                :raises: AssertionError if there is not strictly one match for the pattern
            """

            matches = []

            for file_list in self.files_to_deliver:
                for f in file_list:
                    # Check that type is string, since list might also contain
                    # objects
                    if type(f) is str and re.match(pattern, f):
                        matches.append(f)

            if not matches or len(matches) != 1:
                raise AssertionError("Found none of multiple matches for pattern: {}".format(pattern))
            else:
                return matches[0]

        def create_target_path(target_file_name):
            reports_outbox = self.config["reports_outbox"]
            return self.expand_path(os.path.join(reports_outbox, os.path.basename(target_file_name)))

        files_copied = []
        try:
            # Find and copy aggregate report file
            aggregate_report_src = self.expand_path(find_from_files_to_deliver(r".*_aggregate_report.csv$"))
            aggregate_report_target = create_target_path(aggregate_report_src)
            shutil.copyfile(aggregate_report_src, aggregate_report_target)
            files_copied.append(aggregate_report_target)

            # Find and copy versions report file
            version_report_file_src = self.expand_path(find_from_files_to_deliver(r".*/version_report.txt"))
            version_report_file_target = create_target_path("{}_version_report.txt".format(self.projectid))
            shutil.copyfile(version_report_file_src, version_report_file_target)
            files_copied.append(version_report_file_target)

        except AssertionError as e:
            logger.warning("Had trouble parsing reports from `files_to_deliver` in config.")
            logger.warning(e.message)
        except KeyError as e:
            logger.warning("Could not find specified value in config: {}."
                           "Will not be able to copy the report.".format(e.message))

        return files_copied

    def db_entry(self):
        """ Fetch a database entry representing the instance's project
            :returns: a json-formatted database entry
            :raises taca_ngi_pipeline.utils.database.DatabaseError:
                if an error occurred when communicating with the database
        """
        return db.project_entry(db.dbcon(), self.projectid)

    def deliver_project(self):
        """ Deliver all samples in a project to the destination specified by 
            deliverypath
            
            :returns: True if all samples were delivered successfully, False if
                any sample was not properly delivered or ready to be delivered
        """
        try:
            if not self.stage_only:
                logger.info("Delivering {} to {}".format(
                    str(self), self.expand_path(self.deliverypath)))
            else:
                logger.info("Staging {}".format(str(self)))
        
            if self.get_delivery_status() == 'DELIVERED' \
                    and not self.force:
                logger.info("{} has already been delivered".format(str(self)))
                return True
            # right now, don't catch any errors since we're assuming any thrown 
            # errors needs to be handled by manual intervention
            status = True
            for sampleid in [sentry['sampleid'] for sentry in db.project_sample_entries(
                    db.dbcon(), self.projectid).get('samples', [])]:
                sample_deliver = SampleDeliverer(self.projectid, sampleid)
                sample_deliver.save_meta_info = False
                st = sample_deliver.deliver_sample()
                status = (status and st)
            # Try to deliver any miscellaneous files for the project (like reports, analysis)
            self.deliver_misc_data()
            # Try aggregate and save meta info in database
            self.aggregate_meta_info()
            # query the database whether all samples in the project have been sucessfully delivered
            if self.all_samples_delivered():
                # this is the only delivery status we want to set on the project level, in order to avoid concurrently
                # running deliveries messing with each other's status updates
                # create the final aggregate report
                try:
                    if self.report_aggregate:
                        logger.info("creating final aggregate report")
                        self.create_report()
                except AttributeError as e:
                    pass
                except Exception as e:
                    logger.warning(
                        "failed to create final aggregate report for {}, "\
                        "reason: {}".format(self, e))
                    raise e

                try:
                    if self.copy_reports_to_reports_outbox:
                        logger.info("copying reports to report outbox")
                        self.copy_report()
                except Exception as e:
                    logger.warning("failed to copy report to report outbox, with reason: {}".format(e.message))
                updated_status = "DELIVERED"
                if self.stage_only:
                    updated_status = "STAGED"
                self.update_delivery_status(status=updated_status)
                self.acknowledge_delivery()

            return status
        except (db.DatabaseError, DelivererInterruptedError, Exception):
            raise

    def update_delivery_status(self, status="DELIVERED"):
        """ Update the delivery_status field in the database to the supplied 
            status for the project specified by this instance
            :returns: the result from the underlying api call
            :raises taca_ngi_pipeline.utils.database.DatabaseError:
                if an error occurred when communicating with the database
        """
        return db.update_project(db.dbcon(), self.projectid, delivery_status=status)
    
    def deliver_misc_data(self):
        """ Deliver any miscellaneous files like reports, analysis results for the project
            This is intended only for project delivery, to avoid redundant delivery.
            Any error during staging here should not fail the delivery, just log warning.
            But error during copying of staged folder should be raised
        """
        misc_files_to_deliver = getattr(self, 'misc_files_to_deliver', [])
        if len(misc_files_to_deliver) > 0:
            misc_gathered_files = fs.gather_files([map(self.expand_path, file_pattern) for file_pattern in misc_files_to_deliver],
                                                       no_checksum=self.no_checksum,
                                                       hash_algorithm=self.hash_algorithm)
            misc_digestpath = os.path.join(self.expand_path(self.stagingpath), "miscellaneous.{}".format(self.hash_algorithm))
            misc_filelistpath = os.path.join(self.expand_path(self.stagingpath), "miscellaneous.lst")
            # Try to stage the miscellaneous files
            try:
                with open(misc_digestpath, 'w') as mdh, open(misc_filelistpath, 'w') as mfh:
                    sagent = transfer.SymlinkAgent(None, None, relative=True)
                    for src, dst, digest in misc_gathered_files:
                        sagent.src_path = src
                        sagent.dest_path = dst
                        try:
                            sagent.transfer()
                        except (transfer.TransferError, transfer.SymlinkError) as e:
                            logger.warning("Failed to stage miscellaneous file '{}' when "
                                           "delivering {} - reason: {}".format(src, str(self), e))

                        fpath = os.path.relpath(dst, self.expand_path(self.stagingpath))
                        mfh.write("{}\n".format(fpath))
                        if digest is not None:
                            mdh.write("{}  {}\n".format(digest, fpath))
                    # finally, include the digestfile in the list of files to deliver
                    mfh.write("{}\n".format(os.path.basename(misc_digestpath)))
            except (IOError, fs.FileNotFoundException, fs.PatternNotMatchedException) as e:
                logger.warning("Failed to stage delivery - reason: {}".format(e))
            # Try to deliver staged files if not only stage option given
            if not self.stage_only:
                misc_delivered_digest = os.path.join(self.deliverypath, "miscellaneous.{}".format(self.hash_algorithm))
                ragent = transfer.RsyncAgent(
                            self.expand_path(self.stagingpath),
                            dest_path=self.expand_path(self.deliverypath),
                            digestfile=misc_delivered_digest,
                            remote_host=getattr(self, 'remote_host', None),
                            remote_user=getattr(self, 'remote_user', None),
                            log=logger,
                            opts={
                                '--files-from': [misc_filelistpath],
                                '--copy-links': None,
                                '--recursive': None,
                                '--perms': None,
                                '--chmod': 'ug+rwX,o-rwx',
                                '--verbose': None,
                                '--exclude': ["*rsync.out", "*rsync.err"]
                            })
                try:
                    ragent.transfer(transfer_log=self.transfer_log())
                except transfer.TransferError as e:
                    raise DelivererRsyncError(e)


class SampleDeliverer(Deliverer):
    """
        A class for handling sample deliveries
    """

    def __init__(self, projectid=None, sampleid=None, **kwargs):
        super(SampleDeliverer, self).__init__(
            projectid,
            sampleid,
            **kwargs)

    def create_report(self):
        """ Create a sample report and an aggregate report via a system call """
        logprefix = os.path.abspath(
            self.expand_path(os.path.join(self.logpath, "{}-{}".format(
                self.projectid, self.sampleid))))
        try:
            if not create_folder(os.path.dirname(logprefix)):
                logprefix = None
        except AttributeError:
            logprefix = None
        with chdir(self.expand_path(self.reportpath)):
            # create the ign_sample_report for this sample
            cl = self.report_sample.split(' ')
            cl.extend(["--samples",self.sampleid])
            call_external_command(
                cl,
                with_log_files=(logprefix is not None),
                prefix="{}_sample".format(logprefix))
            # estimate the delivery date for this sample to 0.5 days ahead
            cl = self.report_aggregate.split(' ')
            cl.extend([
                "--samples_extra",
                json.dumps({
                    self.sampleid: {
                        "delivered": "{}(expected)".format(
                            _timestamp(days=0.5))}})
            ])
            call_external_command(
                cl,
                with_log_files=(logprefix is not None),
                prefix="{}_aggregate".format(logprefix))

    def db_entry(self):
        """ Fetch a database entry representing the instance's project and sample
            :returns: a json-formatted database entry
            :raises taca_ngi_pipeline.utils.database.DatabaseError:
                if an error occurred when communicating with the database
        """
        return db.sample_entry(db.dbcon(), self.projectid, self.sampleid)

    def deliver_sample(self, sampleentry=None):
        """ Deliver a sample to the destination specified by the config.
            Will check if the sample has already been delivered and should not
            be delivered again or if the sample is not yet ready to be delivered.

            :params sampleentry: a database sample entry to use for delivery,
                be very careful with caching the database entries though since
                concurrent processes can update the database at any time
            :returns: True if sample was successfully delivered or was previously
                delivered, False if sample was not yet ready to be delivered
            :raises taca_ngi_pipeline.utils.database.DatabaseError: if an entry corresponding to this
                sample could not be found in the database
            :raises DelivererReplaceError: if a previous delivery of this sample
                has taken place but should be replaced
            :raises DelivererError: if the delivery failed
        """
        # propagate raised errors upwards, they should trigger notification to operator
        try:
            if not self.stage_only:
                logger.info("Delivering {} to {}".format(
                    str(self), self.expand_path(self.deliverypath)))
            else:
                logger.info("Staging {}".format(str(self)))
            try:
                if self.get_analysis_status(sampleentry) != 'ANALYZED':
                    if not self.force and not self.ignore_analysis_status:
                        logger.info("{} has not finished analysis and will not be delivered".format(str(self)))
                        return False
                if self.get_delivery_status(sampleentry) == 'DELIVERED' \
                        and not self.force:
                    logger.info("{} has already been delivered. Sample will not be delivered again this time.".format(str(self)))
                    return True
                if self.get_delivery_status(sampleentry) == 'IN_PROGRESS' \
                        and not self.force:
                    logger.info("delivery of {} is already in progress".format(
                        str(self)))
                    return False
                if self.get_sample_status(sampleentry) == 'ABORTED':
                    logger.info("{} has been marked as ABORTED and will not be delivered".format(str(self)))
                    #set it to delivered as ABORTED samples should not fail the status of a project
                    if  self.get_delivery_status(sampleentry):
                        #if status is set, then overwrite it to NOT_DELIVERED
                        self.update_delivery_status(status="NOT_DELIVERED")
                    #otherwhise leave it empty. Return True as an aborted sample should not fail a delivery
                    return True
                if self.get_sample_status(sampleentry) == 'FRESH' \
                        and not self.force:
                    logger.info("{} is marked as FRESH (new unporcessed data is available)and will not be delivered".format(str(self)))
                    return False
                if self.get_delivery_status(sampleentry) == 'FAILED':
                    logger.info("retrying delivery of previously failed sample {}".format(str(self)))
            except db.DatabaseError as e:
                logger.error(
                    "error '{}' occurred during delivery of {}".format(
                        str(e), str(self)))
                raise
            # set the delivery status to in_progress which will also mean that any concurrent deliveries
            # will leave this sample alone
            self.update_delivery_status(status="IN_PROGRESS")
            # an error with the reports should not abort the delivery, so handle
            try:
                if self.report_sample and self.report_aggregate:
                    logger.info("creating sample reports")
                    self.create_report()
            except AttributeError:
                pass
            except Exception as e:
                logger.warning(
                    "failed to create reports for {}, reason: {}".format(
                        self, e))
            # stage the delivery
            if not self.stage_delivery():
                raise DelivererError("sample was not properly staged")
            logger.info("{} successfully staged".format(str(self)))
            if not self.stage_only:
                # perform the delivery
                if not self.do_delivery():
                    raise DelivererError("sample was not properly delivered")
                logger.info("{} successfully delivered".format(str(self)))
                # set the delivery status in database
                self.update_delivery_status()
                # write a delivery acknowledgement to disk
                self.acknowledge_delivery()
            else:
                self.update_delivery_status(status="STAGED")
            self.aggregate_meta_info()
            return True
        except DelivererInterruptedError:
            self.update_delivery_status(status="NOT_DELIVERED")
            raise
        except Exception:
            self.update_delivery_status(status="FAILED")
            raise

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
            remote_host=getattr(self, 'remote_host', None),
            remote_user=getattr(self, 'remote_user', None),
            log=logger,
            opts={
                '--files-from': [self.staging_filelist()],
                '--copy-links': None,
                '--recursive': None,
                '--perms': None,
                '--chmod': 'ug+rwX,o-rwx',
                '--verbose': None,
                '--exclude': ["*rsync.out", "*rsync.err"]
            })
        create_folder(os.path.dirname(self.transfer_log()))
        try:
            return agent.transfer(transfer_log=self.transfer_log())
        except transfer.TransferError as e:
            raise DelivererRsyncError(e)

    def update_delivery_status(self, status="DELIVERED"):
        """ Update the delivery_status field in the database to the supplied 
            status for the project and sample specified by this instance
            :returns: the result from the underlying api call
            :raises taca_ngi_pipeline.utils.database.DatabaseError:
                if an error occurred when communicating with the database
        """
        return db.update_sample(db.dbcon(), self.projectid, self.sampleid, delivery_status=status)






