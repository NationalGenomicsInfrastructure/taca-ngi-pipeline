"""
    Module for controlling deliveries of samples and projects to DDS
"""
import requests
import os
import logging
import json
import subprocess
import sys
import re

from ngi_pipeline.database.classes import CharonSession
from taca.utils.filesystem import create_folder
from taca.utils.config import CONFIG
from taca.utils.statusdb import StatusdbSession, ProjectSummaryConnection

from .deliver import ProjectDeliverer, SampleDeliverer, DelivererInterruptedError
from ..utils.database import DatabaseError

logger = logging.getLogger(__name__)


def proceed_or_not(question):
    yes = set(['yes', 'y', 'ye'])
    no = set(['no', 'n'])
    sys.stdout.write("{}".format(question))
    while True:
        choice = input().lower()
        if choice in yes:
            return True
        elif choice in no:
            return False
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' ")


class DDSProjectDeliverer(ProjectDeliverer):
    """ This object takes care of delivering project samples with DDS.
    """
    def __init__(self, projectid=None, sampleid=None, 
                 pi_email=None, sensitive=True,
                 add_user=None, fcid=None, do_release=False, 
                 project_title=None, project_description=None,
                 ignore_orderportal_members=False, **kwargs):
        super(DDSProjectDeliverer, self).__init__(
            projectid,
            sampleid,
            **kwargs
        )
        self.config_statusdb = CONFIG.get('statusdb', None)
        if self.config_statusdb is None and not do_release:
            raise AttributeError("statusdb configuration is needed when delivering to DDS (url, username, password, port")
        self.orderportal = CONFIG.get('order_portal', None)
        if self.orderportal is None and not do_release:
            raise AttributeError("Order portal configuration is needed when delivering to DDS")
        if self.orderportal:
            self._set_pi_email(pi_email)
            self._set_other_member_details(add_user, CONFIG.get('add_project_owner', False), ignore_orderportal_members)
            self._set_project_details(project_title, project_description)
        self.sensitive = sensitive
        self.fcid = fcid
        
    def get_delivery_status(self, dbentry=None):
        """ Returns the delivery status for this project. If a dbentry
        dict is supplied, it will be used instead of fetching from database

        :params dbentry: a database entry to use instead of
        fetching from db
        :returns: the delivery status of this project as a string
        """
        dbentry = dbentry or self.db_entry()
        if dbentry.get('delivery_token'):
            if dbentry.get('delivery_token') not in ['NO-TOKEN', 'not_under_delivery'] :
                return 'IN_PROGRESS'  # At least some samples are under delivery
        if dbentry.get('delivery_status'):
            if dbentry.get('delivery_status') == 'DELIVERED':
                return 'DELIVERED'  # The project has been marked as delivered
        if dbentry.get('delivery_projects'):
            return 'PARTIAL'  # The project underwent a delivery, but not for all the samples
        return 'NOT_DELIVERED'  # The project is not delivered

    def release_DDS_delivery_project(self, dds_project, dds_deadline=45): 
        """ Update charon when data upload is finished and release DDS project to user.
        For this to work on runfolder deliveries, update the delivery status in Charon maually.
        """
        charon_status = self.get_delivery_status()
        # Skip if status is not in progress
        if charon_status != 'IN_PROGRESS':
            logger.info("Project {} has no delivery token. Project is not being delivered at the moment".format(self.projectid))
            return
        question = "About to release project {} in DDS delivery project {} to user. Continue? ".format(self.projectid, dds_project)
        if proceed_or_not(question):
            logger.info("Releasing DDS project {} to user".format(dds_project))  
        else:
            logger.error("{} delivery has been aborted.".format(str(self)))
            return
        
        delivery_status = 'IN_PROGRESS'
        try:
            cmd = ['dds', '--no-prompt', 'project', 'status', 'release', 
                   '--project', dds_project,
                   '--deadline', dds_deadline]
            process_handle = subprocess.run(cmd)
            process_handle.check_returncode()
            logger.info("Project {} succefully delivered. Delivery project is {}.".format(self.projectid, dds_project))
            delivery_status = 'DELIVERED'
        except subprocess.CalledProcessError as e:
            logger.exception('Could not release project {}, an error occurred'.format(self.projectid))
            delivery_status = 'FAILED'
            raise e
        if delivery_status == 'DELIVERED' or delivery_status == 'FAILED':
            # Fetch all samples that were under delivery and update their status in charon
            in_progress_samples = self.get_samples_from_charon(delivery_status="IN_PROGRESS")
            for sample_id in in_progress_samples:
                try:
                    sample_deliverer = DDSSampleDeliverer(self.projectid, sample_id)
                    sample_deliverer.update_delivery_status(status=delivery_status)
                except Exception as e:  #TODO: catch specific errors instead
                    logger.exception('Sample {}: Problems in setting sample status on charon.'.format(sample_id))
            # Reset delivery in charon
            self.delete_delivery_token_in_charon()
            # If all samples in charon are DELIVERED or ABORTED, then the whole project is DELIVERED
            all_samples_delivered = True
            for sample_id in self.get_samples_from_charon(delivery_status=None):
                try:
                    sample_deliverer = DDSSampleDeliverer(self.projectid, sample_id)
                    if sample_deliverer.get_sample_status() == 'ABORTED':
                        continue
                    if sample_deliverer.get_delivery_status() != 'DELIVERED':
                        all_samples_delivered = False
                except Exception as e:  #TODO: catch specific errors instead
                    logger.exception('Sample {}: Problems in setting sample status on charon.'.format(sample_id))
            if all_samples_delivered:
                self.update_delivery_status(status=delivery_status)

    def deliver_project(self):
        """ Deliver all samples in a project with DDS
        
        :returns: True if all samples were delivered successfully, False if
        any sample was not properly delivered or ready to be delivered
        """
        soft_stagepath = self.expand_path(self.stagingpath)

        if self.get_delivery_status() == 'DELIVERED' and not self.force:
            logger.info("{} has already been delivered. This project will not "
                        "be delivered again this time.".format(str(self)))
            return True
        
        elif self.get_delivery_status() == 'IN_PROGRESS':
            logger.error("Project {} is already under delivery. "
                         "Multiple deliveries are not allowed".format(self.projectid))
            raise DelivererInterruptedError("Project already under delivery")
        
        elif self.get_delivery_status() == 'PARTIAL':
            logger.warning("{} has already been partially delivered. "
                           "Please confirm you want to proceed.".format(str(self)))
            if proceed_or_not("Do you want to proceed (yes/no): "):
                logger.info("{} has already been partially delivered. "
                            "User confirmed to proceed.".format(str(self)))
            else:
                logger.error("{} has already been partially delivered. "
                             "User decided to not proceed.".format(str(self)))
                return False
        
        # Check if the sensitive flag has been set in the correct way
        question = ("This project has been marked as SENSITIVE "
        "(option --sensitive). Do you want to proceed with delivery? ")
        if not self.sensitive:
            question = ("This project has been marked as NON-SENSITIVE "
            "(option --no-sensitive). Do you want to proceed with delivery? ")
        if proceed_or_not(question):
            logger.info("Delivering {} with DDS. "
                        "Project marked as SENSITIVE={}".format(str(self), self.sensitive))
        else:
            logger.error("{} delivery has been aborted. "
                         "Sensitive level was WRONG.".format(str(self)))
            return False
        
        # Now start with the real work
        status = True

        # Connect to charon, return list of sample objects that have been staged
        try:
            samples_to_deliver = self.get_samples_from_charon(delivery_status="STAGED")
        except Exception as e:  #TODO: catch specific errors instead
            logger.exception("Cannot get samples from Charon.")
            raise e
        if len(samples_to_deliver) == 0:
            logger.warning('No staged samples found in Charon')
            raise AssertionError('No staged samples found in Charon')  #TODO: catch error raised by get_samples_from_charon instead?

        # Collect other files (not samples) if any
        misc_to_deliver = [itm for itm in os.listdir(soft_stagepath) if os.path.splitext(itm)[0] not in samples_to_deliver]

        question = "\nProject stagepath: {}\nSamples: {}\nMiscellaneous: {}\n\nProceed with delivery ? "
        question = question.format(soft_stagepath, ", ".join(samples_to_deliver), ", ".join(misc_to_deliver))
        if proceed_or_not(question):
            logger.info("Proceeding with delivery of {}".format(str(self)))
        else:
            logger.error("Aborting delivery for {}, remove/add files as required and try again".format(str(self)))
            return False

        # create a delivery project
        dds_name_of_delivery = ''
        dds_name_of_delivery = self._create_delivery_project()
        logger.info("Delivery project for project {} has been created. Delivery ID is {}".format(self.projectid, dds_name_of_delivery))
            
        # Update delivery status in Charon
        samples_in_progress = []
        for sample_id in samples_to_deliver:
            try:
                sample_deliverer = DDSSampleDeliverer(self.projectid, sample_id)
                sample_deliverer.update_sample_status()
            except Exception as e:
                logger.exception('Sample status for {} has not been updated in Charon.'.format(sample_id))
                raise e
            else:
                samples_in_progress.append(sample_id)
        if len(samples_to_deliver) != len(samples_in_progress):
            # Something unexpected happend, terminate
            logger.warning('Not all the samples have been updated in Charon. Terminating')
            raise AssertionError('len(samples_to_deliver) != len(samples_in_progress): '
                                 '{} != {}'.format(len(samples_to_deliver),
                                                   len(samples_in_progress)))

        delivery_status = self.upload_data(dds_name_of_delivery)  # Status is "uploaded" if successful
        # Update project and samples fields in charon
        if delivery_status:
            self.save_delivery_token_in_charon(delivery_status)
            # Save all delivery projects in charon
            self.add_dds_name_delivery_in_charon(dds_name_of_delivery)
            self.add_dds_name_delivery_in_statusdb(dds_name_of_delivery)
            logger.info("Delivery status for project {}, "
                        "delivery project {} is {}".format(self.projectid,
                                                            dds_name_of_delivery,
                                                            delivery_status))
            for sample_id in samples_to_deliver:
                try:
                    sample_deliverer = DDSSampleDeliverer(self.projectid, sample_id)
                    sample_deliverer.save_delivery_token_in_charon(delivery_status)
                    sample_deliverer.add_dds_name_delivery_in_charon(dds_name_of_delivery)
                except Exception as e:
                    logger.exception('Failed in saving sample information for sample {}'.format(sample_id))
        else:
            logger.error('Something went wrong when uploading data to {} '
                         'for project {}.'.format(dds_name_of_delivery, self.projectid))
            status = False

        return status

    def deliver_run_folder(self):
        """ Symlink run folder to stage path, create DDS delivery project and upload data.
        """
        # Stage the data
        dst = self.expand_path(self.stagingpath)
        path_to_data = self.expand_path(self.datapath)
        runfolder_archive = os.path.join(path_to_data, self.fcid + ".tar.gz")
        runfolder_md5file = runfolder_archive + ".md5"
        
        question = "This project has been marked as SENSITIVE (option --sensitive). Do you want to proceed with delivery? "
        if not self.sensitive:
            question = "This project has been marked as NON-SENSITIVE (option --no-sensitive). Do you want to proceed with delivery? "
        if proceed_or_not(question):
            logger.info("Delivering {} with DDS. Project marked as SENSITIVE={}".format(str(self), self.sensitive))
        else:
            logger.error("{} delivery has been aborted. Sensitive level was WRONG.".format(str(self)))
            return False

        status = True

        create_folder(dst)
        try:
            os.symlink(runfolder_archive, dst)
            os.symlink(runfolder_md5file, dst)
            logger.info("Symlinking files {} and {} to {}".format(runfolder_archive, runfolder_md5file, dst))
        except IOError as e:
            logger.error("Unable to symlink files to {}. Please check that the files "
                         "exist and that the filenames match the flowcell ID.".format(dst))

        delivery_id = ''
        delivery_id = self._create_delivery_project()
        logger.info("Delivery project for project {} has been created. "
                    "Delivery ID is {}".format(self.projectid, delivery_id))

        # Upload with DDS
        dds_delivery_status = self.upload_data(delivery_id)

        if dds_delivery_status:
            logger.info("DDS upload for project {} to "
                        "delivery project {} was sucessful".format(self.projectid,
                                                                   delivery_id))
        else:
            logger.error('Something when wrong when uploading {} '
                         'to DDS project {}'.format(self.projectid, delivery_id))
            status = False
        return status


    def save_delivery_token_in_charon(self, delivery_token):
        """Updates delivery_token in Charon at project level
        """
        charon_session = CharonSession()
        charon_session.project_update(self.projectid, delivery_token=delivery_token)

    def delete_delivery_token_in_charon(self):
        """Removes delivery_token from Charon upon successful delivery
        """
        charon_session = CharonSession()
        charon_session.project_update(self.projectid, delivery_token='NO-TOKEN')

    def add_dds_name_delivery_in_charon(self, name_of_delivery):
        """Updates delivery_projects in Charon at project level
        """
        charon_session = CharonSession()
        try:
            #fetch the project
            project_charon = charon_session.project_get(self.projectid)
            delivery_projects = project_charon['delivery_projects']
            if name_of_delivery not in delivery_projects:
                delivery_projects.append(name_of_delivery)
                charon_session.project_update(self.projectid, delivery_projects=delivery_projects)
                logger.info('Charon delivery_projects for project {} '
                            'updated with value {}'.format(self.projectid, name_of_delivery))
            else:
                logger.warn('Charon delivery_projects for project {} not updated '
                            'with value {} because the value was already present'.format(self.projectid, name_of_delivery))
        except Exception as e:
            logger.exception('Failed to update delivery_projects in charon while '
                         'delivering {}.'.format(self.projectid))

    def add_dds_name_delivery_in_statusdb(self, name_of_delivery):
        """Updates delivery_projects in StatusDB at project level
        """
        save_meta_info = getattr(self, 'save_meta_info', False)
        if not save_meta_info:
            return
        status_db = ProjectSummaryConnection(self.config_statusdb)
        project_page = status_db.get_entry(self.projectid, use_id_view=True)
        delivery_projects = []
        if 'delivery_projects' in project_page:
            delivery_projects = project_page['delivery_projects']

        delivery_projects.append(name_of_delivery)

        project_page['delivery_projects'] = delivery_projects
        try:
            status_db.save_db_doc(project_page)
            logger.info('Delivery_projects for project {} updated with value {} in statusdb'.format(self.projectid, name_of_delivery))
        except Exception as e:
            logger.exception('Failed to update delivery_projects in statusdb while delivering {}.'.format(self.projectid))

    def upload_data(self, name_of_delivery):
        """Upload staged sample data with DDS
        """
        stage_dir = self.expand_path(self.stagingpath)
        log_dir = os.path.join(os.path.dirname(CONFIG.get('log').get('file')), 'DDS_logs') 
        project_log_dir = os.path.join(log_dir, self.projectid)
        cmd = ['dds', '--no-prompt', 'data', 'put', 
               '--mount-dir', project_log_dir,
               '--project', name_of_delivery, 
               '--source', stage_dir]
        try:
            process_handle = subprocess.run(cmd, capture_output=True)
            process_handle.check_returncode()  # Possibly check .returncode manually instead
        except subprocess.CalledProcessError as e:
            logger.exception('DDS upload failed while uploading {} to {}'.format(stage_dir, name_of_delivery))
            raise e
        if "Upload completed!" in process_handle.stdout.decode("utf-8"):
            delivery_status = "uploaded"
        else:
            delivery_status = None
        return delivery_status

    def get_samples_from_charon(self, delivery_status='STAGED'):
        """Takes as input a delivery status and return all samples with that delivery status
        """
        charon_session = CharonSession()
        result = charon_session.project_get_samples(self.projectid)
        samples = result.get('samples')
        if samples is None:
            raise AssertionError('CharonSession returned no results for project {}'.format(self.projectid))
        samples_of_interest = []
        for sample in samples:
            sample_id = sample.get('sampleid')
            charon_delivery_status = sample.get('delivery_status')
            if charon_delivery_status == delivery_status or delivery_status is None:
                samples_of_interest.append(sample_id)
        return samples_of_interest

    def _create_delivery_project(self):
        """Create a DDS delivery project and return the ID
        """
        create_project_cmd = ['dds', '--no-prompt', 'project', 'create',
                              '--title', self.project_title,
                              '--description', self.project_desc,
                              '--principal-investigator', self.pi_email,
                              '--owner', self.pi_email]
        if self.other_member_details:
            for member in self.other_member_details:
                create_project_cmd.append('--researcher')
                create_project_cmd.append(member)
        if not self.sensitive:
            create_project_cmd.append('--non-sensitive')
        dds_project_id = ''
        try:
            process_handle = subprocess.run(create_project_cmd, capture_output=True)
            process_handle.check_returncode()  # Possibly check .returncode manually instead
        except subprocess.CalledProcessError as e:  #FIXME
            logger.exception("An error occurred while setting up the DDS delivery project.")
            raise e
        output = process_handle.stdout.decode("utf-8")
        project_pattern = re.compile('ngis\d{5}')
        found_project = re.search(project_pattern, output)
        if found_project:
            dds_project_id = found_project.group()
            logger.info("DDS project successfully set up for {}. Info:\n".format(self.projectid, output))
            return dds_project_id
        else:
            logger.warn("DDS project NOT set up for {}. Info:\n".format(self.projectid, process_handle.stderr.decode("utf-8")))  #TODO: maybe nothing in stderr?
            return

    def _set_pi_email(self, given_pi_email=None):
        """Set PI email address
        """
        self.pi_email = None
        if given_pi_email:
            logger.warning("PI email for project {} specified by user: {}".format(self.projectid, given_pi_email))
            self.pi_email = given_pi_email
        if not self.pi_email:
            try:
                prj_order = self._get_order_detail()
                self.pi_email = prj_order['fields']['project_pi_email']
                logger.info("PI email for project {} found: {}".format(self.projectid, self.pi_email))
            except Exception as e:
                logger.exception("Cannot fetch pi_email from StatusDB.")
                raise e

    def _set_other_member_details(self, other_member_emails=[], include_owner=False, ignore_orderportal_members=False):
        """Set other contact details if available, this is not mandatory so
        the method will not raise error if it could not find any contact
        """
        self.other_member_details = []
        # try getting appropriate contact emails
        if not ignore_orderportal_members:
            logger.info("Fetching additional members from order portal.")
            try:
                prj_order = self._get_order_detail()
                if include_owner:
                    owner_email = prj_order.get('owner', {}).get('email')
                    if owner_email and owner_email != self.pi_email and owner_email not in other_member_emails:
                        other_member_emails.append(owner_email)
                binfo_email = prj_order.get('fields', {}).get('project_bx_email')
                if binfo_email and binfo_email != self.pi_email and binfo_email not in other_member_emails:
                    other_member_emails.append(binfo_email)
            except (AssertionError, ValueError) as e:
                pass # nothing to worry, just move on
        if other_member_emails:
            logger.info("Other appropriate contacts were found, they will be added to DDS delivery project: {}".format(", ".join(other_member_emails)))
            self.other_member_details = other_member_emails

    def _set_project_details(self, given_title=None, given_desc=None):
        """Set project details, either given or from order portal"""
        self.project_title, self.project_desc = (None, None)
        if given_title:
            logger.warning("Project title for project {} specified by user: {}".format(self.projectid, given_title))
            self.project_title = given_title
        if given_desc:
            logger.warning("Project description for project {} specified by user: {}".format(self.projectid, given_desc))
            self.project_desc = given_desc
        if not self.project_desc or not self.project_title:
            try:
                prj_order = self._get_order_detail()
                self.project_title = prj_order['title']
                self.project_desc = prj_order['fields']['project_desc'].replace('\n', ' ')
                logger.info("Project title for project {} found: {}".format(self.projectid, self.project_title))
                if len(self.project_desc) > 24:
                    short_desc = self.project_desc[:25] + '...'
                else:
                    short_desc = self.project_desc
                logger.info("Project description for project {} found: {}".format(self.projectid, short_desc))
            except Exception as e:  #TODO: catch specific exceptions instead
                    logger.exception("Cannot fetch project title and/or description from StatusDB.")
                    raise e

    def _get_order_detail(self):
        """Fetch order details from order portal"""
        status_db = StatusdbSession(self.config_statusdb)
        projects_db = status_db.connection['projects']
        view = projects_db.view('order_portal/ProjectID_to_PortalID')
        rows = view[self.projectid].rows
        if len(rows) < 1:
            raise AssertionError("Project {} not found in StatusDB".format(self.projectid))
        if len(rows) > 1:
            raise AssertionError('Project {} has more than one entry in orderportal_db'.format(self.projectid))
        portal_id = rows[0].value
        # Get project info from order portal API
        get_project_url = '{}/v1/order/{}'.format(self.orderportal.get('orderportal_api_url'), portal_id)
        headers = {'X-OrderPortal-API-key': self.orderportal.get('orderportal_api_token')}
        response = requests.get(get_project_url, headers=headers)
        if response.status_code != 200:
            raise AssertionError("Status code returned when trying to get "
                                 "project info from the order portal: "
                                 "{} was not 200. Response was: {}".format(portal_id, response.content))
        return json.loads(response.content)


class DDSSampleDeliverer(SampleDeliverer):
    """A class for handling sample deliveries with DDS
    """

    def __init__(self, projectid=None, sampleid=None, **kwargs):
        super(DDSSampleDeliverer, self).__init__(
            projectid,
            sampleid,
            **kwargs)

    def update_sample_status(self, sampleentry=None):
        """ Update delivery status in charon
        """
        try:
            logger.info("Trying to upload {} with DDS".format(str(self)))
            try:
                if self.get_delivery_status(sampleentry) != 'STAGED':
                    logger.info("{} has not been staged and will not be delivered".format(str(self)))
                    return False
            except DatabaseError as e:
                logger.exception("An error occurred during delivery of {}".format(str(self)))
                raise(e)
            self.update_delivery_status(status="IN_PROGRESS")
        except Exception as e:
            self.update_delivery_status(status="STAGED")
            logger.exception(e)
            raise(e)

    def save_delivery_token_in_charon(self, delivery_token):
        """Updates delivery_token in Charon at sample level
        """
        charon_session = CharonSession()
        charon_session.sample_update(self.projectid, self.sampleid, delivery_token=delivery_token)

    def add_dds_name_delivery_in_charon(self, name_of_delivery):
        """Updates delivery_projects in Charon at project level
        """
        charon_session = CharonSession()
        try:
            # Fetch the project
            sample_charon = charon_session.sample_get(self.projectid, self.sampleid)
            delivery_projects = sample_charon['delivery_projects']
            if name_of_delivery not in sample_charon:
                delivery_projects.append(name_of_delivery)
                charon_session.sample_update(self.projectid, self.sampleid, delivery_projects=delivery_projects)
                logger.info('Charon delivery_projects for sample {} updated '
                            'with value {}'.format(self.sampleid, name_of_delivery))
            else:
                logger.warn('Charon delivery_projects for sample {} not updated '
                            'with value {} because the value was already present'.format(self.sampleid, name_of_delivery))
        except Exception as e:
            logger.exception('Failed to update delivery_projects in charon while delivering {}.'.format(self.sampleid))
