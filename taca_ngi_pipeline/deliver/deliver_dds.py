"""
    Module for controlling deliveries os samples and projects to DDS
"""
import time
import requests
import datetime
import os
import logging
import json
import subprocess
import sys
import shutil
import re
from dateutil.relativedelta import relativedelta

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
            sys.stdout.write("Please respond with 'yes' or 'no'")


class DDSProjectDeliverer(ProjectDeliverer):
    """ This object takes care of delivering project samples with DDS.
    """
    def __init__(self, projectid=None, sampleid=None, 
                 pi_email=None, sensitive=True,
                 add_user=None, fcid=None, do_release=False, **kwargs):
        super(DDSProjectDeliverer, self).__init__(
            projectid,
            sampleid,
            **kwargs
        )
        self.config_statusdb = CONFIG.get('statusdb', None)
        if self.config_statusdb is None and not do_release:
            raise AttributeError("statusdb configuration is needed when delivering to DDS (url, username, password, port")
        self.orderportal = CONFIG.get('order_portal', None)
        if self.orderportal:
            self._set_pi_details(pi_email)
            self._set_other_member_details(add_user, CONFIG.get('add_project_owner', False))
            self._set_project_details()
        self.sensitive = sensitive
        self.fcid = fcid
        
    def get_delivery_status(self, dbentry=None):
        """ Returns the delivery status for this project. If a dbentry
        dict is supplied, it will be used instead of fethcing from database

        :params dbentry: a database entry to use instead of
        fetching from db
        :returns: the delivery status of this project as a string
        """
        #TODO: maybe use the dds delivery status here, instead of a token, would that be useful?
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

    def release_DDS_delivery_project(self): 
        """ Update charon when data upload is finished and release DDS project to user.
        """
        #TODO: modify to update charon and release the delivery project to user
        charon_status = self.get_delivery_status()
        # we don't care if delivery is not in progress
        if charon_status != 'IN_PROGRESS':
            logger.info("Project {} has no delivery token. Project is not being delivered at the moment".format(self.projectid))
            return
        dds_delivery_project = self.db_entry().get('delivery_projects')  #TODO: multiple deliveries possible. Add a check for this and an option to specify which delivery to release
        dds_delivery_status = self.db_entry().get('delivery_token')
        logger.info("Project {} (DDS project {}) delivery status is {}.".format(self.projectid, dds_delivery_project, dds_delivery_status))
        #TODO: Add a question to user "Do you want to proceed with the release?" Possibly list samples that have been uploaded and the total nr of samples
        delivery_status = 'IN_PROGRESS'
        try:
            cmd = ['dds', 'project', 'status', 'release', '--project', dds_delivery_project]
            output = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
            logger.info("Project {} succefully delivered. Delivery project is {}.".format(self.projectid, dds_delivery_project))
            delivery_status = 'DELIVERED'
        except Exception as e:
            logger.error('Cannot release project {}, and error occurred: {}'.format(self.projectid, e))
            delivery_status = 'FAILED'
        if delivery_status == 'DELIVERED' or delivery_status == 'FAILED':
            #fetch all samples that were under delivery
            in_progress_samples = self.get_samples_from_charon(delivery_status="IN_PROGRESS")
            # now update them
            for sample_id in in_progress_samples:
                try:
                    sample_deliverer = DDSSampleDeliverer(self.projectid, sample_id)
                    sample_deliverer.update_delivery_status(status=delivery_status)
                except Exception as e:
                    logger.error('Sample {}: Problems in setting sample status on charon. Error: {}'.format(sample_id, e))
                    logger.exception(e)
            # Reset delivery in charon
            self.delete_delivery_token_in_charon()
            # If all samples in charon are DELIVERED or ABORTED, then the whole project is DELIVERED
            all_samples_delivered = True
            for sample_id in self.get_samples_from_charon(delivery_status=None):
                try:
                    sample_deliverer = DDSSampleDeliverer(self.projectid, sample_id)
                    if sample_deliverer.get_sample_status() == 'ABORTED':
                        continue
                    if sample_deliverer.get_delivery_status() != 'DELIVERED':  #TODO: check this logic
                        all_samples_delivered = False
                except Exception as e:
                    logger.error('Sample {}: Problems in setting sample status on charon. Error: {}'.format(sample_id, e))
                    logger.exception(e)
            if all_samples_delivered:
                self.update_delivery_status(status=delivery_status)

    def deliver_project(self):
        """ Deliver all samples in a project with DDS
        
        :returns: True if all samples were delivered successfully, False if
        any sample was not properly delivered or ready to be delivered
        """
        soft_stagepath = self.expand_path(self.stagingpath)

        if self.get_delivery_status() == 'DELIVERED' \
                and not self.force:
            logger.info("{} has already been delivered. This project will not be delivered again this time.".format(str(self)))
            return True
        
        elif self.get_delivery_status() == 'IN_PROGRESS':
            logger.error("Project {} is already under delivery. Multiple deliveries are not allowed".format(
                    self.projectid))
            raise DelivererInterruptedError("Project already under delivery")
        
        elif self.get_delivery_status() == 'PARTIAL':
            logger.warning("{} has already been partially delivered. Please confirm you want to proceed.".format(str(self)))
            if proceed_or_not("Do you want to proceed (yes/no): "):
                logger.info("{} has already been partially delivered. User confirmed to proceed.".format(str(self)))
            else:
                logger.error("{} has already been partially delivered. User decided to not proceed.".format(str(self)))
                return False
        
        # Check if the sensitive flag has been set in the correct way
        question = "This project has been marked as SENSITIVE (option --sensitive). Do you want to proceed with delivery? "
        if not self.sensitive:
            question = "This project has been marked as NON-SENSITIVE (option --no-sensitive). Do you want to proceed with delivery? "
        if proceed_or_not(question):
            logger.info("Delivering {} with DDS. Project marked as SENSITIVE={}".format(str(self), self.sensitive))
        else:
            logger.error("{} delivery has been aborted. Sensitive level was WRONG.".format(str(self)))
            return False
        
        # Now start with the real work
        status = True

        # Connect to charon, return list of sample objects that have been staged
        try:
            samples_to_deliver = self.get_samples_from_charon(delivery_status="STAGED")
        except Exception as e:
            logger.error("Cannot get samples from Charon. Error says: {}".format(str(e)))
            logger.exception(e)
            raise e
        if len(samples_to_deliver) == 0:
            logger.warning('No staged samples found in Charon')
            raise AssertionError('No staged samples found in Charon')

        # Collect other files (not samples) if any
        misc_to_deliver = [itm for itm in os.listdir(soft_stagepath) if os.path.splitext(itm)[0] not in samples_to_deliver]

        question = "\nProject stagepath: {}\nSamples: {}\nMiscellaneous: {}\n\nProceed with delivery ? "
        question = question.format(soft_stagepath, ", ".join(samples_to_deliver), ", ".join(misc_to_deliver))
        if proceed_or_not(question):
            logger.info("Proceeding with delivery of {}".format(str(self)))
        else:
            logger.error("Aborting delivery for {}, remove/add files as required and try again".format(str(self)))
            return False

        # create a delivery project id 
        dds_name_of_delivery = ''
        try:
            dds_name_of_delivery = self._create_delivery_project()
            logger.info("Delivery project for project {} has been created. Delivery ID is {}".format(self.projectid, dds_name_of_delivery))
        except Exception as e: #TODO: where to catch errors?
            logger.error('Cannot create delivery project. Error says: {}'.format(e))
            logger.exception(e)

        # Update delivery status in Charon
        samples_in_progress = []
        for sample_id in samples_to_deliver:
            try:
                sample_deliverer = DDSSampleDeliverer(self.projectid, sample_id)
                sample_deliverer.update_sample_status() #TODO: note -- don't upload per sample, instead deliver whole folder
            except Exception as e:
                logger.error('Sample {} has not been staged. Error says: {}'.format(sample_id, e)) #TODO: make these messages more accurate
                logger.exception(e)
                raise e
            else:
                samples_in_progress.append(sample_id)
        if len(samples_to_deliver) != len(samples_in_progress):
            # Something unexpected happend, terminate
            logger.warning('Not all the samples have been staged. Terminating')
            raise AssertionError('len(samples_to_deliver) != len(samples_in_progress): {} != {}'.format(len(samples_to_deliver),
                                                                                                        len(samples_in_progress)))

        delivery_status = self.do_delivery(dds_name_of_delivery)
        # Update project and samples fields in charon
        if delivery_status:
            self.save_delivery_token_in_charon(delivery_status)
            # Save all delivery projects in charon
            self.add_dds_name_delivery_in_charon(dds_name_of_delivery)
            self.add_dds_name_delivery_in_statusdb(dds_name_of_delivery)
            logger.info("Delivery status for project {}, delivery project {} is {}".format(self.projectid,
                                                                                    dds_name_of_delivery,
                                                                                    delivery_status))
            for sample_id in samples_to_deliver:
                try:
                    sample_deliverer = DDSSampleDeliverer(self.projectid, sample_id)
                    sample_deliverer.save_delivery_token_in_charon(delivery_status)
                    sample_deliverer.add_dds_name_delivery_in_charon(dds_name_of_delivery)
                except Exception as e:
                    logger.error('Failed in saving sample infomration for sample {}. Error says: {}'.format(sample_id, e))
                    logger.exception(e)
        else:
            logger.error('Delivery project for project {} has not been created'.format(self.projectid))
            status = False

        return status

    def deliver_run_folder(self):
        """ Hard stage run folder and initiate delivery.
        """
        # Stage the data
        dst = self.expand_path(self.stagingpath) #TODO: possibly change this in config to avoid conflicts in DELIVERY
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
            shutil.copy(runfolder_archive, dst) #TODO: symlink instead?
            shutil.copy(runfolder_md5file, dst)
            logger.info("Copying files {} and {} to {}".format(runfolder_archive, runfolder_md5file, dst))
        except IOError as e:
            logger.error("Unable to copy files to {}. Please check that the files exist and that the filenames match the flowcell ID.".format(dst))

        delivery_id = ''
        try:
            delivery_id = self._create_delivery_project()
            logger.info("Delivery project for project {} has been created. Delivery ID is {}".format(self.projectid, delivery_id))
        except Exception as e: #TODO: where to catch errors?
            logger.error('Cannot create delivery project. Error says: {}'.format(e))
            logger.exception(e)

        # Upload with DDS
        delivery_token = self.do_delivery(delivery_id) #TODO: DDS token?

        if delivery_token:
            logger.info("Delivery token for project {}, delivery project {} is {}".format(self.projectid,
                                                                                    delivery_id,
                                                                                    delivery_token))
        else:
            logger.error('Delivery project for project {} has not been created'.format(self.projectid))
            status = False
        #TODO: Update charon with status, delivery project and token etc?
        return status


    def save_delivery_token_in_charon(self, delivery_token): #TODO: delivery token in DDS?
        """Updates delivery_token in Charon at project level
        """
        charon_session = CharonSession()
        charon_session.project_update(self.projectid, delivery_token=delivery_token)

    def delete_delivery_token_in_charon(self): #TODO: delivery token in DDS?
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
                logger.info('Charon delivery_projects for project {} updated with value {}'.format(self.projectid, name_of_delivery))
            else:
                logger.warn('Charon delivery_projects for project {} not updated with value {} because the value was already present'.format(self.projectid, name_of_delivery))
        except Exception as e:
            logger.error('Failed to update delivery_projects in charon while delivering {}. Error says: {}'.format(self.projectid, e))
            logger.exception(e)

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
            logger.error('Failed to update delivery_projects in statusdb while delivering {}. Error says: {}'.format(self.projectid, e))
            logger.exception(e)

    def do_delivery(self, name_of_delivery):
        """Upload staged sample data with DDS
        #TODO: decide if we want to upload the whole staging dir at once or provide a list of files
        """
        stage_folder = self.expand_path(self.stagingpath)
        cmd = ['dds', 'data', 'put', 
               '--project', name_of_delivery, 
               '--source', stage_folder]
        try:
            output = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode('utf-8') #TODO: change this to run detached, or look for "Upload completed!"?
        except subprocess.CalledProcessError as e:
            logger.error('DDS upload failed while uploading {} to {}'.format(stage_folder, name_of_delivery))
            logger.exception(e)
        if "Upload completed!" in output:
            delivery_status = "uploaded"  #TODO: possibly get the dds project status instead
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
        """
        dds project create --title "The title of the project" --description "A description of the project" --principal-investigator "The name of the Principal Investigator"
        --researcher    Email of a user to be added to the project as Researcher. Use the option multiple times to specify more than one researcher
        --owner         Email of user to be added to the project as Project Owner. Use the option multiple times to specify more than one project owner
        --is_sensitive  Indicate if the Project includes sensitive data.
        """
        create_project_cmd = ('dds project create'
                              + ' --title ' + self.project_title
                              + ' --description ' + self.project_desc
                              + ' --principal-investigator ' + self.pi_name
                              + ' --owner ' + self.pi_email) #TODO: check that this assumption is correct
        if self.other_member_details:
            other_users = " --researcher ".join(self.other_member_details)
            create_project_cmd += other_users
        if self.sensitive:
            create_project_cmd += ' --is_sensitive '
        dds_project_id = ''
        try:
            output = subprocess.check_output(create_project_cmd, stderr=subprocess.STDOUT).decode("utf-8")
            project_pattern = re.compile('ngis\d{5}')
            dds_project_id = re.search(project_pattern, output).group()
            logger.info("DDS project successfully set up for {}. Info:\n".format(self.projectid, output))
        except Exception as e: #TODO: handle this better
            logger.error("An error occurred while setting up the DDS delivery project: {}".format(e))
        return dds_project_id

    def _set_pi_details(self, given_pi_email=None):
        """
            Set PI email address and PI name using PI email
        """
        self.pi_email, self.pi_name = (None, None)
        # try getting PI email
        if given_pi_email:
            logger.warning("PI email for project {} specified by user: {}".format(self.projectid, given_pi_email))
            self.pi_email = given_pi_email
        else:
            try:
                prj_order = self._get_order_detail()
                self.pi_email = prj_order['fields']['project_pi_email']
                self.pi_name = prj_order['fields']['project_pi_name']
                logger.info("PI email for project {} found: {}".format(self.projectid, self.pi_email))
            except Exception as e:
                logger.error("Cannot fetch pi_email and/or name from StatusDB. Error says: {}".format(str(e)))
                raise e

    def _set_other_member_details(self, other_member_emails=[], include_owner=False):
        """
            Set other contact details if avilable, this is not mandatory so
            the method will not raise error if it could not find any contact
        """
        self.other_member_details = []
        # try getting appropriate contact emails
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
            logger.info("Other appropriate contacts were found, they will be added to GRUS delivery project: {}".format(", ".join(other_member_emails)))
            self.other_member_details = other_member_emails

    def _set_project_details(self):
        try:
            prj_order = self._get_order_detail()
            self.project_title = prj_order['order_details']['title']
            self.project_desc = prj_order['fields']['project_desc'].strip('\n')
            logger.info("Project title for project {} found: {}".format(self.projectid, self.project_title))
            if len(self.project_desc) > 24:
                short_desc = self.project_desc[:25] + '...'
            else:
                short_desc = self.project_desc
            logger.info("Project description for project {} found: {}".format(short_desc))
        except Exception as e:
                logger.error("Cannot fetch project title and/or description from StatusDB. Error says: {}".format(str(e)))
                raise e

    def _get_order_detail(self):
        status_db = StatusdbSession(self.config_statusdb)
        projects_db = status_db.connection['projects']
        view = projects_db.view('order_portal/ProjectID_to_PortalID')
        rows = view[self.projectid].rows
        if len(rows) < 1:
            raise AssertionError("Project {} not found in StatusDB".format(self.projectid))
        if len(rows) > 1:
            raise AssertionError('Project {} has more than one entry in orderportal_db'.format(self.projectid))
        portal_id = rows[0].value
        # Get the PI email from order portal API
        get_project_url = '{}/v1/order/{}'.format(self.orderportal.get('orderportal_api_url'), portal_id)
        headers = {'X-OrderPortal-API-key': self.orderportal.get('orderportal_api_token')}
        response = requests.get(get_project_url, headers=headers)
        if response.status_code != 200:
            raise AssertionError("Status code returned when trying to get "
                                 "PI email from project in order portal: "
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
                logger.error("error '{}' occurred during delivery of {}".format(str(e), str(self)))
                logger.exception(e)
                raise(e)
            self.update_delivery_status(status="IN_PROGRESS")
        except Exception as e:
            self.update_delivery_status(status="STAGED")
            logger.exception(e)
            raise(e)

    def save_delivery_token_in_charon(self, delivery_token): #TODO: DDS delivery token? - can/shuold i use this somewhere?
        """Updates delivery_token in Charon at sample level
        """
        charon_session = CharonSession()
        charon_session.sample_update(self.projectid, self.sampleid, delivery_token=delivery_token)

    def add_dds_name_delivery_in_charon(self, name_of_delivery): #TODO: DDS delivery ID?  - can/shuold i use this somewhere?
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
            logger.error('Failed to update delivery_projects in charon while delivering {}. Error says: {}'.format(self.sampleid, e))
            logger.exception(e)
