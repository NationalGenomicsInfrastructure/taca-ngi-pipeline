"""
    Module for controlling deliveries os samples and projects to GRUS
"""
import paramiko
import getpass
import glob
import time
import stat
import requests
import datetime
from dateutil.relativedelta import relativedelta
import os
import logging
import couchdb
import json
import subprocess
from dateutil import parser
import sys
import re
import shutil

from ngi_pipeline.database.classes import CharonSession, CharonError
from taca.utils.filesystem import do_copy, create_folder
from taca.utils.config import CONFIG

from ..utils.database import statusdb_session
from deliver import ProjectDeliverer, SampleDeliverer, DelivererInterruptedError

logger = logging.getLogger(__name__)

yes = set(['yes','y', 'ye'])
no = set(['no','n'])
def proceed_or_not(question):
    sys.stdout.write("{}".format(question))
    while True:
        choice = raw_input().lower()
        if choice in yes:
            return True
        elif choice in no:
            return False
        else:
            sys.stdout.write("Please respond with 'yes' or 'no'")

def check_mover_version():
    cmd = ['moverinfo', '--version']
    output=subprocess.check_output(cmd, stderr=subprocess.STDOUT)
    m = re.search('.* version (\d\.\d\.\d)', output)
    if not m:
        logger.error("Probelm tring to idenitify mover version. Failed!")
        return False
    if m.group(1) != "1.0.0":
        logger.error("mover version is {}, only allowed version is 1.0.0. Please run module load mover/1.0.0 and retry".format(m.group(1)))
        return False
    return True #if I am here this is mover/1.0.0 so I am finr


class GrusProjectDeliverer(ProjectDeliverer):
    """ This object takes care of delivering project samples to castor's wharf.
    """
    def __init__(self, projectid=None, sampleid=None, pi_email=None, sensitive=True, hard_stage_only=False, add_user=None, fcid=None, **kwargs):
        super(GrusProjectDeliverer, self).__init__(
            projectid,
            sampleid,
            **kwargs
        )
        self.stagingpathhard = getattr(self, 'stagingpathhard', None)
        if self.stagingpathhard is None:
            raise AttributeError("stagingpathhard is required when delivering to GRUS")
        self.config_snic = CONFIG.get('snic',None)
        if self.config_snic is None:
            raise AttributeError("snic confoguration is needed  delivering to GRUS (snic_api_url, snic_api_user, snic_api_password")
        self.config_statusdb = CONFIG.get('statusdb',None)
        if self.config_statusdb is None:
            raise AttributeError("statusdb configuration is needed  delivering to GRUS (url, username, password, port")
        self.orderportal = CONFIG.get('order_portal',None) # do not need to raise exception here, I have already checked for this and monitoring does not need it
        if self.orderportal:
            self._set_pi_details(pi_email) # set PI email and SNIC id
            self._set_other_member_details(add_user, CONFIG.get('add_project_owner', False)) # set SNIC id for other project members
        self.sensitive = sensitive
        self.hard_stage_only = hard_stage_only
        self.fcid = fcid

    def get_delivery_status(self, dbentry=None):
        """ Returns the delivery status for this sample. If a sampleentry
        dict is supplied, it will be used instead of fethcing from database

        :params sampleentry: a database sample entry to use instead of
        fetching from db
        :returns: the delivery status of this sample as a string
        """
        dbentry = dbentry or self.db_entry()
        if dbentry.get('delivery_token'):
            if dbentry.get('delivery_token') not in ['NO-TOKEN', 'not_under_delivery'] :
                return 'IN_PROGRESS' #it means that at least some samples are under delivery
        if  dbentry.get('delivery_status'):
            if dbentry.get('delivery_status') == 'DELIVERED':
                return 'DELIVERED' #it means that the project has been marked as delivered
        if dbentry.get('delivery_projects'):
            return 'PARTIAL' #it means that the project underwent a delivery, but not for all the samples
        return 'NOT_DELIVERED' #last possible case is that the project is not delivered

    def check_mover_delivery_status(self):
        """ This function checks is project is under delivery. If so it waits until projects is delivered or a certain threshold is met
        """
        #first thing check that we are using mover 1.0.0
        if not check_mover_version():
             logger.error("Not delivering becouse wrong mover version detected")
             return False
        charon_status = self.get_delivery_status()
        # we don't care if delivery is not in progress
        if charon_status != 'IN_PROGRESS':
            logger.info("Project {} has no delivery token. Project is not being delivered at the moment".format(self.projectid))
            return
        # if it's 'IN_PROGRESS', checking moverinfo
        delivery_token = self.db_entry().get('delivery_token')
        logger.info("Project {} under delivery. Delivery token is {}. Starting monitoring:".format(self.projectid, delivery_token))
        delivery_status = 'IN_PROGRESS'
        not_monitoring = False
        max_delivery_time = relativedelta(days=7)
        monitoring_start = datetime.datetime.now()
        while ( not not_monitoring ):
            try:
                cmd = ['moverinfo', '-i', delivery_token]
                output=subprocess.check_output(cmd, stderr=subprocess.STDOUT)
            except Exception, e:
                logger.error('Cannot get the delivery status for project {}'.format(self.projectid))
                # write Traceback to the log file
                logger.exception(e)
                # we do not raise, but exit(1). Traceback will be written to log.
                exit(1)
            else:
                #Moverinfo output with option -i can be: InProgress, Accepted, Failed,
                mover_status = output.split(':')[0]
                if mover_status == 'Delivered':
                    # check the filesystem anyway
                    if os.path.exists(self.expand_path(self.stagingpathhard)):
                        logger.error('Delivery {} for project {} delivered done but project folder found in DELIVERY_HARD. Failing delivery.'.format(delivery_token, self.projectid))
                        delivery_status =  'FAILED'
                    else:
                        logger.info("Project {} succefully delivered. Delivery token is {}.".format(self.projectid, delivery_token))
                        delivery_status = 'DELIVERED'
                    not_monitoring = True #stop the monitoring, it is either failed or delivered
                    continue
                else:
                    #check for how long time delivery has been going on
                    if self.db_entry().get('delivery_started'):
                        delivery_started = self.db_entry().get('delivery_started')
                    else:
                        delivery_started = monitoring_start #the first time I checked the status, not necessarly when it begun
                    now = datetime.datetime.now()
                    if now -  max_delivery_time > delivery_started:
                        logger.error('Delivery {} for project {} has been ongoing for more than 48 hours. Check what the f**k is going on. The project status will be reset'.format(delivery_token, self.projectid))
                        delivery_status = 'FAILED'
                        not_monitoring = True #stop the monitoring, it is taking too long
                        continue
                if  mover_status == 'Accepted':
                    logger.info("Project {} under delivery. Status for delivery-token {} is : {}".format(self.projectid, delivery_token, mover_status))
                elif mover_status == 'Failed':
                    logger.warn("Project {} under delivery (attention mover returned {}). Status for delivery-token {} is : {}".format(self.projectid, mover_status, delivery_token, mover_status))
                elif mover_status == 'InProgress':
                    #this is an error because it is a new status
                    logger.info("Project {} under delivery. Status for delivery-token {} is : {}".format(self.projectid, delivery_token, mover_status))
                else:
                    logger.warn("Project {} under delivery. Unexpected status-delivery returned by mover for delivery-token {}: {}".format(self.projectid, delivery_token, mover_status))
            time.sleep(900) #sleep for 15 minutes and then check again the status
        #I am here only if not_monitoring is True, that is only if mover status was delivered or the delivery is ongoing for more than 48h
        if delivery_status == 'DELIVERED' or delivery_status == 'FAILED':
            #fetch all samples that were under delivery
            in_progress_samples = self.get_samples_from_charon(delivery_status="IN_PROGRESS")
            # now update them
            for sample_id in in_progress_samples:
                try:
                    sample_deliverer = GrusSampleDeliverer(self.projectid, sample_id)
                    sample_deliverer.update_delivery_status(status=delivery_status)
                except Exception, e:
                    logger.error('Sample {}: Problems in setting sample status on charon. Error: {}'.format(sample_id, error))
                    logger.exception(e)
            #now reset delivery
            self.delete_delivery_token_in_charon()
            #now check, if all samples in charon are DELIVERED or are ABORTED as status, then the all projecct is DELIVERED
            all_samples_delivered = True
            for sample_id in self.get_samples_from_charon(delivery_status=None):
                try:
                    sample_deliverer = GrusSampleDeliverer(self.projectid, sample_id)
                    if sample_deliverer.get_sample_status() == 'ABORTED':
                        continue
                    if sample_deliverer.get_delivery_status() != 'DELIVERED':
                        all_samples_delivered = False
                except Exception, e:
                    logger.error('Sample {}: Problems in setting sample status on charon. Error: {}'.format(sample_id, error))
                    logger.exception(e)
            if all_samples_delivered:
                self.update_delivery_status(status=delivery_status)

    def deliver_project(self):
        """ Deliver all samples in a project to grus
            :returns: True if all samples were delivered successfully, False if
                any sample was not properly delivered or ready to be delivered
        """
        #first thing check that we are using mover 1.0.0
        if not check_mover_version():
             logger.error("Not delivering becouse wrong mover version detected")
             return False
        # moved this part from constructor, as we can create an object without running the delivery (e.g. to check_delivery_status)
        #check if the project directory already exists, if so abort
        soft_stagepath = self.expand_path(self.stagingpath)
        hard_stagepath = self.expand_path(self.stagingpathhard)
        if os.path.exists(hard_stagepath):
            logger.error("In {} found already folder {}. No multiple mover deliveries are allowed".format(
                    hard_stagepath, self.projectid))
            raise DelivererInterruptedError("Hard Staged Folder already present")
        #check that this project is not under delivery with mover already in this case stop delivery
        if self.get_delivery_status() == 'DELIVERED' \
                and not self.force:
            logger.info("{} has already been delivered. This project will not be delivered again this time.".format(str(self)))
            return True
        elif self.get_delivery_status() == 'IN_PROGRESS':
            logger.error("Project {} is already under delivery. No multiple mover deliveries are allowed".format(
                    self.projectid))
            raise DelivererInterruptedError("Project already under delivery with Mover")
        elif self.get_delivery_status() == 'PARTIAL':
            logger.warning("{} has already been partially delivered. Please confirm you want to proceed.".format(str(self)))
            if proceed_or_not("Do you want to proceed (yes/no): "):
                logger.info("{} has already been partially delivered. User confirmed to proceed.".format(str(self)))
            else:
                logger.error("{} has already been partially delivered. User decided to not proceed.".format(str(self)))
                return False
        #now check if the sensitive flag has been set in the correct way
        question = "This project has been marked as SENSITIVE (option --sensitive). Do you want to proceed with delivery? "
        if not self.sensitive:
            question = "This project has been marked as NON-SENSITIVE (option --no-sensitive). Do you want to proceed with delivery? "
        if proceed_or_not(question):
            logger.info("Delivering {} to GRUS with mover. Project marked as SENSITIVE={}".format(str(self), self.sensitive))
        else:
            logger.error("{} delivery has been aborted. Sensitive level was WRONG.".format(str(self)))
            return False
        #now start with the real work
        status = True
        #otherwise lock the delivery by creating the folder
        create_folder(hard_stagepath)

        # connect to charon, return list of sample objects that have been staged
        try:
            samples_to_deliver = self.get_samples_from_charon(delivery_status="STAGED")
        except Exception, e:
            logger.error("Cannot get samples from Charon. Error says: {}".format(str(e)))
            logger.exception(e)
            raise e
        if len(samples_to_deliver) == 0:
            logger.warning('No staged samples found in Charon')
            raise AssertionError('No staged samples found in Charon')

        # collect other files (not samples) if any to include in the hard staging
        misc_to_deliver = [itm for itm in os.listdir(soft_stagepath) if os.path.splitext(itm)[0] not in samples_to_deliver]

        question = "\nProject stagepath: {}\nSamples: {}\nMiscellaneous: {}\n\nProceed with delivery ? "
        question = question.format(soft_stagepath, ", ".join(samples_to_deliver), ", ".join(misc_to_deliver))
        if proceed_or_not(question):
            logger.info("Proceeding with delivery of {}".format(str(self), self.sensitive))
        else:
            logger.error("Aborting delivery for {}, remove unwanted files and try again".format(str(self)))
            return False

        hard_staged_samples = []
        for sample_id in samples_to_deliver:
            try:
                sample_deliverer = GrusSampleDeliverer(self.projectid, sample_id)
                sample_deliverer.deliver_sample()
            except Exception, e:
                logger.error('Sample {} has not been hard staged. Error says: {}'.format(sample_id, e))
                logger.exception(e)
                raise e
            else:
                hard_staged_samples.append(sample_id)
        if len(samples_to_deliver) != len(hard_staged_samples):
            # Something unexpected happend, terminate
            logger.warning('Not all the samples have been hard staged. Terminating')
            raise AssertionError('len(samples_to_deliver) != len(hard_staged_samples): {} != {}'.format(len(samples_to_deliver),
                                                                                                        len(hard_staged_samples)))

        hard_staged_misc = []
        for itm in misc_to_deliver:
            src_misc = os.path.join(soft_stagepath, itm)
            dst_misc = os.path.join(hard_stagepath, itm)
            try:
                if os.path.isdir(src_misc):
                    shutil.copytree(src_misc, dst_misc)
                else:
                    shutil.copy(src_misc, dst_misc)
                hard_staged_misc.append(itm)
            except Exception, e:
                logger.error('Miscellaneous file {} has not been hard staged for project {}. Error says: {}'.format(itm, proj, e))
                logger.exception(e)
                raise e
        if len(misc_to_deliver) != len(hard_staged_misc):
            # Something unexpected happend, terminate
            logger.warning('Not all the Miscellaneous files have been hard staged for project {}. Terminating'.format(proj))
            raise AssertionError('len(misc_to_deliver) != len(hard_staged_misc): {} != {}'.format(len(misc_to_deliver),
                                                                                                  len(hard_staged_misc)))

        # create a delivery project id
        supr_name_of_delivery = ''
        try:
            delivery_project_info = self._create_delivery_project()
            supr_name_of_delivery = delivery_project_info['name']
            logger.info("Delivery project for project {} has been created. Delivery IDis {}".format(self.projectid, supr_name_of_delivery))
        except Exception, e:
            logger.error('Cannot create delivery project. Error says: {}'.format())
            logger.exception(e)
        delivery_token = self.do_delivery(supr_name_of_delivery) # instead of to_outbox
        #at this point I have delivery_token and supr_name_of_delivery so I need to update the project fields and the samples fields
        if delivery_token:
            #memorise the delivery token used to check if project is under delivery
            self.save_delivery_token_in_charon(delivery_token)
            #memorise the delivery project so I know each NGi project to how many delivery projects it has been sent
            self.add_supr_name_delivery_in_charon(supr_name_of_delivery)
            self.add_supr_name_delivery_in_statusdb(supr_name_of_delivery)
            logger.info("Delivery token for project {}, delivery project {} is {}".format(self.projectid,
                                                                                    supr_name_of_delivery,
                                                                                    delivery_token))
            for sample_id in samples_to_deliver:
                try:
                    sample_deliverer = GrusSampleDeliverer(self.projectid, sample_id)
                    sample_deliverer.save_delivery_token_in_charon(delivery_token)
                    sample_deliverer.add_supr_name_delivery_in_charon(supr_name_of_delivery)
                except Exception, e:
                    logger.error('Failed in saving sample infomration for sample {}. Error says: {}'.format(sample_id, e))
                    logger.exception(e)
        else:
            logger.error('Delivery project for project {} has not been created'.format(self.projectid))
            status = False
        return status

    def deliver_run_folder(self):
        '''Hard stages run folder and initiates delivery
        '''
        #stage the data
        dst = self.expand_path(self.stagingpathhard)
        path_to_data = self.expand_path(self.datapath)
        runfolder_archive = os.path.join(path_to_data, self.fcid + ".tar.gz")
        runfolder_md5file = runfolder_archive + ".md5"

        question = "This project has been marked as SENSITIVE (option --sensitive). Do you want to proceed with delivery? "
        if not self.sensitive:
            question = "This project has been marked as NON-SENSITIVE (option --no-sensitive). Do you want to proceed with delivery? "
        if proceed_or_not(question):
            logger.info("Delivering {} to GRUS with mover. Project marked as SENSITIVE={}".format(str(self), self.sensitive))
        else:
            logger.error("{} delivery has been aborted. Sensitive level was WRONG.".format(str(self)))
            return False

        status = True

        create_folder(dst)
        try:
            shutil.copy(runfolder_archive, dst)
            shutil.copy(runfolder_md5file, dst)
            logger.info("Copying files {} and {} to {}".format(runfolder_archive, runfolder_md5file, dst))
        except IOError, e:
            logger.error("Unable to copy files to {}. Please check that the files exist and that the filenames match the flowcell ID.".format(dst))

        delivery_id = ''
        try:
            delivery_project_info = self._create_delivery_project()
            delivery_id = delivery_project_info['name']
            logger.info("Delivery project for project {} has been created. Delivery IDis {}".format(self.projectid, delivery_id))
        except Exception, e:
            logger.error('Cannot create delivery project. Error says: {}'.format())
            logger.exception(e)

        #invoke mover
        delivery_token = self.do_delivery(delivery_id)

        if delivery_token:
            logger.info("Delivery token for project {}, delivery project {} is {}".format(self.projectid,
                                                                                    delivery_id,
                                                                                    delivery_token))
        else:
            logger.error('Delivery project for project {} has not been created'.format(self.projectid))
            status = False
        return status


    def save_delivery_token_in_charon(self, delivery_token):
        '''Updates delivery_token in Charon at project level
        '''
        charon_session = CharonSession()
        charon_session.project_update(self.projectid, delivery_token=delivery_token)

    def delete_delivery_token_in_charon(self):
        '''Removes delivery_token from Charon upon successful delivery
        '''
        charon_session = CharonSession()
        charon_session.project_update(self.projectid, delivery_token='NO-TOKEN')

    def get_delivery_token_in_charon(self):
        '''fetches delivery_token from Charon
        '''
        charon_session = CharonSession()
        project_charon = charon_session.project_get(self.projectid)
        if project_charon.get('delivery_token'):
            return project_charon.get('delivery_token')
        else:
            return 'NO-TOKEN'

    def add_supr_name_delivery_in_charon(self, supr_name_of_delivery):
        '''Updates delivery_projects in Charon at project level
        '''
        charon_session = CharonSession()
        try:
            #fetch the project
            project_charon = charon_session.project_get(self.projectid)
            delivery_projects = project_charon['delivery_projects']
            if supr_name_of_delivery not in delivery_projects:
                delivery_projects.append(supr_name_of_delivery)
                charon_session.project_update(self.projectid, delivery_projects=delivery_projects)
                logger.info('Charon delivery_projects for project {} updated with value {}'.format(self.projectid, supr_name_of_delivery))
            else:
                logger.warn('Charon delivery_projects for project {} not updated with value {} because the value was already present'.format(self.projectid, supr_name_of_delivery))
        except Exception, e:
            logger.error('Failed to update delivery_projects in charon while delivering {}. Error says: {}'.format(self.projectid, e))
            logger.exception(e)

    def add_supr_name_delivery_in_statusdb(self, supr_name_of_delivery):
        '''Updates delivery_projects in StatusDB at project level
        '''
        save_meta_info = getattr(self, 'save_meta_info', False)
        if not save_meta_info:
            return
        status_db = statusdb_session(self.config_statusdb)
        project_page=status_db.get_project(self.projectid)
        dprojs=[]
        if 'delivery_projects' in project_page:
            dprojs=project_page['delivery_projects']

        dprojs.append(supr_name_of_delivery)

        project_page['delivery_projects'] = dprojs
        try:
            status_db.connection['projects'].save(project_page)
            logger.info('Delivery_projects for project {} updated with value {} in statusdb'.format(self.projectid, supr_name_of_delivery))
        except Exception, e:
            logger.error('Failed to update delivery_projects in statusdb while delivering {}. Error says: {}'.format(projectid, e))
            logger.exception(e)

    def do_delivery(self, supr_name_of_delivery):
        # this one returns error : "265 is non-existing at /usr/local/bin/to_outbox line 214". (265 is delivery_project_id, created via api)
        # or: id=P6968-ngi-sw-1488209917 Error: receiver 274 does not exist or has expired.
        hard_stage = self.expand_path(self.stagingpathhard)
        #need to change group to all files
        os.chown(hard_stage, -1, 47537)
        for root, dirs, files in os.walk(hard_stage):
            for dir in dirs:
                dir_path = os.path.join(root, dir)
                os.chown(dir_path, -1, 47537) #gr_id is the one of ngi2016003
            for file in files:
                fname = os.path.join(root, file)
                os.chown(fname, -1, 47537)
        cmd = ['to_outbox', hard_stage, supr_name_of_delivery]
        if self.hard_stage_only:
            logger.warning("to_mover command not executed, only hard-staging done. Do what you need to do and then run: {}".format(" ".join(cmd)))
            return "manually-set-up"

        try:
            output=subprocess.check_output(cmd, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            logger.error('to_outbox failed while delivering {} to {}'.format(hard_stage, supr_name_of_delivery))
            logger.exception(e)
        delivery_token = output.rstrip()
        return delivery_token

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
        create_project_url = '{}/ngi_delivery/project/create/'.format(self.config_snic.get('snic_api_url'))
        user               = self.config_snic.get('snic_api_user')
        password           = self.config_snic.get('snic_api_password')
        supr_date_format = '%Y-%m-%d'
        today = datetime.date.today()
        three_months_from_now = (today + relativedelta(months=+3))
        data = {
            'ngi_project_name': self.projectid,
            'title': "DELIVERY_{}_{}".format(self.projectid, today.strftime(supr_date_format)),
            'pi_id': self.pi_snic_id,
            'start_date': today.strftime(supr_date_format),
            'end_date': three_months_from_now.strftime(supr_date_format),
            'continuation_name': '',
            # You can use this field to allocate the size of the delivery
            # 'allocated': size_of_delivery,
            # This field can be used to add any data you like
            'api_opaque_data': '',
            'ngi_ready': False,
            'ngi_delivery_status': '',
            'ngi_sensitive_data': self.sensitive,
            'member_ids': self.other_member_snic_ids
            }
        response = requests.post(create_project_url, data=json.dumps(data), auth=(user, password))
        if response.status_code != 200:
            raise AssertionError("API returned status code {}. Response: {}. URL: {}".format(response.status_code, response.content, create_project_url))
        result = json.loads(response.content)
        return result

    def _set_pi_details(self, given_pi_email=None):
        """
            Set PI email address and PI SNIC ID using PI email
        """
        self.pi_email, self.pi_snic_id = (None, None)
        # try getting PI email
        if given_pi_email:
            logger.warning("PI email for project {} specified by user: {}".format(self.projectid, given_pi_email))
            self.pi_email = given_pi_email
        else:
            try:
                self.pi_email = self._get_order_detail()['fields']['project_pi_email']
                logger.info("PI email for project {} found: {}".format(self.projectid, self.pi_email))
            except Exception, e:
                logger.error("Cannot fetch pi_email from StatusDB. Error says: {}".format(str(e)))
                raise e
        # try getting PI SNIC ID
        try:
            self.pi_snic_id = self._get_user_snic_id(self.pi_email)
            logger.info("SNIC PI-id for delivering of project {} is {}".format(self.projectid, self.pi_snic_id))
        except Exception, e:
            logger.error("Cannot fetch PI SNIC id using snic API. Error says: {}".format(str(e)))
            raise e

    def _set_other_member_details(self, other_member_emails=[], include_owner=False):
        """
            Set other contact details if avilable, this is not mandatory so
            the method will not raise error if it could not find any contact
        """
        self.other_member_snic_ids = []
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
        # try getting snic id for other emails if any
        for uemail in other_member_emails:
            try:
                self.other_member_snic_ids.append(self._get_user_snic_id(uemail))
            except:
                logger.warning("Was not able to get SNIC id for email {}, so that user will not be included in the GRUS project".format(uemail))

    def _get_user_snic_id(self, uemail):
        user = self.config_snic.get('snic_api_user')
        password = self.config_snic.get('snic_api_password')
        get_user_url = '{}/person/search/'.format(self.config_snic.get('snic_api_url'))
        params   = {'email_i': uemail}
        response = requests.get(get_user_url, params=params, auth=(user, password))
        if response.status_code != 200:
            raise AssertionError("Unexpected code returned when trying to get SNIC id for email: {}. Response was: {}".format(uemail, response.content))
        result = json.loads(response.content)
        matches = result.get("matches")
        if matches is None:
            raise AssertionError('The response returned unexpected data')
        if len(matches) < 1:
            raise AssertionError("There was no hit in SUPR for email: {}".format(uemail))
        if len(matches) > 1:
            raise AssertionError("There were more than one hit in SUPR for email: {}".format(uemail))
        return matches[0].get("id")

    def _get_order_detail(self):
        status_db = statusdb_session(self.config_statusdb)
        projects_db = status_db.connection['projects']
        view = projects_db.view('order_portal/ProjectID_to_PortalID')
        rows = view[self.projectid].rows
        if len(rows) < 1:
            raise AssertionError("Project {} not found in StatusDB".format(self.projectid))
        if len(rows) > 1:
            raise AssertionError('Project {} has more than one entry in orderportal_db'.format(self.projectid))
        portal_id = rows[0].value
        #now get the PI email from order portal API
        get_project_url = '{}/v1/order/{}'.format(self.orderportal.get('orderportal_api_url'), portal_id)
        headers = {'X-OrderPortal-API-key': self.orderportal.get('orderportal_api_token')}
        response = requests.get(get_project_url, headers=headers)
        if response.status_code != 200:
            raise AssertionError("Status code returned when trying to get PI email from project in order portal: {} was not 200. Response was: {}".format(portal_id, response.content))
        return json.loads(response.content)


class GrusSampleDeliverer(SampleDeliverer):
    """
        A class for handling sample deliveries to castor
    """

    def __init__(self, projectid=None, sampleid=None, **kwargs):
        super(GrusSampleDeliverer, self).__init__(
            projectid,
            sampleid,
            **kwargs)

    def deliver_sample(self, sampleentry=None):
        """ Deliver a sample to the destination specified via command line of on Charon.
            Will check if the sample has already been delivered and should not
            be delivered again or if the sample is not yet ready to be delivered.
            Delivers only samples that have been staged.

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
        # try:
        logger.info("Delivering {} to GRUS with MOVER".format(str(self)))
        hard_stagepath = self.expand_path(self.stagingpathhard)
        soft_stagepath = self.expand_path(self.stagingpath)

        try:
            logger.info("Trying to deliver {} to GRUS with MOVER".format(str(self)))
            hard_stagepath = self.expand_path(self.stagingpathhard)
            soft_stagepath = self.expand_path(self.stagingpath)
            try:
                if self.get_delivery_status(sampleentry) != 'STAGED':
                    logger.info("{} has not been staged and will not be delivered".format(str(self)))
                    return False
            except db.DatabaseError as e:
                logger.error("error '{}' occurred during delivery of {}".format(str(e), str(self)))
                logger.exception(e)
                raise(e)
            #at this point copywith deferance the softlink folder
            self.update_delivery_status(status="IN_PROGRESS")
            self.do_delivery()
        #in case of faiulure put again the status to STAGED
        except DelivererInterruptedError, e:
            self.update_delivery_status(status="STAGED")
            logger.exception(e)
            raise(e)
        except Exception, e:
            self.update_delivery_status(status="STAGED")
            logger.exception(e)
            raise(e)

    def save_delivery_token_in_charon(self, delivery_token):
        '''Updates delivery_token in Charon at sample level
        '''
        charon_session = CharonSession()
        charon_session.sample_update(self.projectid, self.sampleid, delivery_token=delivery_token)

    def add_supr_name_delivery_in_charon(self, supr_name_of_delivery):
        '''Updates delivery_projects in Charon at project level
        '''
        charon_session = CharonSession()
        try:
            #fetch the project
            sample_charon = charon_session.sample_get(self.projectid, self.sampleid)
            delivery_projects = sample_charon['delivery_projects']
            if supr_name_of_delivery not in sample_charon:
                delivery_projects.append(supr_name_of_delivery)
                charon_session.sample_update(self.projectid, self.sampleid, delivery_projects=delivery_projects)
                logger.info('Charon delivery_projects for sample {} updated with value {}'.format(self.sampleid, supr_name_of_delivery))
            else:
                logger.warn('Charon delivery_projects for sample {} not updated with value {} because the value was already present'.format(self.sampleid, supr_name_of_delivery))
        except Exception, e:
            logger.error('Failed to update delivery_projects in charon while delivering {}. Error says: {}'.format(self.sampleid, e))
            logger.exception(e)

    def do_delivery(self):
        """ Creating a hard copy of staged data
        """
        logger.info("Creating hard copy of sample {}".format(self.sampleid))
        # join stage dir with sample dir
        source_dir = os.path.join(self.expand_path(self.stagingpath), self.sampleid)
        destination_dir = os.path.join(self.expand_path(self.stagingpathhard), self.sampleid)
        # destination must NOT exist
        do_copy(source_dir, destination_dir)
        #now copy md5 and other files
        for file in glob.glob("{}.*".format(source_dir)):
            shutil.copy(file, self.expand_path(self.stagingpathhard))
        logger.info("Sample {} has been hard staged to {}".format(self.sampleid, destination_dir))
        return
