"""
    Module for controlling deliveries of project run folders to GRUS
"""
#import paramiko
#import getpass
#import glob
#import time
#import stat
#import requests
#import datetime
#from dateutil.relativedelta import relativedelta
#import os
import logging
#import couchdb
#import json
#import subprocess
#from dateutil import parser
#import sys
#import re
#import shutil

#from ngi_pipeline.database.classes import CharonSession, CharonError
#from taca.utils.filesystem import do_copy, create_folder
#from taca.utils.config import CONFIG

#from ..utils.database import statusdb_session
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
        logger.error("Problem trying to idenitify mover version. Failed!")
        return False
    if m.group(1) != "1.0.0":
        logger.error("mover version is {}, only allowed version is 1.0.0. Please run module load mover/1.0.0 and retry".format(m.group(1)))
        return False
    return True #if I am here this is mover/1.0.0 so I am fine


class GrusProjectRunDeliverer(ProjectDeliverer):
    """ This object takes care of delivering project run folders.
    """

    def __init__(self, projectid=None, **kwargs):
        super(GrusProjectRunDeliverer, self).__init__(
            projectid,
            **kwargs
            )
#        self.stagingpathhard = getattr(self, 'stagingpathhard', None)
#        if self.stagingpathhard is None:
#            raise AttributeError("stagingpathhard is required when delivering to GRUS")


#    def __init__(self, projectid=None, sampleid=None, pi_email=None, sensitive=True, hard_stage_only=False, add_user=None, **kwargs):
#        super(GrusProjectDeliverer, self).__init__(
#            projectid,
#            sampleid,
#            **kwargs
#        )
#        self.stagingpathhard = getattr(self, 'stagingpathhard', None)
#        if self.stagingpathhard is None:
#            raise AttributeError("stagingpathhard is required when delivering to GRUS")
#        self.config_snic = CONFIG.get('snic',None)
#        if self.config_snic is None:
#            raise AttributeError("snic confoguration is needed  delivering to GRUS (snic_api_url, snic_api_user, snic_api_password")
#        self.config_statusdb = CONFIG.get('statusdb',None)
#        if self.config_statusdb is None:
#            raise AttributeError("statusdb configuration is needed  delivering to GRUS (url, username, password, port")
#        self.orderportal = CONFIG.get('order_portal',None) # do not need to raise exception here, I have already checked for this and monitoring does not need it
#        if self.orderportal:
#            self._set_pi_details(pi_email) # set PI email and SNIC id
#            self._set_other_member_details(add_user, CONFIG.get('add_project_owner', False)) # set SNIC id for other project members
#        self.sensitive = sensitive
#        self.hard_stage_only = hard_stage_only

    def deliver_project(self):
        """ Deliver run folder to grus
            :returns: True if run folder was delivered successfully, False if
                it was not properly delivered or ready to be delivered
        """
        print "Delivering runfolder for project " + self.projectid
