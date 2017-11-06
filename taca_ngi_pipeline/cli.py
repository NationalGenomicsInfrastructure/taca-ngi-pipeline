""" CLI for the deliver subcommand
"""
import click
import logging
import os
import subprocess

from ngi_pipeline.database.classes import CharonSession
from taca.utils.config import CONFIG

import taca.utils.misc
from deliver import deliver as _deliver
from deliver import deliver_mosler as _deliver_mosler
from deliver import deliver_castor as _deliver_castor
from deliver import deliver_grus as _deliver_grus

from deliver.deliver_grus import GrusProjectDeliverer

logger = logging.getLogger(__name__)

#######################################
# deliver
#######################################


@click.group()
@click.pass_context
@click.option('--deliverypath', type=click.STRING, help="Deliver to this destination folder")
@click.option('--stagingpath', type=click.STRING, help="Stage the delivery under this path")
@click.option('--uppnexid', type=click.STRING, help="Use this UppnexID instead of fetching from database")
@click.option('--operator', type=click.STRING, default=None, multiple=True,
              help="Email address to notify operator at. Multiple operators can be specified")
@click.option('--stage_only', is_flag=True, default=False,
              help="Only stage the delivery but do not transfer any files")
@click.option('--ignore-analysis-status', is_flag=True, default=False,
              help="Do not check analysis status upon delivery. To be used only when delivering projects without BP (e.g., WHG)")
@click.option('--force', is_flag=True, default=False,
              help="Force delivery, even if e.g. analysis has not finished or sample has already been delivered")
@click.option('--cluster', default="milou",  type=click.Choice(['milou', 'mosler', 'bianca', 'grus']),
              help="Specify to which cluster one wants to deliver")



def deliver(ctx, deliverypath, stagingpath, uppnexid, operator, stage_only, force, cluster, ignore_analysis_status):
    """ Deliver methods entry point
    """
    if deliverypath is None:
        del ctx.params['deliverypath']
    if stagingpath is None:
        del ctx.params['stagingpath']
    if uppnexid is None:
        del ctx.params['uppnexid']
    if operator is None or len(operator) == 0:
        del ctx.params['operator']

# deliver subcommands
# project delivery
@deliver.command()
@click.pass_context
@click.argument('projectid', type=click.STRING, nargs=-1)
@click.option('--snic-api-credentials',
            default=None,
            envvar='SNIC_API_STOCKHOLM',
            type=click.File('r'),
            help='Path to SNIC-API credentials to create delivery projects')
@click.option('--statusdb-config',
            default=None,
            envvar='STATUS_DB_CONFIG',
            type=click.File('r'),
            help='Path to statusdb-configuration')
@click.option('--order-portal',
            default=None,
            envvar='ORDER_PORTAL',
            type=click.File('r'),
            help='Path to order portal credantials to retrive PI email')
@click.option('--pi-email',
            default=None,
            type=click.STRING,
            help='pi-email, to be specified if PI-email stored in statusdb does not correspond SUPR PI-email')
@click.option('--include-project',
            type=click.STRING,
            multiple=True,
            help='List of project that to be included in same delivery project')
@click.option('--delivery-name',
            default=None,
            type=click.STRING,
            help='A name for target directory in which all projects (main project and projects passed by "--include-project") will be hard staged')
@click.option('--sensitive/--no-sensitive',
            default=True,
            help='flag to specify if data contained in the project is sensitive or not')
@click.option('--hard-stage-only',
            is_flag=True,
            default=False,
            help='Perform all the delivery actions but does not run to_mover (to be used for semi-manual deliveries)')

def project(ctx, projectid, snic_api_credentials=None, statusdb_config=None, order_portal=None, pi_email=None, include_project=None, delivery_name=None, sensitive=True, hard_stage_only=False):
    """ Deliver the specified projects to the specified destination
    """
    if len(projectid) > 1:
        if ctx.parent.params['cluster'] == 'bianca':
            logger.error("Only one project can be specified when delivering to Bianca. Specficied {} projects".format(len(projectid)))
            return 1
        elif len(projectid) != len(set(projectid)):
            logger.error("Given project list '[{}]' have repeated project(s). Kindly double check the command".format(" ".join(projectid)))
            return 1
        elif len(include_project) > 0:
            logger.error("Can not give '--include-project' ({}) while delivering multiple projects ({})".format(" ".join(include_project), " ".join(projectid)))
            return 1
    for pid in projectid:
        if ctx.parent.params['cluster'] == 'milou':
            d = _deliver.ProjectDeliverer(
                pid,
                **ctx.parent.params)
        elif ctx.parent.params['cluster'] == 'mosler':
            d = _deliver_mosler.MoslerProjectDeliverer(
                pid,
                **ctx.parent.params)
        elif ctx.parent.params['cluster'] == 'bianca':
            d = _deliver_castor.CastorProjectDeliverer(
                pid,
                **ctx.parent.params)
        elif ctx.parent.params['cluster'] == 'grus':
            if statusdb_config == None:
                logger.error("--statusdb-config or env variable $STATUS_DB_CONFIG need to be set to perform GRUS delivery")
                return 1
            taca.utils.config.load_yaml_config(statusdb_config)
            if snic_api_credentials == None:
                logger.error("--snic-api-credentials or env variable $SNIC_API_STOCKHOLM need to be set to perform GRUS delivery")
                return 1
            taca.utils.config.load_yaml_config(snic_api_credentials)
            if order_portal == None:
                logger.error("--order-portal or env variable $ORDER_PORTAL need to be set to perform GRUS delivery")
                return 1
            taca.utils.config.load_yaml_config(order_portal)
            if len(include_project) > 0:
                if pid in include_project:
                    logger.error("Project {} is given as main project and also by '--include-project' ({})".format(pid, " ".join(include_project)))
                    return 1
                if delivery_name == None:
                    delivery_name = "-".join(projectid + include_project)
                    logger.warn("Project(s) are ({}) given by '--include-project' but no '--delivery-name' given. Will use '{}' as delivery name"
                            .format(" ".join(include_project), delivery_name))
            elif delivery_name != None:
                logger.error("Option '--delivery-name' can be given only with '--include-project'")
                return 1
            d = _deliver_grus.GrusProjectDeliverer(
                projectid=pid,
                pi_email=pi_email,
                sensitive=sensitive,
                hard_stage_only=hard_stage_only,
                include_project=include_project,
                delivery_name=delivery_name,
                **ctx.parent.params)
        _exec_fn(d, d.deliver_project)

# sample delivery
@deliver.command()
@click.pass_context
@click.argument('projectid', type=click.STRING, nargs=1)
@click.argument('sampleid', type=click.STRING, nargs=-1)
def sample(ctx, projectid, sampleid):
    """ Deliver the specified sample to the specified destination
    """
    if ctx.parent.params['cluster'] == 'bianca':
        #in this case I need to open a sftp connection in order to avoid to insert password everytime
        projectObj = _deliver_castor.CastorProjectDeliverer(projectid, **ctx.parent.params)
        projectObj.create_sftp_connnection()
        #create the project folder in the remote server
        #move to delivery folder
        projectObj.sftp_client.chdir(projectObj.config.get('castordeliverypath', '/wharf'))
        projectObj.sftp_client.mkdir(projectid, ignore_existing=True)
        #move inside the project folder
        projectObj.sftp_client.chdir(projectid)
    for sid in sampleid:
        if ctx.parent.params['cluster'] == 'milou':
            d = _deliver.SampleDeliverer(
                projectid,
                sid,
                **ctx.parent.params)
        elif ctx.parent.params['cluster'] == 'mosler':
            d = _deliver_mosler.MoslerSampleDeliverer(
                projectid,
                sid,
                **ctx.parent.params)
        elif ctx.parent.params['cluster'] == 'bianca':
            d = _deliver_castor.CastorSampleDeliverer(
                projectid,
                sid,
                sftp_client=projectObj.sftp_client,
                **ctx.parent.params)
        elif ctx.parent.params['cluster'] == 'grus':
            logger.error("When delivering to grus only project can be specified, not sample")
            return 1
        _exec_fn(d, d.deliver_sample)
    if ctx.parent.params['cluster'] == 'bianca':
        projectObj.close_sftp_connnection()


# helper function to handle error reporting
def _exec_fn(obj, fn):
    try:
        if fn():
            logger.info(
                "{} processed successfully".format(str(obj)))
        else:
            logger.info(
                "{} processed with some errors, check log".format(
                    str(obj)))
    except Exception as e:
        logger.exception(e)
        try:
            taca.utils.misc.send_mail(
                subject="[ERROR] processing failed: {}".format(str(obj)),
                content="Project: {}\nSample: {}\nCommand: {}\n\nAdditional information:{}\n".format(
                    obj.projectid, obj.sampleid, str(fn), str(e)),
                receiver=obj.config.get('operator'))
        except Exception as me:
            logger.error(
                "processing {} failed - reason: {}, but operator {} could not be notified - reason: {}".format(
                    str(obj), e, obj.config.get('operator'), me))
        else:
            logger.error("processing {} failed - reason: {}, operator {} has been notified".format(
                str(obj), str(e), obj.config.get('operator')))




@deliver.command()
@click.pass_context
@click.argument('projectid', type=click.STRING, nargs=-1)
@click.option('--snic-api-credentials',
			  default=None,
			  envvar='SNIC_API_STOCKHOLM',
			  type=click.File('r'),
			  help='Path to SNIC-API credentials to create delivery projects')
@click.option('--statusdb-config',
			  default=None,
			  envvar='STATUS_DB_CONFIG',
			  type=click.File('r'),
			  help='Path to statusdb-configuration')

def check_status(ctx, projectid, snic_api_credentials=None, statusdb_config=None):
    """In grus delivery mode checks the status of an onggoing delivery
    """
    for pid in projectid:
        if statusdb_config == None:
            logger.error("--statusdb-config or env variable $STATUS_DB_CONFIG need to be set to perform GRUS delivery")
            return 1
        taca.utils.config.load_yaml_config(statusdb_config)
        if snic_api_credentials == None:
            logger.error("--snic-api-credentials or env variable $SNIC_API_STOCKHOLM need to be set to perform GRUS delivery")
            return 1
        taca.utils.config.load_yaml_config(snic_api_credentials)

        d = _deliver_grus.GrusProjectDeliverer(
                pid,
                **ctx.parent.params)
        d.check_mover_delivery_status()



