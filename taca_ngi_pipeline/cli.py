""" CLI for the deliver subcommand
"""
import click
import logging

from taca.utils.misc import send_mail
from taca.utils.config import load_yaml_config
from taca_ngi_pipeline.deliver import deliver as _deliver
from taca_ngi_pipeline.deliver import deliver_grus as _deliver_grus
from taca_ngi_pipeline.deliver import deliver_dds as _deliver_dds

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
@click.option('--cluster', type=click.Choice(['grus', 'dds']), # Can be expanded to include future clusters
              help="Specify to which cluster one wants to deliver")
@click.option('--generate_xml_and_manifest_files_only', is_flag=True,  default=False,
              help="Explicitly generate xml amd manifest files for ENA submission on a staged project")


def deliver(ctx, deliverypath, stagingpath, 
            uppnexid, operator, stage_only, 
            force, cluster, ignore_analysis_status,
            generate_xml_and_manifest_files_only):
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
@click.option('--sensitive/--no-sensitive',
            default=True,
            help='flag to specify if data contained in the project is sensitive or not')
@click.option('--hard-stage-only',
            is_flag=True,
            default=False,
            help='Perform all the delivery actions but does not run to_mover (to be used for semi-manual deliveries)')
@click.option('--add-user',
            multiple=True,
            type=click.STRING,
            help='User email address to add in GRUS delivery project. Multiple user can be given by calling parameter multiple times')
@click.option('--fc-delivery',
              default=False,
              type=click.STRING,
              help='Flowcell id for delivering whole Illumnina run folder')
@click.option('--project-title',
            default=None,
            type=click.STRING,
            help='Project title, to be specified if project not in order portal (DDS only)')
@click.option('--project-desc',
            default=None,
            type=click.STRING,
            help='Project description, to be specified if project not in order portal (DDS only)')

def project(ctx, projectid, 
            snic_api_credentials=None, statusdb_config=None, 
            order_portal=None, pi_email=None,
            sensitive=True, hard_stage_only=False, 
            add_user=None, fc_delivery=False,
            project_title=None, project_desc=None):
    """ Deliver the specified projects to the specified destination
    """
    for pid in projectid:
        if ctx.parent.params['cluster']:
            if statusdb_config == None:
                logger.error("--statusdb-config or env variable $STATUS_DB_CONFIG"
                             " need to be set to perform {} delivery".format(ctx.parent.params['cluster']))
                return 1
            load_yaml_config(statusdb_config.name)
            if order_portal == None:
                logger.error("--order-portal or env variable $ORDER_PORTAL"
                             " need to be set to perform {} delivery".format(ctx.parent.params['cluster']))
                return 1
            load_yaml_config(order_portal.name)
        if not ctx.parent.params['cluster']: # Soft stage case
            d = _deliver.ProjectDeliverer(
                pid,
                **ctx.parent.params)
        elif ctx.parent.params['cluster'] == 'grus': # Hard stage and deliver to GRUS
            if snic_api_credentials == None:
                logger.error("--snic-api-credentials or env variable $SNIC_API_STOCKHOLM need to be set to perform GRUS delivery")
                return 1
            load_yaml_config(snic_api_credentials.name)
            d = _deliver_grus.GrusProjectDeliverer(
                projectid=pid,
                pi_email=pi_email,
                sensitive=sensitive,
                hard_stage_only=hard_stage_only,
                add_user=list(set(add_user)),
                fcid=fc_delivery,
                **ctx.parent.params)
        elif ctx.parent.params['cluster'] == 'dds': # Hard stage and deliver using DDS
            d = _deliver_dds.DDSProjectDeliverer(
                projectid=pid,
                pi_email=pi_email,
                sensitive=sensitive,
                add_user=list(set(add_user)),
                fcid=fc_delivery,
                do_release=False,
                project_title=project_title,
                project_description=project_desc,
                **ctx.parent.params)
            

        if fc_delivery:
            _exec_fn(d, d.deliver_run_folder)
        else:
            _exec_fn(d, d.deliver_project)

# sample delivery
#TODO: not used? remove?
@deliver.command()
@click.pass_context
@click.argument('projectid', type=click.STRING, nargs=1)
@click.argument('sampleid', type=click.STRING, nargs=-1)
def sample(ctx, projectid, sampleid):
    """ Deliver the specified sample to the specified destination
    """
    for sid in sampleid:
        if not ctx.parent.params['cluster']: # Soft stage case
            d = _deliver.SampleDeliverer(
                projectid,
                sid,
                **ctx.parent.params)
        elif ctx.parent.params['cluster'] == 'grus':
            logger.error("When delivering to grus only project can be specified, not sample")
            return 1
        elif ctx.parent.params['cluster'] == 'dds':
            logger.error("When delivering with DDS only project can be specified, not sample")
            return 1
        _exec_fn(d, d.deliver_sample)

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
            send_mail(
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


# check status of ongoing GRUS delivery
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
        load_yaml_config(statusdb_config.name)
        if snic_api_credentials == None:
            logger.error("--snic-api-credentials or env variable $SNIC_API_STOCKHOLM need to be set to perform GRUS delivery")
            return 1
        load_yaml_config(snic_api_credentials.name)

        d = _deliver_grus.GrusProjectDeliverer(
                pid,
                **ctx.parent.params)
        d.check_mover_delivery_status()

@deliver.command()
@click.pass_context
@click.argument('projectid', type=click.STRING)
@click.option('--dds_project',
              default=None,
              type=click.STRING,
              help='DDS project ID to release')
@click.option('--dds_deadline',
              default=45,
              type=click.IntRange(1, 90),
              help='Deadline for DDS project in days [min 1; max 90; default 45]')

def release_dds_project(ctx, projectid, dds_project, dds_deadline):
    """Updates DDS delivery status in Charon and releases DDS project to user.
    """
    if not dds_project:
        logger.error('Please specify the DDS project ID to release with --dds_project')
        return 1
    d = _deliver_dds.DDSProjectDeliverer(
        projectid,
        do_release=True,
        **ctx.parent.params)
    d.release_DDS_delivery_project(dds_project, dds_deadline)