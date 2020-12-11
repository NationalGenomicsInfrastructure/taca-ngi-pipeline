import unittest
import shutil
import tempfile
import datetime
import json
import os
from mock import patch, call
from dateutil.relativedelta import relativedelta

from taca_ngi_pipeline.deliver.deliver_grus import GrusProjectDeliverer, GrusSampleDeliverer, proceed_or_not, check_mover_version

SAMPLECFG = {
    'deliver': {
        'analysispath': '<ROOTDIR>/ANALYSIS',
        'datapath': '<ROOTDIR>/DATA',
        'stagingpath': '<ROOTDIR>/STAGING',
        'stagingpathhard': '<ROOTDIR>/STAGING_HARD',
        'deliverypath': '<ROOTDIR>/DELIVERY_DESTINATION',
        'operator': 'operator@domain.com',
        'logpath': '<ROOTDIR>/ANALYSIS/logs',
        'reportpath': '<ANALYSISPATH>',
        'copy_reports_to_reports_outbox': 'True',
        'reports_outbox': '/test/this/path',
        'deliverystatuspath': '<ANALYSISPATH>',
        'report_aggregate': 'ngi_reports ign_aggregate_report -n uppsala',
        'report_sample': 'ngi_reports ign_sample_report -n uppsala',
        'hash_algorithm': 'md5',
        'save_meta_info': 'True',
        'files_to_deliver': [
            ['<ANALYSISPATH>/level0_folder?_file*',
             '<STAGINGPATH>']]
        },
    'snic': {
        'snic_api_url': 'url', 
        'snic_api_user': 'usr', 
        'snic_api_password': 'pwd'
        },
    'statusdb': {
        'url': 'sdb_url',
        'username': 'sdb_usr',
        'password': 'sdb_pwd',
        'port': 'sdb_port'
        }
    }

class TestMisc(unittest.TestCase):
    
    @patch('taca_ngi_pipeline.deliver.deliver_grus.raw_input')
    def test_proceed_or_not(self, mock_input):
        mock_input.return_value = 'y'
        self.assertTrue(proceed_or_not('Q'))
        
        mock_input.return_value = 'no'
        self.assertFalse(proceed_or_not('Q'))
    
    @patch('taca_ngi_pipeline.deliver.deliver_grus.subprocess.check_output')
    def test_check_mover_version(self, mock_output):
        # No pattern match
        mock_output.return_value = 'no match'
        self.assertFalse(check_mover_version())
        
        # Match but wrong version
        mock_output.return_value = '/usr/local/mover/1.0.0/moverinfo version 0.9.0 calling Getopt::Std::getopts (version 1.07),'
        self.assertFalse(check_mover_version())
        
        # Match to right version
        mock_output.return_value = '/usr/local/mover/1.0.0/moverinfo version 1.0.0 calling Getopt::Std::getopts (version 1.07),'
        self.assertTrue(check_mover_version())
    
    
class TestGrusProjectDeliverer(unittest.TestCase):
    
    @classmethod
    @patch.dict('taca_ngi_pipeline.deliver.deliver_grus.CONFIG', SAMPLECFG)
    def setUpClass(self):
        db_entry = {'name': 'S.One_20_01',
                    'uppnex_id': 'a2099999',
                    'delivery_token': 'atoken'}
        with patch('taca_ngi_pipeline.utils.database.project_entry', 
                   return_value=db_entry) as dbmock:
            self.tmp_dir = tempfile.mkdtemp()
            self.pid = 'P12345'
            self.deliverer = GrusProjectDeliverer(projectid=self.pid,
                                                  fcid='FC1',
                                                  **SAMPLECFG['deliver'])
            self.deliverer.pi_email = 'pi@email.com'
            self.deliverer.rootdir = self.tmp_dir

    @classmethod
    def tearDownClass(self):
        shutil.rmtree(self.tmp_dir)
    
    def test_get_delivery_status(self):
        dbentry_in_progress = {'delivery_token': 'token'}
        got_status_in_progress = self.deliverer.get_delivery_status(dbentry=dbentry_in_progress)
        self.assertEqual(got_status_in_progress, 'IN_PROGRESS')
        
        dbentry_delivered = {'delivery_token': 'NO-TOKEN',
                             'delivery_status': 'DELIVERED'}
        got_status_delivered = self.deliverer.get_delivery_status(dbentry=dbentry_delivered)
        self.assertEqual(got_status_delivered, 'DELIVERED')
        
        dbentry_partial = {'delivery_projects': 'delivery0123'}
        got_status_partial = self.deliverer.get_delivery_status(dbentry=dbentry_partial)
        self.assertEqual(got_status_partial, 'PARTIAL')
        
        dbentry_not_delivered = {'delivery_token': 'not_under_delivery'}
        got_status_not_delivered = self.deliverer.get_delivery_status(dbentry=dbentry_not_delivered)
        self.assertEqual(got_status_not_delivered, 'NOT_DELIVERED')

    @patch('taca_ngi_pipeline.deliver.deliver_grus.check_mover_version')
    @patch('taca_ngi_pipeline.deliver.deliver_grus.GrusProjectDeliverer.get_delivery_status')
    @patch('taca_ngi_pipeline.deliver.deliver_grus.subprocess.check_output')
    @patch('taca_ngi_pipeline.deliver.deliver_grus.time.sleep')
    @patch('taca_ngi_pipeline.deliver.deliver_grus.GrusProjectDeliverer.get_samples_from_charon')
    @patch('taca_ngi_pipeline.deliver.deliver_grus.GrusSampleDeliverer')
    @patch('taca_ngi_pipeline.deliver.deliver_grus.GrusProjectDeliverer.delete_delivery_token_in_charon')
    @patch('taca_ngi_pipeline.deliver.deliver_grus.GrusProjectDeliverer.update_delivery_status')
    def test_check_mover_delivery_status(self,
                                         mock_update_delivery,
                                         mock_update_charon,
                                         mock_sample_deliverer, 
                                         mock_samples, 
                                         mock_sleep, 
                                         mock_check_output, 
                                         mock_status, 
                                         mock_version):
        mock_status.return_value = 'IN_PROGRESS'
        mock_check_output.side_effect = ['Accepted:', 'Delivered:']
        mock_samples.return_value = ['P12345_1001']
        mock_sample_deliverer().get_delivery_status.return_value = 'DELIVERED'

        db_entry = {'name': 'S.One_20_01',
                    'uppnex_id': 'a2099999',
                    'delivery_token': 'atoken'}
        with patch('taca_ngi_pipeline.utils.database.project_entry', 
                   return_value=db_entry) as dbmock:
            self.deliverer.check_mover_delivery_status()
            mock_update_delivery.assert_called_once_with(status='DELIVERED')

    @patch('taca_ngi_pipeline.deliver.deliver_grus.check_mover_version')
    @patch('taca_ngi_pipeline.deliver.deliver_grus.GrusProjectDeliverer.get_delivery_status')
    @patch('taca_ngi_pipeline.deliver.deliver_grus.proceed_or_not')
    @patch('taca_ngi_pipeline.deliver.deliver_grus.GrusProjectDeliverer.get_samples_from_charon')
    @patch('taca_ngi_pipeline.deliver.deliver_grus.GrusSampleDeliverer')
    @patch('taca_ngi_pipeline.deliver.deliver_grus.GrusProjectDeliverer._create_delivery_project')
    @patch('taca_ngi_pipeline.deliver.deliver_grus.GrusProjectDeliverer.do_delivery')
    @patch('taca_ngi_pipeline.deliver.deliver_grus.GrusProjectDeliverer.save_delivery_token_in_charon')
    @patch('taca_ngi_pipeline.deliver.deliver_grus.GrusProjectDeliverer.add_supr_name_delivery_in_charon')
    @patch('taca_ngi_pipeline.deliver.deliver_grus.GrusProjectDeliverer.add_supr_name_delivery_in_statusdb')
    def test_deliver_project(self,
                             mock_statusdb_name,
                             mock_charon_name,
                             mock_charon_token,
                             mock_deliver,
                             mock_create_project,
                             mock_sample_deliverer,
                             mock_samples,
                             mock_query,
                             mock_status,
                             mock_check_mover):
        mock_status.return_value = 'NOT_DELIVERED'
        mock_query.return_value = True
        mock_samples.return_value = ['S1']
        mock_create_project.return_value = {'name': 'delivery123'}
        mock_deliver.return_value = 'token123'
                
        os.makedirs(os.path.join(self.tmp_dir, 'STAGING'))
        open(os.path.join(self.tmp_dir, 'STAGING', 'misc_file.txt'), 'w').close()
        
        delivered = self.deliverer.deliver_project()
        self.assertTrue(delivered)

    @patch('taca_ngi_pipeline.deliver.deliver_grus.proceed_or_not')
    @patch('taca_ngi_pipeline.deliver.deliver_grus.shutil')
    @patch('taca_ngi_pipeline.deliver.deliver_grus.GrusProjectDeliverer._create_delivery_project')
    @patch('taca_ngi_pipeline.deliver.deliver_grus.GrusProjectDeliverer.do_delivery')
    def test_deliver_run_folder(self, mock_deliver, mock_create_project, mock_shutil, mock_query):
        mock_query.return_value = True
        mock_create_project.return_value = {'name': 'delivery123'}
        mock_deliver.return_value = 'token123'
        got_status = self.deliverer.deliver_run_folder()
        self.assertTrue(got_status)
        mock_deliver.assert_called_once_with('delivery123')

    @patch('taca_ngi_pipeline.deliver.deliver_grus.CharonSession')
    def test_add_supr_name_delivery_in_charon(self, mock_charon):
        mock_charon().project_get.return_value = {'delivery_projects': ['delivery123']}
        self.deliverer.add_supr_name_delivery_in_charon('delivery456')
        mock_charon().project_update.assert_called_once_with(self.pid, 
                                                             delivery_projects=['delivery123', 
                                                                                'delivery456'])

    @patch('taca_ngi_pipeline.deliver.deliver_grus.ProjectSummaryConnection')
    def test_add_supr_name_delivery_in_statusdb(self, mock_project_summary):
        mock_project_summary().get_entry.return_value = {'delivery_projects': ['delivery123']}
        self.deliverer.add_supr_name_delivery_in_statusdb('delivery456')
        mock_project_summary().save_db_doc.assert_called_once_with(
            {'delivery_projects': 
                ['delivery123', 'delivery456']
                }
            )
    
    @patch('taca_ngi_pipeline.deliver.deliver_grus.GrusProjectDeliverer.expand_path')
    @patch('taca_ngi_pipeline.deliver.deliver_grus.os.chown')
    @patch('taca_ngi_pipeline.deliver.deliver_grus.subprocess')
    def test_do_delivery(self, mock_subprocess, mock_chown, mock_path):
        mock_path.return_value = self.tmp_dir
        mock_subprocess.check_output.return_value = 'deliverytoken'
        got_token = self.deliverer.do_delivery('supr_delivery')
        self.assertEqual(got_token, 'deliverytoken')
    
    @patch('taca_ngi_pipeline.deliver.deliver_grus.CharonSession')
    def test_get_samples_from_charon(self, mock_charon):
        mock_charon().project_get_samples.return_value = {
            'samples': 
                [{'sampleid': 'S1',
                  'delivery_status': 'STAGED'}, 
                 {'sampleid': 'S2',
                  'delivery_status': 'DELIVERED'}]
                }
        got_samples = self.deliverer.get_samples_from_charon(delivery_status='STAGED')
        expected_samples = ['S1']
        self.assertEqual(got_samples, expected_samples)
    
    @patch('taca_ngi_pipeline.deliver.deliver_grus.requests')
    @patch('taca_ngi_pipeline.deliver.deliver_grus.datetime')
    @patch('taca_ngi_pipeline.deliver.deliver_grus.json.loads')
    def test__create_delivery_project(self, mock_json_load, mock_datetime, mock_requests):
        self.deliverer.pi_snic_id = '123'
        self.deliverer.other_member_snic_ids = []
        supr_date_format = '%Y-%m-%d'
        today = datetime.date.today()
        three_months_from_now = (today + relativedelta(months=+3))
        mock_datetime.date.today.return_value = today
        data = {
            'ngi_project_name': 'P12345',
            'title': "DELIVERY_P12345_{}".format(today.strftime(supr_date_format)),
            'pi_id': '123',
            'start_date': today.strftime(supr_date_format),
            'end_date': three_months_from_now.strftime(supr_date_format),
            'continuation_name': '',
            'api_opaque_data': '',
            'ngi_ready': False,
            'ngi_delivery_status': '',
            'ngi_sensitive_data': True,
            'member_ids': []
            }
        mock_requests.post().status_code = 200
        got_result = self.deliverer._create_delivery_project()
        calls = [call(), 
                 call('url/ngi_delivery/project/create/',
                      data=json.dumps(data),
                      auth=('usr', 'pwd'))]
        mock_requests.post.assert_has_calls(calls)

    @patch('taca_ngi_pipeline.deliver.deliver_grus.GrusProjectDeliverer._get_order_detail')
    @patch('taca_ngi_pipeline.deliver.deliver_grus.GrusProjectDeliverer._get_user_snic_id')    
    def test__set_pi_details(self, mock_id, mock_detail):
        mock_detail.return_value = {'fields': {'project_pi_email': 'pi@email.com'}}
        mock_id.return_value = '123'
        self.deliverer._set_pi_details()
        self.assertEqual(self.deliverer.pi_email, 'pi@email.com')
        self.assertEqual(self.deliverer.pi_snic_id, '123')

    @patch('taca_ngi_pipeline.deliver.deliver_grus.GrusProjectDeliverer._get_order_detail')
    @patch('taca_ngi_pipeline.deliver.deliver_grus.GrusProjectDeliverer._get_user_snic_id')
    def test__set_other_member_details(self, mock_snic_id, mock_get_details):
        mock_get_details.return_value = {'owner': {'email': 'owner@mail.com'},
                                         'fields': {'project_bx_email': 'bx@mail.com'}}
        mock_snic_id.side_effect = ['id1', 'id2', 'id3']
        emails = ['some@email.com']
        self.deliverer._set_other_member_details(other_member_emails=emails, include_owner=True)
        got_details = self.deliverer.other_member_snic_ids
        expected_details = ['id1', 'id2', 'id3']
        self.assertEqual(got_details, expected_details)

    @patch('taca_ngi_pipeline.deliver.deliver_grus.requests.get')
    @patch('taca_ngi_pipeline.deliver.deliver_grus.json.loads')
    def test__get_user_snic_id(self, mock_json, mock_requests):
        mock_requests().status_code = 200
        mock_json.return_value = {'matches': [{'id': '123'}]}
        got_user_id = self.deliverer._get_user_snic_id('some@email.com')
        self.assertEqual(got_user_id, '123')

    @patch('taca_ngi_pipeline.deliver.deliver_grus.StatusdbSession')
    def test__get_order_detail(self, mock_statusdb):
        with self.assertRaises(AssertionError):
            got_details = self.deliverer._get_order_detail()
    

class TestGrusSampleDeliverer(unittest.TestCase):

    def test_deliver_sample(self):
        pass

    def test_save_delivery_token_in_charon(self):
        pass

    def test_add_supr_name_delivery_in_charon(self):
        pass

    def test_do_delivery(self):
        pass