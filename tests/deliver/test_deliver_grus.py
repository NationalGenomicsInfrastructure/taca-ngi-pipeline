import unittest
import shutil
import tempfile
from mock import patch

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
                    'uppnex_id': 'a2099999'}
        with patch('taca_ngi_pipeline.utils.database.project_entry', 
                   return_value=db_entry) as dbmock:
            self.tmp_dir = tempfile.mkdtemp()
            self.pid = 'P12345'
            self.deliverer = GrusProjectDeliverer(projectid=self.pid,
                                                  **SAMPLECFG['deliver'])
    
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
    
    def test_check_mover_delivery_status(self):
        pass
    
    def test_deliver_project(self):
        pass
    
    def test_deliver_run_folder(self):
        pass
    
    def test_save_delivery_token_in_charon(self):
        pass
    
    def test_delete_delivery_token_in_charon(self):
        pass
    
    def test_add_supr_name_delivery_in_charon(self):
        pass
    
    def test_add_supr_name_delivery_in_statusdb(self):
        pass
    
    def test_do_delivery(self):
        pass
    
    def test_get_samples_from_charon(self):
        pass
    
    def test__create_delivery_project(self):
        pass
    
    def test__set_pi_details(self):
        pass
    
    def test__set_other_member_details(self):
        pass
    
    def test__get_user_snic_id(self):
        pass
    
    def test__get_order_detail(self):
        pass
    
    
class TestGrusSampleDeliverer(unittest.TestCase):
    
    def test_deliver_sample(self):
        pass
    
    def test_save_delivery_token_in_charon(self):
        pass
    
    def test_add_supr_name_delivery_in_charon(self):
        pass
    
    def test_do_delivery(self):
        pass