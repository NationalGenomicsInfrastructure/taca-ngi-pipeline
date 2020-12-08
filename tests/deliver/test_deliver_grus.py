import unittest
from mock import patch

from taca_ngi_pipeline.deliver.deliver_grus import GrusProjectDeliverer, GrusSampleDeliverer, proceed_or_not, check_mover_version

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
    
    def test_get_delivery_status(self):
        pass
    
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