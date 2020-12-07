import unittest
import tempfile
import shutil
from couchdb.client import Document
from mock import Mock


from taca_ngi_pipeline.utils.nbis_xml_generator import xml_generator

class TestXmlGen(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.log = Mock()
        self.pid = 'P12345'
        
        self.pcon = Mock()
        couch_doc = {'staged_files': {'P12345_1001': 'b'},
                     'project_id': self.pid,
                     'details': {'application': 'metagenomics',
                                 'library_construction_method': 'Library, By user, -, -, -',
                                 'sequencing_setup': '2x250'},
                     'samples': {'P12345_1001':
                         {'library_prep': 
                             {'A': {'sequenced_fc': 'ABC'}
                              }
                             }
                         }
                     }
        self.pcon.get_entry.return_value = Document(couch_doc)
        
        self.fcon = Mock()
        self.fcon.get_project_flowcell.return_value = {'a': {'run_name': 'a_run',
                                                             'db': 'x_flowcells',
                                                             'RunInfo': {'Id': 'run_id_M0'}
                                                             }}
       
        self.xcon = Mock()
        self.xcon.get_project_flowcell.return_value = {'c': {'run_name': 'another_run',
                                                             'db': 'x_flowcells',
                                                             'RunInfo': {'Id': 'another_run_id_M0'},
                                                             }}
        self.xcon.get_entry.return_value = {'RunInfo': {'Id': 'run_id_M0'},
                                            'illumina': {'Demultiplex_Stats': {'Barcode_lane_statistics': [{'Sample': 'P12345_1001'}]}}}
        
        self.outdir = tempfile.mkdtemp()
        self.xgen = xml_generator(self.pid, outdir=self.outdir, LOG=self.log, pcon=self.pcon, fcon=self.fcon, xcon=self.xcon)
    
    @classmethod
    def tearDownClass(self):
        shutil.rmtree(self.outdir)
    
    def test_generate_xml_and_manifest(self):
        pass
    
    def test__generate_manifest_file(self):
        pass
    
    def test__collect_sample_stats(self):
        pass
    
    def test__stats_from_flowcells(self):
        expected_stats = {
            'P12345_1001': 
                {'A_illumina_miseq': 
                    {'runs': ['run_id_M0', 'run_id_M0'], 
                     'xml_text': 'Illumina MiSeq'}}}
        self.assertEqual(self.xgen.sample_aggregated_stat, expected_stats)
    
    def test__set_project_design(self):
        expected_design = {
            'selection': 'unspecified', 
            'protocol': 'NA', 
            'strategy': 'OTHER', 
            'source': 'METAGENOMIC', 
            'design': 'Sample library for sequencing on {instrument}', 
            'layout': '<PAIRED></PAIRED>'}
        self.assertEqual(self.xgen.project_design, expected_design)
    
    def test__generate_files_block(self):
        pass
    
    def test__check_and_load_project(self):
        expected_project = {
            'staged_files': {'P12345_1001': 'b'}, 
            'project_id': 'P12345', 
            'details': {
                'application': 'metagenomics', 
                'sequencing_setup': '2x250', 
                'library_construction_method': 'Library, By user, -, -, -'}, 
            'samples': 
                {'P12345_1001': 
                    {'library_prep': 
                        {'A': {'sequenced_fc': 'ABC'}}
                        }
                    }
                }
        self.assertEqual(self.xgen.project, expected_project)
    
    def test__check_and_load_flowcells(self):
        expected_flowcells = {
            'a': 
                {'RunInfo': 
                    {'Id': 'run_id_M0'}, 
                    'instrument': 'Illumina MiSeq', 
                    'db': 'x_flowcells', 
                    'samples': ['P12345_1001'], 
                    'run_name': 'a_run'}, 
            'c': 
                {'RunInfo': 
                    {'Id': 'another_run_id_M0'}, 
                    'instrument': 'Illumina MiSeq', 
                    'db': 'x_flowcells', 
                    'samples': ['P12345_1001'], 
                    'run_name': 'another_run'}
                }
        self.assertEqual(self.xgen.flowcells, expected_flowcells)
    
    def test__check_and_load_lib_preps(self):
        self.assertEqual(self.xgen.sample_prep_fc_map, {'P12345_1001': {'A': 'ABC'}})
    
    def test__check_and_load_outdir(self):
        self.assertEqual(self.outdir, self.xgen.outdir)