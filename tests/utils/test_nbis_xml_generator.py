import unittest
import tempfile
import shutil
import os
import filecmp

from couchdb.client import Document
from mock import Mock, patch

from taca_ngi_pipeline.utils.nbis_xml_generator import xml_generator


class TestXmlGen(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.log = Mock()
        self.pid = 'P12345'
        
        self.pcon = Mock()
        couch_doc = {'staged_files': {'P12345_1001': {'P12345_1001_R1_a.fastq.gz': 'val1', 'P12345_1001_R2_a.fastq.gz': 'val2'}},
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
        
        self.stats = {
            'experiment': 
                {'selection': 'unspecified', 
                 'protocol': 'NA', 
                 'library': 'P12345_1001_prep', 
                 'design': 'Sample library for sequencing on Illumina MiSeq', 
                 'discriptor': 'P12345_1001', 
                 'layout': '<PAIRED></PAIRED>', 
                 'source': 'METAGENOMIC', 
                 'study': 'P12345', 
                 'title': 'Prep for P12345_1001 sequenced in Illumina MiSeq',
                 'strategy': 'OTHER', 
                 'alias': 'P12345_1001_A_illumina_miseq_experiment',
                 'instrument': 'Illumina MiSeq'
                 },
                'run': 
                    {'exp_ref': 'P12345_1001_A_illumina_miseq_experiment', 
                     'alias': 'P12345_1001_A_illumina_miseq_runs', 
                     'data_name': 'P12345_1001', 
                     'files': '\t\t\t\t<FILE filename="file1.fastq.gz" filetype="fastq" checksum_method="MD5" checksum="sum" />\n'
                     }
                    }
    
    @classmethod
    def tearDownClass(self):
        shutil.rmtree(self.outdir)
    
    @patch('taca_ngi_pipeline.utils.nbis_xml_generator.xml_generator._collect_sample_stats')
    @patch('taca_ngi_pipeline.utils.nbis_xml_generator.xml_generator._generate_manifest_file')
    def test_generate_xml_and_manifest(self, mock_generate, mock_collect):
        mock_collect.return_value = [self.stats]
        self.xgen.generate_xml_and_manifest()
        got_exml = os.path.join(self.outdir, 'P12345_experiments.xml')
        exp_exml = os.path.join('data', 'P12345_experiments.xml')
        got_rxml = os.path.join(self.outdir, 'P12345_runs.xml')
        exp_rxml = os.path.join('data', 'P12345_runs.xml')
 
        self.assertTrue(filecmp.cmp(got_exml, exp_exml))
        self.assertTrue(filecmp.cmp(got_rxml, exp_rxml))
    
    def test__generate_manifest_file(self):
        experiment_details = {'study': 'P12345',
                              'discriptor': 'P12345_1001',
                              'alias': 'alias',
                              'instrument': 'inst',
                              'source': 'src',
                              'selection': 'sel',
                              'strategy': 'strat',
                              'layout': '<PAIRED></PAIRED>'}
        self.xgen._generate_manifest_file(experiment_details, 'run_details')
        got_manifest_path = os.path.join(self.outdir, 'manifestFiles', 'P12345_1001_manifest.txt')
        self.assertTrue(filecmp.cmp(got_manifest_path,'data/P12345_1001_manifest.txt'))
    
    @patch('taca_ngi_pipeline.utils.nbis_xml_generator.xml_generator._generate_files_block')
    def test__collect_sample_stats(self, mock_blockgen):
        mock_blockgen.return_value = '\t\t\t\t<FILE filename="file1.fastq.gz" filetype="fastq" checksum_method="MD5" checksum="sum" />\n'
        got_stats = self.xgen._collect_sample_stats()  # Generator object
        expected_stats = self.stats
        for i in got_stats:
            self.assertEqual(i, expected_stats)
    
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
        files = {'file1.fastq.gz': {'md5_sum': 'sum'}, 'file2.txt': {}}
        got_file_block = self.xgen._generate_files_block(files)
        expected_file_block = '\t\t\t\t<FILE filename="file1.fastq.gz" filetype="fastq" checksum_method="MD5" checksum="sum" />\n'
        self.assertEqual(got_file_block, expected_file_block)
    
    def test__check_and_load_project(self):
        expected_project = {
            'staged_files': {'P12345_1001': {'P12345_1001_R1_a.fastq.gz': 'val1', 'P12345_1001_R2_a.fastq.gz': 'val2'}}, 
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