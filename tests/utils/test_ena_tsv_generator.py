import unittest
import tempfile
import shutil
import os

from unittest.mock import Mock, patch

from taca_ngi_pipeline.utils.ena_tsv_generator import tsv_generator

class TestTsvGenerator(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.log = Mock()
        cls.pid = "P12345"

        # Mock ProjectSummaryConnection
        cls.pcon = Mock()
        couch_doc = {
            "project_id": cls.pid,
            "staged_files": {
                "P12345_1001": {
                    "P12345_1001_R1_001.fastq.gz": {"md5": "abc123"},
                    "P12345_1001_R2_001.fastq.gz": {"md5": "def456"},
                }
            },
            "details": {
                "portal_id": "ORDER123",
                "insert_size_aimed_for": "350",
                "library_selection": "PCR",
                "library_source": "GENOMIC",
                "library_strategy": "WGS",
                "sequencing_platform": "Illumina NovaSeq 6000",
                "library_construction_method": "TruSeq DNA",
                "sequencing_setup": "150-8-8-150",
            },
            "samples": {
                "P12345_1001": {
                    "library_prep": {"A": {"sequenced_fc": "ABC123"}}
                }
            },
            "open_date": "2024-01-01",
        }
        cls.pcon.get_entry.return_value = couch_doc

        # Mock GenericFlowcellRunConnection
        cls.fcon = Mock()
        cls.fcon.get_project_flowcell.return_value = {
            "flowcell_1": {
                "run_name": "run_1",
                "RunInfo": {"Id": "run_id_1"},
            }
        }

        cls.outdir = tempfile.mkdtemp()
        cls.db_conf = None

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.outdir)

    @patch("taca_ngi_pipeline.utils.ena_tsv_generator.ProjectSummaryConnection")
    @patch("taca_ngi_pipeline.utils.ena_tsv_generator.GenericFlowcellRunConnection")
    def test_tsv_generator_init(self, mock_fc_conn, mock_proj_conn):
        """Test tsv_generator initialization"""
        mock_proj_conn.return_value.get_entry.return_value = {
            "project_id": self.pid,
            "staged_files": {
                "P12345_1001": {
                    "P12345_1001_R1_001.fastq.gz": {"md5": "abc123"},
                }
            },
            "details": {
                "sequencing_setup": "150-8-8-150",
                "sequencing_platform": "Illumina NovaSeqXPlus",
            },
            "samples": {"P12345_1001": {}},
        }
        mock_fc_conn.return_value.get_project_flowcell.return_value = {}

        gen = tsv_generator(self.pid, outdir=self.outdir, LOG=self.log, db_conf=self.db_conf)
        self.assertEqual(gen.project_doc["project_id"], self.pid)
        self.assertIsNotNone(gen.common_details)

    @patch("taca_ngi_pipeline.utils.ena_tsv_generator.ProjectSummaryConnection")
    @patch("taca_ngi_pipeline.utils.ena_tsv_generator.GenericFlowcellRunConnection")
    def test_check_and_load_outdir(self, mock_fc_conn, mock_proj_conn):
        """Test outdir validation and creation"""
        mock_proj_conn.return_value.get_entry.return_value = {
            "project_id": self.pid,
            "staged_files": {
                "P12345_1001": {
                    "P12345_1001_R1_001.fastq.gz": {"md5": "abc123"},
                }
            },
            "details": {"sequencing_setup": "150-8-8-150"},
            "samples": {"P12345_1001": {}},
        }
        mock_fc_conn.return_value.get_project_flowcell.return_value = {}

        test_outdir = tempfile.mkdtemp()
        try:
            gen = tsv_generator(self.pid, outdir=test_outdir, LOG=self.log, db_conf=self.db_conf)
            self.assertEqual(gen.outdir, test_outdir)
            self.assertTrue(os.path.isdir(gen.outdir))
        finally:
            shutil.rmtree(test_outdir)

    @patch("taca_ngi_pipeline.utils.ena_tsv_generator.ProjectSummaryConnection")
    @patch("taca_ngi_pipeline.utils.ena_tsv_generator.GenericFlowcellRunConnection")
    def test_set_common_details(self, mock_fc_conn, mock_proj_conn):
        """Test that common details are set correctly"""
        project_doc = {
            "project_id": self.pid,
            "staged_files": {
                "P12345_1001": {"P12345_1001_R1_001.fastq.gz": {"md5": "abc"}},
            },
            "details": {
                "portal_id": "ORDER123",
                "sequencing_platform": "Illumina MiSeq",
                "library_strategy": "WGS",
                "sequencing_setup": "150-8-8-150",
            },
            "samples": {"P12345_1001": {}},
        }
        mock_proj_conn.return_value.get_entry.return_value = project_doc
        mock_fc_conn.return_value.get_project_flowcell.return_value = {}

        gen = tsv_generator(self.pid, outdir=self.outdir, LOG=self.log, db_conf=self.db_conf)
        self.assertEqual(gen.common_details["unit_internal_project_id"], self.pid)
        self.assertEqual(gen.common_details["library_layout"], "PAIRED")
        self.assertEqual(gen.common_details["order_id"], "ORDER123")

    @patch("taca_ngi_pipeline.utils.ena_tsv_generator.ProjectSummaryConnection")
    @patch("taca_ngi_pipeline.utils.ena_tsv_generator.GenericFlowcellRunConnection")
    def test_load_staged_files(self, mock_fc_conn, mock_proj_conn):
        """Test that staged files are loaded correctly"""
        project_doc = {
            "project_id": self.pid,
            "staged_files": {
                "P12345_1001": {
                    "P12345_1001_R1_001.fastq.gz": {"md5": "abc123"},
                    "P12345_1001_R2_001.fastq.gz": {"md5": "def456"},
                }
            },
            "details": {"sequencing_setup": "150-8-8-150"},
            "samples": {"P12345_1001": {}},
        }
        mock_proj_conn.return_value.get_entry.return_value = project_doc
        mock_fc_conn.return_value.get_project_flowcell.return_value = {}

        gen = tsv_generator(self.pid, outdir=self.outdir, LOG=self.log, db_conf=self.db_conf)
        self.assertIn("P12345_1001_001", gen.file_pairs_delivered)

@patch("taca_ngi_pipeline.utils.ena_tsv_generator.GenericFlowcellRunConnection")
@patch("taca_ngi_pipeline.utils.ena_tsv_generator.ProjectSummaryConnection")
def test_generate_and_validate_tsv_file(self, mock_fc_conn, mock_proj_conn):
    """Test TSV file generation and validation"""
    project_doc = {
        "project_id": self.pid,
        "staged_files": {
            "P12345_1001": {
                "P12345_1001_R1_001.fastq.gz": {"md5_sum": "abc123"},
                "P12345_1001_R2_001.fastq.gz": {"md5_sum": "def456"},
            }
        },
        "details": {
            "portal_id": "ORDER123",
            "insert_size_aimed_for": "350",
            "library_selection": "PCR",
            "library_source": "GENOMIC",
            "library_strategy": "WGS",
            "sequencing_platform": "Illumina NovaSeq 6000",
            "library_construction_method": "TruSeq DNA",
            "sequencing_setup": "150-8-8-150",
        },
        "samples": {
            "P12345_1001": {"customer_name": "Sample_001"}
        },
        "open_date": "2024-01-01",
    }
    mock_proj_conn.return_value.get_entry.return_value = project_doc
    mock_fc_conn.return_value.get_project_flowcell.return_value = {}

    gen = tsv_generator(self.pid, outdir=self.outdir, LOG=self.log, db_conf=self.db_conf)
    tsv_file_path = gen.generate_tsv_file()

    # Verify TSV file was created
    self.assertTrue(os.path.exists(tsv_file_path))
    self.assertEqual(tsv_file_path, os.path.join(self.outdir, f"{self.pid}_submission.tsv"))

    # Verify TSV file content
    with open(tsv_file_path, "r") as f:
        lines = f.readlines()
    
    # Should have header + at least one data row
    self.assertGreaterEqual(len(lines), 2)
    
    # Verify header contains expected columns
    header = lines[0].strip().split("\t")
    self.assertIn("study_alias", header)
    self.assertIn("sample_alias", header)
    self.assertIn("instrument_model", header)
    self.assertIn("library_layout", header)
    self.assertIn("file_name", header)
    self.assertIn("file_md5", header)

    # Verify data row contains expected values
    data_row = lines[1].strip().split("\t")
    self.assertEqual(len(data_row), len(header))
    
    # Check specific field values
    study_idx = header.index("study_alias")
    instrument_idx = header.index("instrument_model")
    layout_idx = header.index("library_layout")
    
    self.assertEqual(data_row[instrument_idx], "Illumina NovaSeq 6000")
    self.assertEqual(data_row[layout_idx], "PAIRED")

    @patch("taca_ngi_pipeline.utils.ena_tsv_generator.validate_genomics_data")
    @patch("taca_ngi_pipeline.utils.ena_tsv_generator.ProjectSummaryConnection")
    @patch("taca_ngi_pipeline.utils.ena_tsv_generator.GenericFlowcellRunConnection")
    def test_validate_tsv_file(self, mock_fc_conn, mock_proj_conn, mock_validate):
        """Test TSV file validation against schema"""
        project_doc = {
            "project_id": self.pid,
            "staged_files": {
                "P12345_1001": {
                    "P12345_1001_R1_001.fastq.gz": {"md5_sum": "abc123"},
                    "P12345_1001_R2_001.fastq.gz": {"md5_sum": "def456"},
                }
            },
            "details": {
                "sequencing_setup": "150-8-8-150",
                "sequencing_platform": "Illumina MiSeq",
            },
            "samples": {"P12345_1001": {}},
            "open_date": "2024-01-01",
        }
        mock_proj_conn.return_value.get_entry.return_value = project_doc
        mock_fc_conn.return_value.get_project_flowcell.return_value = {}
        mock_validate.return_value = []  # No validation errors

        gen = tsv_generator(self.pid, outdir=self.outdir, LOG=self.log, db_conf=self.db_conf)
        tsv_file_path = gen.generate_tsv_file()
        gen.validate_tsv_file(tsv_file_path)

        # Verify validate_genomics_data was called with the correct file
        mock_validate.assert_called_once_with(tsv_file_path)

    @patch("taca_ngi_pipeline.utils.ena_tsv_generator.validate_genomics_data")
    @patch("taca_ngi_pipeline.utils.ena_tsv_generator.ProjectSummaryConnection")
    @patch("taca_ngi_pipeline.utils.ena_tsv_generator.GenericFlowcellRunConnection")
    def test_validate_tsv_file_with_errors(self, mock_fc_conn, mock_proj_conn, mock_validate):
        """Test TSV file validation with schema errors"""
        project_doc = {
            "project_id": self.pid,
            "staged_files": {
                "P12345_1001": {
                    "P12345_1001_R1_001.fastq.gz": {"md5_sum": "abc123"},
                }
            },
            "details": {
                "sequencing_setup": "150-8-8-150",
            },
            "samples": {"P12345_1001": {}},
            "open_date": "2024-01-01",
        }
        mock_proj_conn.return_value.get_entry.return_value = project_doc
        mock_fc_conn.return_value.get_project_flowcell.return_value = {}
        
        # Mock validation errors
        validation_errors = [
            {"row": 2, "message": "Missing required field: library_strategy"}
        ]
        mock_validate.return_value = validation_errors

        gen = tsv_generator(self.pid, outdir=self.outdir, LOG=self.log, db_conf=self.db_conf)
        tsv_file_path = gen.generate_tsv_file()
        gen.validate_tsv_file(tsv_file_path)

        # Verify the error was logged
        self.log.error.assert_called()
        mock_validate.assert_called_once_with(tsv_file_path)

if __name__ == "__main__":
    unittest.main()