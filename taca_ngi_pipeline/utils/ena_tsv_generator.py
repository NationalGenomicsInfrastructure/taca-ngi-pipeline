#!/usr/bin/env python

import argparse
from datetime import date
import yaml
from taca.utils.statusdb import (
    ProjectSummaryConnection,
    GenericFlowcellRunConnection,
)
import os
import re
import logging
import csv
from scilifelab_metadata_templates.genomics import validate_genomics_data

template = {
   "study_alias": "", #Optional
   "sample_alias": "", #Optional
   "instrument_model": "", # Sequencing platform from LIMS
   "library_name": "",  #provided by submitter
   "library_source": "", #from Project creation form
   "library_selection": "", #from Project creation form
   "library_strategy": "", #from Project creation form
   "library_layout": "", #["SINGLE","PAIRED"]
   "insert_size": "", # from Project creation form
   "library_construction_protocol": "", # from Project creation form
   "file_type": "", #["bam", "cram", "fastq",  "OxfordNanopore_native"]
   "file_name": "",#Pxxxx_101_S1_L001_R1_001.fastq.gz
   "file_md5": "",
   #"reverse_file_name": "",#Pxxxx_101_S1_L001_R2_001.fastq.gz, Only for paired end data, to be added later
   #"reverse_file_md5": "", Only for paired end data, to be added later
   "scilifelab_unit": "", #"National Genomics Infrastructure" same as citation
   "unit_internal_project_id": "", #Pxxxx
   "order_id": "", # Order Portal id
   "experimental_sample_id": "", #Pxxxxx_101
   "associated_sample_id": "", #User defined sample ID
   "metadata_file_creation_date": "", #Creation date of the metadata file
   "template_name": "genomics_template",
   "template_version": "0.0.1" # Version of the scilifelab_metadata_templates template used to generate the metadata file
}

class tsv_generator(object):
    """
    A class with class methods to generate run/experiment TSV files
    which user can submit to reads archive with the help of NBIS
    """

    def __init__(
        self,
        project,
        outdir=os.getcwd(),
        flowcells=None,
        LOG=None,
        db_conf=None,
    ):
        """Instantiate required objects"""
        self.LOG = LOG
        try:
            self.projdb_conn = ProjectSummaryConnection(db_conf)
            self.fc_con = GenericFlowcellRunConnection(db_conf)
            self._check_and_load_project(project)
            assert isinstance(self.project_doc, dict), (
                f"Could not get proper project document for {project} from StatusDB"
            )
            self.staged_files = self.project_doc.get("staged_files", {})
            assert self.staged_files, (
                f"No staged samples for project {project}, cannot generate TSV files"
            )
            self._check_and_load_flowcells(flowcells)
            assert isinstance(self.flowcells, dict), (
                f"Could not get the flowcells for project {self.project_doc['project_id']} from StatusDB"
            )
        except AssertionError as e:
            self.LOG.error(e)
            raise e
        self.samples = self.project_doc.get("samples", {})
        self.file_pairs_delivered = {}
        self.outdir = self._check_and_load_outdir(outdir)
        self._set_common_details()
        self._load_staged_files()

    def _check_and_load_project(self, project):
        """Get the project document from couchDB"""
        if isinstance(project, str):
            self.LOG.info(f"Fetching project '{project}' from statusDB")
            project_doc = self.projdb_conn.get_entry(
                project, use_id_view=True, db="projects"
            )
        self.project_doc = project_doc

    def _check_and_load_flowcells(self, flowcells):
        """Get the project's flowcells if not already given"""
        if not flowcells or not isinstance(flowcells, dict):
            self.LOG.info(
                f"Fetching flowcells sequenced for project '{self.project_doc['project_id']}' from StatusDB"
            )
            flowcells = {}
            # get flowcells for project
            for db in ["x_flowcells", "nanopore_runs", "element_runs"]:
                flowcells.update(
                    self.fc_con.get_project_flowcell(
                        self.project_doc["project_id"],
                        self.project_doc.get("open_date", "2015-01-01"),
                        dbname=db,
                    )
                )
        self.flowcells = flowcells

    def _check_and_load_outdir(self, outdir):
        """Check the given outdir and see if its valid one"""
        if not os.path.exists(outdir):
            self.LOG.info(
                f"Given outdir '{outdir}' does not exist so will create it"
            )
            os.makedirs(outdir)
        elif not os.path.isdir(outdir):
            self.LOG.warning(
                f"Given outdir '{outdir}' is not valid so will use current directory"
            )
            outdir = os.getcwd()
        return outdir

    def _set_common_details(self):
        """Get project library design and protocol details"""
        self.common_details = {}

        # User provided details
        self.common_details["study_alias"] = "Needs to be filled in by submitter"
        self.common_details["sample_alias"] = "Needs to be filled in by submitter"
        self.common_details["scilifelab_unit"] = "NGI Stockholm"

        self.common_details["unit_internal_project_id"] = self.project_doc.get(
            "project_id", ""
        )
        self.common_details["metadata_file_creation_date"] = str(date.today())
        self.common_details["template_name"] = "genomics_template"
        self.common_details["template_version"] = "0.0.1"

        proj_details = self.project_doc.get("details", {})
        # get library construction method and parse neccesary information
        self.common_details["order_id"] = proj_details.get("portal_id", "")
        self.common_details["insert_size"] = proj_details.get(
            "insert_size_aimed_for", ""
        )
        self.common_details["library_selection"] = proj_details.get(
            "library_selection", ""
        )
        self.common_details["library_source"] = proj_details.get("library_source", "")
        self.common_details["library_strategy"] = proj_details.get(
            "library_strategy", ""
        )
        self.common_details["instrument_model"] = proj_details.get(
            "sequencing_platform", ""
        )
        self.common_details["library_construction_protocol"] = proj_details.get(
            "library_construction_method", ""
        )
        self.common_details["library_construction_protocol"] = (
            self.common_details["library_construction_protocol"]
            + " - Please contact support@ngisweden.se"
        )
        seq_setup = proj_details.get("sequencing_setup", "")
        seq_setup_match = re.search(r"^(\d+)-(\d+)-(\d+)-(\d+)$", seq_setup)
        if seq_setup_match:
            read1, _, _, read2 = map(int, seq_setup_match.groups())
            if read1 > 0:
                if read2 > 0:
                    self.common_details["library_layout"] = "PAIRED"
                    # Add reverse file name and md5 keys to the template for paired end data
                    template["reverse_file_name"] = ""
                    template["reverse_file_md5"] = ""
                else:
                    self.common_details["library_layout"] = "SINGLE"
            else:
                self.LOG.error(
                    f"The sequencing setup from couchdb for project {self.project_doc['project_id']} was not in the expected format"
                )
                self.common_details["library_layout"] = "UNKNOWN"
        else:
            self.LOG.error(
                f"Was not able to fetch sequencing setup from couchdb for project {self.project_doc['project_id']}"
            )
            self.common_details["library_layout"] = "UNKNOWN"

    def _load_staged_files(self):
        """Load the staged files for the project and check if they are in expected format"""

        for sample, files in self.staged_files.items():
            for file_name, file_stats in files.items():
                if file_name.endswith("fastq.gz"):
                    fastq_name = (
                        file_name.split("/")[-1]
                        .replace(".fastq.gz", "")
                        .replace("_R1_", "_")
                        .replace("_R2_", "_")
                    )
                    if fastq_name not in self.file_pairs_delivered:
                        self.file_pairs_delivered[fastq_name] = {
                            "file_type": "fastq",
                            "file_name": "",
                            "file_md5": "",
                            "experimental_sample_id": sample,
                            "library_name": sample,
                            "associated_sample_id": self.samples[sample].get(
                                "customer_name", ""
                            ),
                        }
                        if self.common_details["library_layout"] == "PAIRED":
                            self.file_pairs_delivered[fastq_name][
                                "reverse_file_name"
                            ] = ""
                            self.file_pairs_delivered[fastq_name][
                                "reverse_file_md5"
                            ] = ""
                    if "_R1_" in file_name:
                        self.file_pairs_delivered[fastq_name]["file_name"] = file_name
                        self.file_pairs_delivered[fastq_name]["file_md5"] = (
                            file_stats.get("md5_sum", "")
                        )
                    elif "_R2_" in file_name:
                        self.file_pairs_delivered[fastq_name]["reverse_file_name"] = (
                            file_name
                        )
                        self.file_pairs_delivered[fastq_name]["reverse_file_md5"] = (
                            file_stats.get("md5_sum", "")
                        )

    def generate_tsv_file(self):
        """Generate TSV file from the string template"""
        tsv_file_path = os.path.join(
            self.outdir, f"{self.project_doc['project_id']}_submission.tsv"
        )
        with open(tsv_file_path, "w", newline="") as tsvfile:
            writer = csv.DictWriter(tsvfile, fieldnames=template.keys(), delimiter="\t")
            writer.writeheader()
            for file_pair in self.file_pairs_delivered.values():
                row = {**self.common_details, **file_pair}
                writer.writerow(row)
        self.LOG.info(f"Generated TSV file at {tsv_file_path}")
        return tsv_file_path

    def validate_tsv_file(self, tsv_file_path):
        """Validate the generated TSV file against the genomics schema"""
        validation_errors = validate_genomics_data(tsv_file_path)
        if validation_errors:
            self.LOG.error("Validation errors found in the generated TSV file:")
            for error in validation_errors:
                self.LOG.error(f"Row {error['row']}: {error['message']}")
        else:
            self.LOG.info("No validation errors found in the generated TSV file.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser("ena_tsv_generator.py")
    parser.add_argument(
        "project",
        type=str,
        metavar="<project id>",
        help="NGI project id for which TSV files are to be generated",
    )
    parser.add_argument(
        "--outdir",
        type=str,
        default=os.getcwd(),
        help="Output directory where the TSV files will be saved",
    )
    parser.add_argument(
        "--db_conf_path",
        type=str,
        default=os.getenv("STATUS_DB_CONFIG"),
        help="Path to the statusdb configuration file",
    )
    kwargs = vars(parser.parse_args())
    LOG = logging.getLogger("ena_tsv_generator")
    LOG.info(f"Generating TSV files for project {kwargs['project']}")
    with open(kwargs["db_conf_path"], "r") as db_cred_file:
        db_conf = yaml.safe_load(db_cred_file)["statusdb"]

    tsvgen = tsv_generator(
        kwargs["project"],
        LOG=LOG,
        outdir=kwargs["outdir"],
        db_conf=db_conf,
    )
    tsv_file_path = tsvgen.generate_tsv_file()
    tsvgen.validate_tsv_file(tsv_file_path)
    LOG.info(f"Generated TSV files for project {kwargs['project']}")
