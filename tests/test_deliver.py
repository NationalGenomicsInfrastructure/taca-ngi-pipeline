""" Unit tests for the deliver commands """

import __builtin__
import json
# noinspection PyPackageRequirements
import mock
import os
import shutil
import signal
import taca_ngi_pipeline.utils.filesystem
import tempfile
import unittest

from ngi_pipeline.database import classes as db
from taca_ngi_pipeline.deliver import deliver
from taca.utils.filesystem import create_folder
from taca.utils.misc import hashfile
from taca.utils.transfer import SymlinkError, SymlinkAgent

SAMPLECFG = {
    'deliver': {
        'analysispath': '<ROOTDIR>/ANALYSIS',
        'datapath': '<ROOTDIR>/DATA',
        'stagingpath': '<ROOTDIR>/STAGING',
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
             '<STAGINGPATH>'],
            ['<ANALYSISPATH>/level1_folder2',
             '<STAGINGPATH>'],
            ['<ANALYSISPATH>/*folder0/*/*_file?',
             '<STAGINGPATH>'],
            ['<ANALYSISPATH>/*/<SAMPLEID>_folder?_file0',
             '<STAGINGPATH>'],
            ['<ANALYSISPATH>/*/*/this-file-does-not-exist',
             '<STAGINGPATH>'],
            ['<ANALYSISPATH>/level0_folder0_file0',
             '<STAGINGPATH>'],
            ['<DATAPATH>/level1_folder1/level2_folder1/level3_folder1',
             '<STAGINGPATH>'],
            ['<DATAPATH>/level1_folder1/level1_folder1_file1.md5',
             '<STAGINGPATH>'],
            ['<DATAPATH>/level1_folder1/level2_folder1/this_aggregate_report.csv',
             '<STAGINGPATH>', {'no_digest_cache': True, 'required': True}],
            ['<DATAPATH>/level1_folder1/level2_folder1/version_report.txt',
             '<STAGINGPATH>'],
        ]}}

SAMPLEENTRY = json.loads(
    '{"delivery_status": "this-is-the-sample-delivery-status", '
    '"analysis_status": "this-is-the-sample-analysis-status", '
    '"sampleid": "NGIU-S001"}')
PROJECTENTRY = json.loads(
    '{"delivery_status": "this-is-the-project-delivery-status", '
    '"analysis_status": "this-is-the-project-analysis-status", '
    '"projectid": "NGIU-P001", '
    '"uppnex_id": "a2099999"}')
PROJECTENTRY['samples'] = [SAMPLEENTRY]


class TestDeliverer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.nfolders = 3
        cls.nfiles = 3
        cls.nlevels = 3
        cls.rootdir = tempfile.mkdtemp(prefix="test_taca_deliver_")

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.rootdir, ignore_errors=True)

    def setUp(self):
        with mock.patch.object(deliver.db, 'dbcon', autospec=db.CharonSession) as dbmock:
            self.casedir = tempfile.mkdtemp(prefix="case_", dir=self.rootdir)
            self.projectid = 'NGIU-P001'
            self.sampleid = 'NGIU-S001'
            self.dbmock = dbmock
            self.deliverer = deliver.Deliverer(
                self.projectid,
                self.sampleid,
                rootdir=self.casedir,
                **SAMPLECFG['deliver'])
            self.create_content(
                self.deliverer.expand_path(self.deliverer.analysispath))
            self.create_content(
                self.deliverer.expand_path(self.deliverer.datapath))

    def tearDown(self):
        shutil.rmtree(self.casedir, ignore_errors=True)

    def create_content(self, parentdir, level=0, folder=0):
        if not os.path.exists(parentdir):
            os.mkdir(parentdir)
        for nf in xrange(self.nfiles):
            open(
                os.path.join(
                    parentdir,
                    "level{}_folder{}_file{}".format(level, folder, nf)),
                'w').close()
        if level == self.nlevels:
            return
        level += 1
        for nd in xrange(self.nfolders):
            self.create_content(
                os.path.join(
                    parentdir,
                    "level{}_folder{}".format(level, nd)),
                level,
                nd)

    def test_fetch_db_entry(self):
        """ db_entry in parent class must not be called directly """
        with self.assertRaises(NotImplementedError):
            self.deliverer.db_entry()

    def test_update_sample_delivery(self):
        """ update_delivery_status in parent class must not be called directly
        """
        with self.assertRaises(NotImplementedError):
            self.deliverer.update_delivery_status()

    @mock.patch.object(
        deliver.db.db.CharonSession,
        'project_create',
        return_value="mocked return value")
    def test_wrap_database_query(self, dbmock):
        self.assertEqual(
            deliver.db._wrap_database_query(
                deliver.db.dbcon().project_create,
                "funarg1",
                funarg2="funarg2"),
            "mocked return value")
        dbmock.assert_called_with(
            "funarg1",
            funarg2="funarg2")
        dbmock.side_effect = db.CharonError("mocked error")
        with self.assertRaises(deliver.db.DatabaseError):
            deliver.db._wrap_database_query(
                deliver.db.dbcon().project_create,
                "funarg1",
                funarg2="funarg2")

    def test_gather_files1(self):
        """ Gather files in the top directory """
        expected = [
            os.path.join(
                self.deliverer.expand_path(self.deliverer.analysispath),
                "level0_folder0_file{}".format(n))
            for n in xrange(self.nfiles)]
        pattern = SAMPLECFG['deliver']['files_to_deliver'][0]
        self.deliverer.files_to_deliver = [pattern]
        self.assertItemsEqual(
            [p for p, _, _ in self.deliverer.gather_files()],
            expected)

    def test_gather_files2(self):
        """ Gather a folder in the top directory """
        expected = [os.path.join(d, f) for d, _, files in os.walk(
            os.path.join(
                self.deliverer.expand_path(self.deliverer.analysispath),
                "level1_folder2")) for f in files]
        pattern = SAMPLECFG['deliver']['files_to_deliver'][1]
        self.deliverer.files_to_deliver = [pattern]
        self.assertItemsEqual(
            [p for p, _, _ in self.deliverer.gather_files()],
            expected)

    def test_gather_files3(self):
        """ Gather the files two levels down """
        expected = ["level2_folder{}_file{}".format(m, n)
                    for m in xrange(self.nfolders)
                    for n in xrange(self.nfiles)]
        pattern = SAMPLECFG['deliver']['files_to_deliver'][2]
        self.deliverer.files_to_deliver = [pattern]
        self.assertItemsEqual(
            [os.path.basename(p) for p, _, _ in self.deliverer.gather_files()],
            expected)

    def test_gather_files4(self):
        """ Replace the SAMPLE keyword in pattern """
        expected = ["level1_folder{}_file0".format(n)
                    for n in xrange(self.nfolders)]
        pattern = SAMPLECFG['deliver']['files_to_deliver'][3]
        self.deliverer.files_to_deliver = [pattern]
        self.deliverer.sampleid = "level1"
        self.assertItemsEqual(
            [os.path.basename(p) for p, _, _ in self.deliverer.gather_files()],
            expected)

    def test_gather_files5(self):
        """ Do not pick up non-existing file """
        expected = []
        pattern = SAMPLECFG['deliver']['files_to_deliver'][4]
        self.deliverer.files_to_deliver = [pattern]
        self.assertItemsEqual(
            [os.path.basename(p) for p, _, _ in self.deliverer.gather_files()],
            expected)

    def test_gather_files6(self):
        """ Checksum should be cached in checksum file """
        pattern = SAMPLECFG['deliver']['files_to_deliver'][5]
        self.deliverer.files_to_deliver = [pattern]
        # create a checksum file and assert that it was used as a cache
        checksumfile = "{}.{}".format(
            self.deliverer.expand_path(pattern[0]),
            self.deliverer.hash_algorithm)
        exp_checksum = "this checksum should be cached"
        with open(checksumfile, 'w') as fh:
            fh.write(exp_checksum)
        for _, _, obs_checksum in self.deliverer.gather_files():
            self.assertEqual(
                obs_checksum,
                exp_checksum,
                "checksum '{}' from cache file was not picked up: '{}'".format(
                    obs_checksum, exp_checksum))
        os.unlink(checksumfile)
        # assert that the checksum file is created as expected
        for spath, _, exp_checksum in self.deliverer.gather_files():
            checksumfile = "{}.{}".format(spath, self.deliverer.hash_algorithm)
            self.assertTrue(os.path.exists(checksumfile),
                            "checksum cache file was not created")
            with open(checksumfile, 'r') as fh:
                obs_checksum = fh.next()
            self.assertEqual(obs_checksum, exp_checksum,
                             "cached and returned checksums did not match")
            os.unlink(checksumfile)
        exp_checksum = "mocked-digest"
        with mock.patch.object(
                taca_ngi_pipeline.utils.filesystem, 'hashfile', return_value=exp_checksum):
            # ensure that a thrown IOError when writing checksum cache file is handled gracefully
            # mock hashfile's call to open builtin
            with mock.patch.object(__builtin__, 'open', side_effect=IOError("mocked IOError")) as iomock:
                for spath, _, obs_checksum in self.deliverer.gather_files():
                    checksumfile = "{}.{}".format(
                        spath, self.deliverer.hash_algorithm)
                    self.assertFalse(os.path.exists(checksumfile),
                                     "checksum cache file should not have been created")
                    self.assertTrue(iomock.call_count == 2, "open should have been "
                                                            "called twice on checksum cache file")
                    self.assertEqual(
                        exp_checksum,
                        obs_checksum, "observed checksum doesn't match expected")
            # ensure that a digest that shouldn't be cached are not written to file
            tpat = list(pattern)
            tpat.append({'no_digest_cache': True})
            self.deliverer.files_to_deliver = [tpat]
            for spath, _, obs_checksum in self.deliverer.gather_files():
                checksumfile = "{}.{}".format(spath, self.deliverer.hash_algorithm)
                self.assertFalse(os.path.exists(checksumfile), "checksum cache file should not have been created")
                self.assertEqual(exp_checksum, obs_checksum, "observed cheksum doesn't match expected")
            # ensure that no digest is computed for a file labeled with no_digest
            tpat = list(pattern)
            tpat.append({'no_digest': True})
            self.deliverer.files_to_deliver = [tpat]
            self.assertTrue(all(map(lambda d: d[2] is None, self.deliverer.gather_files())),
                            "the digest for files with no_digest=True was computed")

    def test_gather_files7(self):
        """ Traverse folders also if they are symlinks """
        dest_path = self.deliverer.expand_path(
            os.path.join(
                self.deliverer.datapath,
                "level1_folder1",
                "level2_folder1",
                "level3_folder1"))
        sa = SymlinkAgent(
            src_path=self.deliverer.expand_path(
                os.path.join(
                    self.deliverer.analysispath,
                    "level1_folder0",
                    "level2_folder0",
                    "level3_folder0")),
            dest_path=os.path.join(dest_path, "level3_folder0"),
            relative=False)
        self.assertTrue(
            sa.transfer(),
            "failed when setting up test")

        expected = [os.path.join(dest_path, "level3_folder1_file{}".format(n))
                    for n in xrange(self.nfiles)]
        expected.extend([os.path.join(dest_path, "level3_folder0", "level3_folder0_file{}".format(n))
                         for n in xrange(self.nfiles)])
        pattern = SAMPLECFG['deliver']['files_to_deliver'][6]
        self.deliverer.files_to_deliver = [pattern]
        self.assertItemsEqual(
            [p for p, _, _ in self.deliverer.gather_files()],
            expected)

    def test_gather_files8(self):
        """ Skip checksum files """
        expected = []
        pattern = SAMPLECFG['deliver']['files_to_deliver'][7]
        self.deliverer.files_to_deliver = [pattern]
        open(self.deliverer.expand_path(pattern[0]), 'w').close()
        self.assertItemsEqual(
            [obs for obs in self.deliverer.gather_files()],
            expected)

    def test_gather_files9(self):
        """ Do not attempt to process broken symlinks """
        expected = []
        pattern = SAMPLECFG['deliver']['files_to_deliver'][5]
        spath = self.deliverer.expand_path(pattern[0])
        os.unlink(spath)
        os.symlink(
            os.path.join(
                os.path.dirname(spath),
                "this-file-does-not-exist"),
            spath)
        self.deliverer.files_to_deliver = [pattern]
        observed = [p for p, _, _ in self.deliverer.gather_files()]
        self.assertItemsEqual(observed, expected)

    def test_gather_files10(self):
        """ A missing required file should throw an error """
        pattern = list(SAMPLECFG['deliver']['files_to_deliver'][5])
        pattern.append({'required': True})
        spath = self.deliverer.expand_path(pattern[0])
        os.unlink(spath)
        os.symlink(
            os.path.join(
                os.path.dirname(spath),
                "this-file-does-not-exist"),
            spath)
        self.deliverer.files_to_deliver = [pattern]
        # assert broken symlink raises exception
        with self.assertRaises(deliver.fs.FileNotFoundException):
            list(self.deliverer.gather_files())
        # assert missing file raises exception
        os.unlink(spath)
        with self.assertRaises(deliver.fs.PatternNotMatchedException):
            list(self.deliverer.gather_files())
        # assert exception not raised if file is not required
        self.deliverer.files_to_deliver = [pattern[0:2]]
        self.assertListEqual([], list(self.deliverer.gather_files()), "empty result expected for missing file")

    def test_stage_delivery1(self):
        """ The correct folder structure should be created and exceptions 
            handled gracefully
        """
        gathered_files = (
            os.path.join(
                self.deliverer.expand_path(self.deliverer.analysispath),
                "level1_folder1",
                "level2_folder0",
                "level2_folder0_file1"),
            os.path.join(
                self.deliverer.expand_path(self.deliverer.stagingpath),
                "level1_folder1_link",
                "level2_folder0_link",
                "level2_folder0_file1_link"),
            "this-is-the-file-hash")
        with mock.patch.object(deliver, 'create_folder', return_value=False):
            with self.assertRaises(deliver.DelivererError):
                self.deliverer.stage_delivery()
        with mock.patch.object(
                deliver.Deliverer, 'gather_files', return_value=[gathered_files]):
            self.deliverer.stage_delivery()
            expected = os.path.join(
                self.deliverer.expand_path(self.deliverer.stagingpath),
                gathered_files[1])
            self.assertTrue(
                os.path.exists(expected),
                "Expected staged file does not exist")
            self.assertTrue(
                os.path.islink(expected),
                "Staged file is not a link")
            self.assertTrue(
                os.path.exists(self.deliverer.staging_digestfile()),
                "Digestfile does not exist in staging directory")
            with mock.patch.object(
                    deliver.transfer.SymlinkAgent,
                    'transfer',
                    side_effect=SymlinkError("mocked error")):
                self.assertTrue(self.deliverer.stage_delivery())

    def test_stage_delivery2(self):
        """ A single file should be staged correctly
        """
        pattern = SAMPLECFG['deliver']['files_to_deliver'][5]
        expected = os.path.join(
            self.deliverer.expand_path(self.deliverer.stagingpath),
            "level0_folder0_file0")
        self.deliverer.files_to_deliver = [pattern]
        self.deliverer.stage_delivery()
        self.assertTrue(os.path.exists(expected),
                        "The expected file was not staged")

    def test_stage_delivery3(self):
        """ Stage a folder and its subfolders in the top directory """
        expected = [
            os.path.join(
                self.deliverer.expand_path(self.deliverer.stagingpath),
                os.path.relpath(
                    os.path.join(d, f),
                    self.deliverer.expand_path(self.deliverer.analysispath)))
            for d, _, files in os.walk(
                os.path.join(
                    self.deliverer.expand_path(self.deliverer.analysispath),
                    "level1_folder2"))
            for f in files]
        pattern = SAMPLECFG['deliver']['files_to_deliver'][1]
        self.deliverer.files_to_deliver = [pattern]
        self.deliverer.stage_delivery()
        self.assertItemsEqual(
            [os.path.exists(e) for e in expected],
            [True for _ in xrange(len(expected))])

    def test_expand_path(self):
        """ Paths should expand correctly """
        cases = [
            ["this-path-should-not-be-touched",
             "this-path-should-not-be-touched",
             "a path without placeholders was modified"],
            ["this-path-<SHOULD>-be-touched",
             "this-path-was-to-be-touched",
             "a path with placeholders was not correctly modified"],
            ["this-path-<SHOULD>-be-touched-<MULTIPLE>",
             "this-path-was-to-be-touched-twice",
             "a path with multiple placeholders was not correctly modified"],
            ["this-path-should-<not>-be-touched",
             "this-path-should-<not>-be-touched",
             "a path without valid placeholders was modified"],
            [None, None, "a None path should be handled without exceptions"]]
        self.deliverer.should = 'was-to'
        self.deliverer.multiple = 'twice'
        for case, exp, msg in cases:
            self.assertEqual(self.deliverer.expand_path(case), exp, msg)
        with self.assertRaises(deliver.DelivererError):
            self.deliverer.expand_path("this-path-<WONT>-be-touched")

    def test_acknowledge_sample_delivery(self):
        """ A delivery acknowledgement should be written if requirements are met
        """
        # without the deliverystatuspath attribute, no acknowledgement should be written
        del self.deliverer.deliverystatuspath
        self.deliverer.acknowledge_delivery()
        ackfile = os.path.join(
            self.deliverer.expand_path(SAMPLECFG['deliver']['deliverystatuspath']),
            "{}_delivered.ack".format(self.sampleid))
        self.assertFalse(os.path.exists(ackfile),
                         "delivery acknowledgement was created but it shouldn't have been")
        # with the deliverystatuspath attribute, acknowledgement should be written with the supplied timestamp
        self.deliverer.deliverystatuspath = SAMPLECFG['deliver']['deliverystatuspath']
        for t in [deliver._timestamp(), "this-is-a-timestamp"]:
            self.deliverer.acknowledge_delivery(tstamp=t)
            self.assertTrue(os.path.exists(ackfile),
                            "delivery acknowledgement not created")
            with open(ackfile, 'r') as fh:
                self.assertEquals(t, fh.read().strip(),
                                  "delivery acknowledgement did not match expectation")
            os.unlink(ackfile)


class TestProjectDeliverer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rootdir = tempfile.mkdtemp(prefix="test_taca_deliver_")

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.rootdir, ignore_errors=True)

    def setUp(self):
        with mock.patch.object(deliver.db, 'dbcon', autospec=db.CharonSession):
            self.casedir = tempfile.mkdtemp(prefix="case_", dir=self.rootdir)
            self.projectid = 'NGIU-P001'
            self.deliverer = deliver.ProjectDeliverer(
                self.projectid,
                rootdir=self.casedir,
                **SAMPLECFG['deliver'])

    def tearDown(self):
        shutil.rmtree(self.casedir)

    def test_init(self):
        """ A ProjectDeliverer should initiate properly """
        self.assertIsInstance(
            getattr(self, 'deliverer'),
            deliver.ProjectDeliverer)

    @mock.patch.object(
        deliver.db.db.CharonSession,
        'project_update',
        return_value="mocked return value")
    def test_update_delivery_status(self, dbmock):
        """ Updating the delivery status for a project """
        self.assertEquals(
            self.deliverer.update_delivery_status(),
            "mocked return value")
        dbmock.assert_called_with(
            self.projectid,
            delivery_status="DELIVERED")

    def test_catching_sigint(self):
        """ SIGINT should raise DelivererInterruptedError
        """
        with self.assertRaises(deliver.DelivererInterruptedError):
            os.kill(os.getpid(), signal.SIGINT)

    def test_catching_sigterm(self):
        """ SIGTERM should raise DelivererInterruptedError
        """
        with self.assertRaises(deliver.DelivererInterruptedError):
            os.kill(os.getpid(), signal.SIGTERM)

    def test_acknowledge_project_delivery(self):
        """ A project delivery acknowledgement should be written to disk """
        self.deliverer.acknowledge_delivery()
        ackfile = os.path.join(
            self.deliverer.expand_path(SAMPLECFG['deliver']['deliverystatuspath']),
            "{}_delivered.ack".format(self.projectid))
        self.assertTrue(os.path.exists(ackfile),
                        "delivery acknowledgement not created")

    @mock.patch.object(
        deliver.db.db.CharonSession, 'project_get', return_value=PROJECTENTRY)
    def test_get_delivery_status(self, dbmock):
        """ retrieving delivery_status and analysis_status from db """
        self.assertEquals(
            self.deliverer.get_delivery_status(),
            PROJECTENTRY.get('delivery_status'))
        dbmock.assert_called_with(PROJECTENTRY['projectid'])
        self.assertEquals(
            self.deliverer.get_analysis_status(),
            PROJECTENTRY.get('analysis_status'))
        dbmock.assert_called_with(PROJECTENTRY['projectid'])

    def test_all_samples_delivered(self):
        """ retrieving all_samples_delivered status """
        with mock.patch.object(deliver.db.db.CharonSession, 'project_get_samples',
                               return_value=PROJECTENTRY) as dbmock:
            self.assertFalse(
                self.deliverer.all_samples_delivered(),
                "all samples should not be listed as delivered")
            dbmock.assert_called_with(PROJECTENTRY['projectid'])
        PROJECTENTRY['samples'][0]['delivery_status'] = 'DELIVERED'
        with mock.patch.object(deliver.db.db.CharonSession, 'project_get_samples',
                               return_value=PROJECTENTRY) as dbmock:
            self.assertTrue(
                self.deliverer.all_samples_delivered(),
                "all samples should not be listed as delivered")
            dbmock.assert_called_with(PROJECTENTRY['projectid'])
        PROJECTENTRY['samples'] = [SAMPLEENTRY]

    def test_create_project_report(self):
        """ creating the project report """
        with mock.patch.object(deliver,'call_external_command') as syscall:
            self.deliverer.create_report()
            self.assertEqual(
                " ".join(syscall.call_args[0][0]),
                SAMPLECFG['deliver']['report_aggregate'])

    def test_copy_project_report(self):
        """ Copy the project report to the specified report outbox"""
        with mock.patch.object(shutil, 'copyfile') as syscall:

            report_outbox = SAMPLECFG["deliver"]["reports_outbox"]

            aggregate_report_src = self.deliverer.expand_path("<DATAPATH>/level1_folder1/"
                                                              "level2_folder1/this_aggregate_report.csv")
            aggregate_report_target = os.path.join(report_outbox, "this_aggregate_report.csv")

            version_report_src = self.deliverer.expand_path("<DATAPATH>/level1_folder1/"
                                                            "level2_folder1/version_report.txt")
            version_report_target = os.path.join(report_outbox, "{}_version_report.txt".format(self.deliverer.projectid))

            expected = [aggregate_report_target, version_report_target]

            actual = self.deliverer.copy_report()

            syscall.assert_any_call(aggregate_report_src, aggregate_report_target)
            syscall.assert_any_call(version_report_src, version_report_target)

            self.assertListEqual(expected, actual)


class TestSampleDeliverer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rootdir = tempfile.mkdtemp(prefix="test_taca_deliver_")

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.rootdir, ignore_errors=True)

    def setUp(self):
        with mock.patch.object(deliver.db, 'dbcon', autospec=db.CharonSession):
            self.casedir = tempfile.mkdtemp(prefix="case_", dir=self.rootdir)
            self.projectid = 'NGIU-P001'
            self.sampleid = 'NGIU-S001'
            self.deliverer = deliver.SampleDeliverer(
                self.projectid,
                self.sampleid,
                rootdir=self.casedir,
                **SAMPLECFG['deliver'])

    def tearDown(self):
        shutil.rmtree(self.casedir, ignore_errors=True)

    def test_init(self):
        """ A SampleDeliverer should initiate properly """
        self.assertIsInstance(
            getattr(self, 'deliverer'),
            deliver.SampleDeliverer)

    @mock.patch.object(
        deliver.db.db.CharonSession, 'project_get', return_value=PROJECTENTRY)
    def test_fetch_uppnexid(self, dbmock):
        """ A SampleDeliverer should be able to fetch the Uppnex ID for the 
            project
        """
        # if an uppnexid is given in the configuration, it should be used and the database should not be queried
        deliverer = deliver.SampleDeliverer(
            self.projectid,
            self.sampleid,
            rootdir=self.casedir,
            uppnexid="this-is-the-uppnexid",
            **SAMPLECFG['deliver'])
        self.assertEquals(deliverer.uppnexid, "this-is-the-uppnexid")
        #called once due to projectname
        dbmock.assert_called_once_with(self.projectid)
        # if an uppnexid is not supplied in the config, the database should be consulted
        dbmock.reset_mock()
        prior = dbmock.call_count
        deliverer = deliver.SampleDeliverer(
            self.projectid,
            self.sampleid,
            rootdir=self.casedir,
            **SAMPLECFG['deliver'])
        self.assertEquals(deliverer.uppnexid, PROJECTENTRY['uppnex_id'])
        #two calls, one for projectname one for uppnexid
        self.assertEquals(dbmock.call_count, 2)

    @mock.patch.object(
        deliver.db.db.CharonSession,
        'sample_update',
        return_value="mocked return value")
    def test_update_delivery_status(self, dbmock):
        """ Updating the delivery status for a sample """
        self.assertEquals(
            self.deliverer.update_delivery_status(),
            "mocked return value")
        dbmock.assert_called_with(
            self.projectid,
            self.sampleid,
            delivery_status="DELIVERED")

    def test_catching_sigint(self):
        """ SIGINT should raise DelivererInterruptedError
        """
        with self.assertRaises(deliver.DelivererInterruptedError):
            os.kill(os.getpid(), signal.SIGINT)

    def test_catching_sigterm(self):
        """ SIGTERM should raise DelivererInterruptedError
        """
        with self.assertRaises(deliver.DelivererInterruptedError):
            os.kill(os.getpid(), signal.SIGTERM)

    def test_deliver_sample1(self):
        """ transfer a sample using rsync
        """
        # create some content to transfer
        digestfile = self.deliverer.staging_digestfile()
        filelist = self.deliverer.staging_filelist()
        basedir = os.path.dirname(digestfile)
        create_folder(basedir)
        expected = []
        with open(digestfile, 'w') as dh, open(filelist, 'w') as fh:
            curdir = basedir
            for d in xrange(4):
                if d > 0:
                    curdir = os.path.join(curdir, "folder{}".format(d))
                    create_folder(curdir)
                for n in xrange(5):
                    fpath = os.path.join(curdir, "file{}".format(n))
                    open(fpath, 'w').close()
                    rpath = os.path.relpath(fpath, basedir)
                    digest = hashfile(fpath, hasher=self.deliverer.hash_algorithm)
                    if n < 3:
                        expected.append(rpath)
                        fh.write("{}\n".format(rpath))
                        dh.write("{}  {}\n".format(digest, rpath))
            rpath = os.path.basename(digestfile)
            expected.append(rpath)
            fh.write("{}\n".format(rpath))
        # transfer the listed content
        destination = self.deliverer.expand_path(self.deliverer.deliverypath)
        create_folder(os.path.dirname(destination))
        self.assertTrue(self.deliverer.do_delivery(), "failed to deliver sample")
        # list the trasferred files relative to the destination
        observed = [os.path.relpath(os.path.join(d, f), destination)
                    for d, _, files in os.walk(destination) for f in files]
        self.assertItemsEqual(observed, expected)

    def test_acknowledge_sample_delivery(self):
        """ A sample delivery acknowledgement should be written to disk """
        ackfile = os.path.join(
            self.deliverer.expand_path(SAMPLECFG['deliver']['deliverystatuspath']),
            "{}_delivered.ack".format(self.sampleid))
        self.deliverer.acknowledge_delivery()
        self.assertTrue(os.path.exists(ackfile),
                        "delivery acknowledgement not created")

    @mock.patch.object(
        deliver.db.db.CharonSession, 'sample_get', return_value="mocked return value")
    def test_fetch_db_entry(self, dbmock):
        """ retrieving sample entry from db """
        self.assertEquals(
            self.deliverer.db_entry(),
            "mocked return value")
        dbmock.assert_called_with(self.projectid, self.sampleid)

    @mock.patch.object(
        deliver.db.db.CharonSession, 'sample_get', return_value=SAMPLEENTRY)
    def test_get_delivery_status(self, dbmock):
        """ retrieving delivery_status and analysis_status from db """
        self.assertEquals(
            self.deliverer.get_delivery_status(),
            SAMPLEENTRY.get('delivery_status'))
        dbmock.assert_called_with(self.projectid, SAMPLEENTRY.get('sampleid'))
        self.assertEquals(
            self.deliverer.get_analysis_status(),
            SAMPLEENTRY.get('analysis_status'))
        dbmock.assert_called_with(self.projectid, SAMPLEENTRY.get('sampleid'))

    def test_create_sample_report(self):
        """ creating the sample report """
        with mock.patch.object(deliver, 'call_external_command') as syscall:
            self.deliverer.create_report()
            self.assertEqual(
                " ".join(syscall.call_args_list[0][0][0]),
                "{} --samples {}".format(
                    SAMPLECFG['deliver']['report_sample'],
                    self.deliverer.sampleid))
            self.assertEqual(
                " ".join(syscall.call_args_list[1][0][0][:-1]),
                "{} --samples_extra".format(
                    SAMPLECFG['deliver']['report_aggregate']))
