""" Unit tests for the deliver commands """

import __builtin__
import mock
import os
import shutil
import signal
import tempfile
import unittest

from ngi_pipeline.database import classes as db
from taca_ngi_pipeline.deliver import deliver
from taca.utils.filesystem import create_folder
from taca.utils.misc import hashfile
from taca.utils.transfer import SymlinkError, SymlinkAgent

SAMPLECFG = {
    'deliver': {
        'analysispath': '_ROOTDIR_/ANALYSIS',
        'datapath': '_ROOTDIR_/DATA',
        'stagingpath': '_ROOTDIR_/STAGING',
        'deliverypath': '_ROOTDIR_/DELIVERY_DESTINATION',
        'operator': 'operator@domain.com',
        'hash_algorithm': 'md5',
        'files_to_deliver': [
            ['_ANALYSISPATH_/level0_folder?_file*',
            '_STAGINGPATH_'],
            ['_ANALYSISPATH_/level1_folder2',
            '_STAGINGPATH_'],
            ['_ANALYSISPATH_/*folder0/*/*_file?',
            '_STAGINGPATH_'],
            ['_ANALYSISPATH_/*/_SAMPLEID__folder?_file0',
            '_STAGINGPATH_'],
            ['_ANALYSISPATH_/*/*/this-file-does-not-exist',
            '_STAGINGPATH_'],
            ['_ANALYSISPATH_/level0_folder0_file0',
            '_STAGINGPATH_'],
            ['_DATAPATH_/level1_folder1/level2_folder1/level3_folder1',
            '_STAGINGPATH_'],
            ['_DATAPATH_/level1_folder1/level1_folder1_file1.md5',
            '_STAGINGPATH_'],
        ]}}

class TestDeliverer(unittest.TestCase):  
     
    @classmethod
    def setUpClass(self):
        self.nfolders = 3
        self.nfiles = 3
        self.nlevels = 3
        self.rootdir = tempfile.mkdtemp(prefix="test_taca_deliver_")
        
    @classmethod
    def tearDownClass(self):
        shutil.rmtree(self.rootdir)
    
    @mock.patch.object(deliver.Deliverer,'dbcon',autospec=db.CharonSession)
    def setUp(self,dbmock):
        self.casedir = tempfile.mkdtemp(prefix="case_",dir=self.rootdir)
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
        try:
            shutil.rmtree(self.casedir)
        except:
            pass
            
    def create_content(self,parentdir,level=0,folder=0):
        if not os.path.exists(parentdir):
            os.mkdir(parentdir)
        for nf in xrange(self.nfiles):
            open(
                os.path.join(
                    parentdir,
                    "level{}_folder{}_file{}".format(level,folder,nf)),
                'w').close()
        if level == self.nlevels:
            return
        level = level+1
        for nd in xrange(self.nfolders):
            self.create_content(
                os.path.join(
                    parentdir,
                    "level{}_folder{}".format(level,nd)),
                level,
                nd)
    
    @mock.patch.object(
        deliver.db.CharonSession,'sample_get',return_value="mocked return value")
    def test_fetch_sample_db_entry(self,dbmock):
        """ retrieving sample entry from db and caching result """
        self.assertEquals(
            self.deliverer.sample_entry(),
            "mocked return value")
        dbmock.assert_called_with(self.projectid,self.sampleid)
        dbmock.reset_mock()
        self.assertEquals(
            self.deliverer.sample_entry(),
            "mocked return value")
        self.assertFalse(
            dbmock.called,
            "sample_get method should not have been called for cached result")
    
    def test_update_sample_delivery(self):
        with self.assertRaises(NotImplementedError):
            self.deliverer.update_delivery_status()
        
    @mock.patch.object(
        deliver.db.CharonSession,
        'project_create',
        return_value="mocked return value")
    def test_wrap_database_query(self,dbmock):
        self.assertEqual(
            self.deliverer.wrap_database_query(
                self.deliverer.dbcon().project_create,
                "funarg1",
                funarg2="funarg2"),
            "mocked return value")
        dbmock.assert_called_with(
            "funarg1",
            funarg2="funarg2")
        dbmock.side_effect = db.CharonError("mocked error")
        with self.assertRaises(deliver.DelivererDatabaseError):
            self.deliverer.wrap_database_query(
                self.deliverer.dbcon().project_create,
                "funarg1",
                funarg2="funarg2")
        
    def test_gather_files1(self):
        """ Gather files in the top directory """
        expected = [
            os.path.join(
                self.deliverer.expand_path(self.deliverer.analysispath),
                "level0_folder0_file{}".format(n)) \
            for n in xrange(self.nfiles)]
        pattern = SAMPLECFG['deliver']['files_to_deliver'][0]
        self.deliverer.files_to_deliver = [pattern]
        self.assertItemsEqual(
            [p for p,_,_ in self.deliverer.gather_files()],
            expected)
            
    def test_gather_files2(self):
        """ Gather a folder in the top directory """
        expected = [os.path.join(d,f) for d,_,files in os.walk(
            os.path.join(
                self.deliverer.expand_path(self.deliverer.analysispath),
                "level1_folder2")) for f in files]
        pattern = SAMPLECFG['deliver']['files_to_deliver'][1]
        self.deliverer.files_to_deliver = [pattern]
        self.assertItemsEqual(
            [p for p,_,_ in self.deliverer.gather_files()],
            expected)
        
    def test_gather_files3(self):
        """ Gather the files two levels down """
        expected = ["level2_folder{}_file{}".format(m,n) 
                    for m in xrange(self.nfolders) 
                    for n in xrange(self.nfiles)]
        pattern = SAMPLECFG['deliver']['files_to_deliver'][2]
        self.deliverer.files_to_deliver = [pattern]
        self.assertItemsEqual(
            [os.path.basename(p) for p,_,_ in self.deliverer.gather_files()],
            expected)
            
        
    def test_gather_files4(self):
        """ Replace the SAMPLE keyword in pattern """
        expected = ["level1_folder{}_file0".format(n) 
                    for n in xrange(self.nfolders)]
        pattern = SAMPLECFG['deliver']['files_to_deliver'][3]
        self.deliverer.files_to_deliver = [pattern]
        self.deliverer.sampleid = "level1"
        self.assertItemsEqual(
            [os.path.basename(p) for p,_,_ in self.deliverer.gather_files()],
            expected)
            
        
    def test_gather_files5(self):
        """ Do not pick up non-existing file """
        expected = []
        pattern = SAMPLECFG['deliver']['files_to_deliver'][4]
        self.deliverer.files_to_deliver = [pattern]
        self.assertItemsEqual(
            [os.path.basename(p) for p,_,_ in self.deliverer.gather_files()],
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
        with open(checksumfile,'w') as fh:
            fh.write(exp_checksum)
        for _,_,obs_checksum in self.deliverer.gather_files():
            self.assertEqual(
                obs_checksum,
                exp_checksum,
                "checksum '{}' from cache file was not picked up: '{}'".format(
                    obs_checksum,exp_checksum))
        os.unlink(checksumfile)
        # assert that the checksum file is created as expected
        for spath,_,exp_checksum in self.deliverer.gather_files():
            cheksumfile = "{}.{}".format(spath,self.deliverer.hash_algorithm)
            self.assertTrue(os.path.exists(checksumfile),
                "checksum cache file was not created")
            with open(checksumfile,'r') as fh:
                obs_checksum = fh.next()
            self.assertEqual(obs_checksum,exp_checksum,
                "cached and returned checksums did not match")
            os.unlink(checksumfile)
        # ensure that a thrown IOError when writing checksum cache file is handled gracefully
        # do a double mock to mask hashfile's call to open builtin
        with mock.patch.object(
            __builtin__,
            'open',
            side_effect=IOError("mocked IOError")) as iomock, mock.patch.object(
                deliver,
                'hashfile',
                return_value="mocked-digest"
            ) as digestmock:
            for spath,_,obs_checksum in self.deliverer.gather_files():
                cheksumfile = "{}.{}".format(
                    spath,self.deliverer.hash_algorithm)
                self.assertFalse(os.path.exists(checksumfile),
                    "checksum cache file should not have been created")
                self.assertTrue(iomock.call_count == 2,"open should have been "\
                    "called twice on checksum cache file")
                self.assertEqual(
                    "mocked-digest",
                    obs_checksum,"observed cheksum doesn't match expected")

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
            dest_path=os.path.join(dest_path,"level3_folder0"),
            relative=False)
        self.assertTrue(
            sa.transfer(),
            "failed when setting up test")
        
        expected = [os.path.join(dest_path,"level3_folder1_file{}".format(n)) 
                    for n in xrange(self.nfiles)]
        expected.extend([
            os.path.join(
                dest_path,
                "level3_folder0",
                "level3_folder0_file{}".format(n)) 
                    for n in xrange(self.nfiles)])
        pattern = SAMPLECFG['deliver']['files_to_deliver'][6]
        self.deliverer.files_to_deliver = [pattern]
        self.assertItemsEqual(
            [p for p,_,_ in self.deliverer.gather_files()],
            expected)

    def test_gather_files8(self):
        """ Skip checksum files """
        expected = []
        pattern = SAMPLECFG['deliver']['files_to_deliver'][7]
        self.deliverer.files_to_deliver = [pattern]
        open(self.deliverer.expand_path(pattern[0]),'w').close()
        self.assertItemsEqual(
            [obs for obs in self.deliverer.gather_files()],
            expected)
    
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
        with mock.patch.object(deliver,'create_folder',return_value=False):
            with self.assertRaises(deliver.DelivererError):
                self.deliverer.stage_delivery()
        with mock.patch.object(
            deliver.Deliverer,'gather_files',return_value=[gathered_files]):
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
                    os.path.join(d,f),
                    self.deliverer.expand_path(self.deliverer.analysispath))) \
                for d,_,files in os.walk(
                    os.path.join(
                        self.deliverer.expand_path(self.deliverer.analysispath),
                        "level1_folder2")) \
                for f in files]
        pattern = SAMPLECFG['deliver']['files_to_deliver'][1]
        self.deliverer.files_to_deliver = [pattern]
        self.deliverer.stage_delivery()
        self.assertItemsEqual(
            [os.path.exists(e) for e in expected],
            [True for i in xrange(len(expected))])
    
    def test_expand_path(self):
        """ Paths should expand correctly """
        cases = [
            ["this-path-should-not-be-touched",
            "this-path-should-not-be-touched",
            "a path without placeholders was modified"],
            ["this-path-_SHOULD_-be-touched",
            "this-path-was-to-be-touched",
            "a path with placeholders was not correctly modified"],
            ["this-path-_SHOULD_-be-touched-_MULTIPLE_",
            "this-path-was-to-be-touched-twice",
            "a path with multiple placeholders was not correctly modified"],
            ["this-path-should-_not_-be-touched",
            "this-path-should-_not_-be-touched",
            "a path without valid placeholders was modified"],
            [None,None,"a None path should be handled without exceptions"]]
        self.deliverer.should = 'was-to'
        self.deliverer.multiple = 'twice'
        for case, exp, msg in cases:
            self.assertEqual(self.deliverer.expand_path(case),exp,msg)
        with self.assertRaises(deliver.DelivererError):
            self.deliverer.expand_path("this-path-_WONT_-be-touched")

class TestProjectDeliverer(unittest.TestCase):  
    
    @classmethod
    def setUpClass(self):
        self.rootdir = tempfile.mkdtemp(prefix="test_taca_deliver_")
        
    @classmethod
    def tearDownClass(self):
        shutil.rmtree(self.rootdir)
    
    @mock.patch.object(deliver.Deliverer,'dbcon',autospec=db.CharonSession)
    def setUp(self,dbmock):
        self.casedir = tempfile.mkdtemp(prefix="case_",dir=self.rootdir)
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
            getattr(self,'deliverer'),
            deliver.ProjectDeliverer)
    
    @mock.patch.object(
        deliver.db.CharonSession,
        'project_update',
        return_value="mocked return value")
    def test_update_delivery_status(self,dbmock):
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
            os.kill(os.getpid(),signal.SIGINT)

    def test_catching_sigterm(self):
        """ SIGTERM should raise DelivererInterruptedError
        """
        with self.assertRaises(deliver.DelivererInterruptedError):
            os.kill(os.getpid(),signal.SIGTERM)

class TestSampleDeliverer(unittest.TestCase):  
    
    @classmethod
    def setUpClass(self):
        self.rootdir = tempfile.mkdtemp(prefix="test_taca_deliver_")
        
    @classmethod
    def tearDownClass(self):
        shutil.rmtree(self.rootdir)
    
    @mock.patch.object(deliver.Deliverer,'dbcon',autospec=db.CharonSession)
    def setUp(self,dbmock):
        self.casedir = tempfile.mkdtemp(prefix="case_",dir=self.rootdir)
        self.projectid = 'NGIU-P001'
        self.sampleid = 'NGIU-S001'
        self.deliverer = deliver.SampleDeliverer(
            self.projectid,
            self.sampleid,
            rootdir=self.casedir,
            **SAMPLECFG['deliver'])
            
    def tearDown(self):
        shutil.rmtree(self.casedir)
    
    def test_init(self):
        """ A SampleDeliverer should initiate properly """
        self.assertIsInstance(
            getattr(self,'deliverer'),
            deliver.SampleDeliverer)
    
    @mock.patch.object(
        deliver.db.CharonSession,
        'sample_update',
        return_value="mocked return value")
    def test_update_delivery_status(self,dbmock):
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
            os.kill(os.getpid(),signal.SIGINT)

    def test_catching_sigterm(self):
        """ SIGTERM should raise DelivererInterruptedError
        """
        with self.assertRaises(deliver.DelivererInterruptedError):
            os.kill(os.getpid(),signal.SIGTERM)

    def test_deliver_sample1(self):
        """ transfer a sample using rsync
        """
        # create some content to transfer
        digestfile = self.deliverer.staging_digestfile()
        filelist = self.deliverer.staging_filelist()
        basedir = os.path.dirname(digestfile)
        create_folder(basedir)
        expected = []
        with open(digestfile,'w') as dh, open(filelist,'w') as fh:
            curdir = basedir
            for d in xrange(4):
                if d > 0:
                    curdir = os.path.join(curdir,"folder{}".format(d))
                    create_folder(curdir)
                for n in xrange(5):
                    fpath = os.path.join(curdir,"file{}".format(n))
                    open(fpath,'w').close()
                    rpath = os.path.relpath(fpath,basedir)
                    dh.write("{}  {}\n".format(
                        hashfile(fpath,hasher=self.deliverer.hash_algorithm),
                        rpath))
                    if n < 3:
                        expected.append(rpath)
                        fh.write("{}\n".format(rpath))
            rpath = os.path.basename(digestfile)
            expected.append(rpath)
            fh.write("{}\n".format(rpath))
        # transfer the listed content
        destination = self.deliverer.expand_path(self.deliverer.deliverypath)
        create_folder(os.path.dirname(destination))
        self.assertTrue(self.deliverer.do_delivery(),"failed to deliver sample")
        # list the trasferred files relative to the destination
        observed = [os.path.relpath(os.path.join(d,f),destination) \
            for d,_,files in os.walk(destination) for f in files]
        self.assertItemsEqual(observed,expected)
