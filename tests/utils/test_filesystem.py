import unittest

import taca_ngi_pipeline.utils.filesystem as filesystem


class TestFilesystem(unittest.TestCase):
    
    def test_gather_files(self):
        files_to_deliver = [['data/deliver_testset*', 'data/stage']]
        found = filesystem.gather_files(files_to_deliver)
        expected_sourcepath = 'data/deliver_testset.tar.gz'
        expected_dest_path = 'data/stage/deliver_testset.tar.gz'
        expected_digest = '640ec90a89e9d8aaca6d5364e4139375  deliver_testset.tar.gz\n'
        for src, dest, dig in found:
            self.assertEqual(src, expected_sourcepath)
            self.assertEqual(dest, expected_dest_path)
            self.assertEqual(dig, expected_digest)
    
    def test_parse_hash_file(self):
        hashfile = 'data/deliver_testset.tar.gz.md5'
        got_dict = filesystem.parse_hash_file(hashfile, '2020-12-07', root_path='data')
        expected_dict = {'deliver_testset.tar':
                            {'deliver_testset.tar.gz':
                                {'size_in_bytes': 52639,
                                 'md5_sum': '640ec90a89e9d8aaca6d5364e4139375',
                                 'last_modified': '2020-12-07'}
                                }
                            }
        self.assertEqual(got_dict, expected_dict)
    
    def test_merge_dicts(self):
        d1 = {'A': {'a1': ['a', 'b'], 
                    'a2': ['c', 'd']}, 
              'B': 'b1', 
              'C': 'c1'}
        d2 = {'A': {'a1': ['a', 'b']}, 
              'B': 'b1'}
        merged_dict = filesystem.merge_dicts(d1, d2)
        expected_dict = {'A': {'a1': ['a', 'b'], 
                               'a2': ['c', 'd']}, 
                         'B': 'b1',
                         'C': 'c1'}
        self.assertEqual(merged_dict, expected_dict)