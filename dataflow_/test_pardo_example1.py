import unittest
import datetime
import tempfile
import glob
import os
import shutil
import csv

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.io import WriteToText

from pardo_example1 import ExampleWithStartbundle

def _get_data_form_temp_dir(dir_name):
    files = glob.glob(f'{dir_name}/*')
    l = []
    for i in files:
        assert os.path.isfile(os.path.join(dir_name, i))
        with open(os.path.join(dir_name, i), 'r') as read_obj:
            for i in read_obj:
                l.append(i)
        """
        with open(os.path.join(dir_name, i), 'r') as read_obj:
            csv_reader = csv.reader(read_obj)
            for i in csv_reader:
                l.extend(i)
        """
    return l

class TestPardoExample1(unittest.TestCase):

  def test_transform_data_set(self):
    expected=[(1, 10570.185786231425), (2, 13.375337533753376), (3, 13.315649867374006)]
    expected = [(('user1', 1), 
                 datetime.datetime(2024, 11, 24, 11, 15, 35, 852880), 
                 datetime.datetime(2024, 11, 24, 11, 15, 35, 858920)),
                (('user2', 2), 
                 datetime.datetime(2024, 11, 24, 11, 15, 35, 852880), 
                 datetime.datetime(2024, 11, 24, 11, 15, 35, 858920))
                ]
    input_elements =  [
        ('user1', 1),
        ('user2', 2),
    ]
    temp_dir =  tempfile.mkdtemp()

    with beam.Pipeline() as p2:
        result = (
                p2
                | beam.Create(input_elements)
                | beam.ParDo(ExampleWithStartbundle(arg1 = 'foo'))
                | WriteToText(f'{temp_dir}/')
                )
        
        #assert_that(r,equal_to(expected))
    r = _get_data_form_temp_dir(temp_dir)
    shutil.rmtree(temp_dir)

    self.assertEqual(len(r), 2)
if __name__ == '__main__':
    unittest.main()
