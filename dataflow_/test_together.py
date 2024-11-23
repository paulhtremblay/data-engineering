import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from together1 import MapAndCombineTransform

class TestBeam(unittest.TestCase):

# This test corresponds to example 3, and is written to confirm the pipeline works as intended.
  def test_transform_data_set(self):
    expected=[(1, 10570.185786231425), (2, 13.375337533753376), (3, 13.315649867374006)]
    input_elements = [
      '-122.050000,37.370000,27.000000,3885.000000,661.000000,1537.000000,606.000000,6.608500,344700.000000',
      '121.05,99.99,23.30,39.5,55.55,41.01,10,34,74.30,91.91',
      '122.05,100.99,24.30,40.5,56.55,42.01,11,35,75.30,92.91',
      '-120.05,39.37,29.00,4085.00,681.00,1557.00,626.00,6.8085,364700.00'
    ]
    with beam.Pipeline() as p2:
      result = (
                p2
                | beam.Create(input_elements)
                | MapAndCombineTransform()
        )
      assert_that(result,equal_to(expected))

if __name__ == '__main__':
    unittest.main()
