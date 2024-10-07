import argparse
import logging
import os

import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions


from time import sleep
import random

from apache_beam.io.restriction_trackers import OffsetRange

#import pdb; pdb.set_trace()

class FileToWordsRestrictionProvider(beam.transforms.core.RestrictionProvider
                                     ):

    def initial_restriction(self, list_):
        return OffsetRange(0, len(list_))

    def create_tracker(self, restriction):
        return beam.io.restriction_trackers.OffsetRestrictionTracker(restriction)

    def restriction_size(self, element, restriction):
        return restriction.size()


class SplitFn(beam.DoFn):
    def process(
        self,
        list_,
        tracker=beam.DoFn.RestrictionParam(FileToWordsRestrictionProvider())):
        counter = 0
        while tracker.try_claim(counter):
            counter += 1
            yield list_[counter -1: counter]


def get_next_list(list_):
    e = list_[0:1]
    #print(e)


def run():
    pipeline_options = PipelineOptions()

    with beam.Pipeline(options=pipeline_options) as p:
        execute_pipeline(p)


def execute_pipeline(p):
    _ = (
          p |
          'Create' >> beam.Create([[1, 2]]) |
          'Read File' >> beam.ParDo(SplitFn())
          | beam.Map(print)
    )


if __name__ == '__main__':
    run()
