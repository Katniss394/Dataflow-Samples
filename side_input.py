from __future__ import absolute_import

import argparse
import logging
import re

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

from apache_beam.pvalue import AsList
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class SplitLinesToWordsFn(beam.DoFn):

    def process(self, element):
    
        id = element[:4]
        quantity = int(element[-2:])
    
        if quantity >= 90:
            yield id

class GetItemsOnOfferFn(beam.DoFn):
    
    def process(self, element):
    
        id = element[:4]
        yield id

def match_ids_fn( sold_item, discounted_items):
    
    if sold_item in discounted_items:
        yield sold_item

def run(argv=None):
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--input1',
                        required=True,
                        help='Input file to process.')
            
    parser.add_argument('--input2',
                        required=True,
                        help='Input file to process.')
                
    parser.add_argument('--output',
                        required=True,
                        help='Output prefix for files to write results to.')
                    
    known_args, pipeline_args = parser.parse_known_args(argv)
                        
    pipeline_options = PipelineOptions(pipeline_args)
                            
    pipeline_options.view_as(SetupOptions).save_main_session = True
                                
    with beam.Pipeline(options=pipeline_options) as p:
                                    
    sold_items = p | 'Read input-one' >> ReadFromText(known_args.input1)
                                        
    offer_applied_items = p | 'Read input-two' >> ReadFromText(known_args.input2)
                                            
    sold_items_result = (sold_items | 'Read Product IDs' >> beam.ParDo(SplitLinesToWordsFn()))
                                                
    discounted_items_result = (offer_applied_items | 'Find Discounted Product IDs' >> beam.ParDo(GetItemsOnOfferFn()))
     pcoll_discounted_productids = ( sold_items_result |'Find Discounted Top Sellers' >> beam.FlatMap(match_ids_fn,AsList(discounted_items_result)))
                                                        
     (pcoll_discounted_productids  'Write Results' >> WriteToText(known_args.output + 'sold-on-discount'))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
        run()
