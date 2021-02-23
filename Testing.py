#!/usr/bin/env python
# coding: utf-8

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import argparse
class Split(beam.DoFn):
    def process(self, element):
        Date,Open,High,Low,Close,Volume, AdjClose = element.split(',')
        return [{
            'Date': Date,
            'Open': float(Open),
            'Close': float(Close)
        }]
class CollectOpen(beam.DoFn):
    def process(self, element):
        result = [(1,element['Open'])]
        return result

class CollectClose(beam.DoFn):
    def process(self, element):
        result = [(1,element['Close'])]
        return result
    
def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
      '--input',
      dest='input',
      default='../data/sp500.csv',
      help='Input file to process.')
    parser.add_argument(
      '--output',
      dest='output',
      default='../output/result.txt',
      help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)
    options = PipelineOptions(pipeline_args)
    with beam.Pipeline(options=PipelineOptions()) as p:
        csv_lines = (p 
                     | beam.io.ReadFromText(known_args.input, skip_header_lines = 1) 
                     | beam.ParDo(Split()))
        open_col  = (csv_lines 
                     | beam.ParDo(CollectOpen()) 
                     | "Grouping Keys Open" >> beam.GroupByKey()
                    )
        close_col = (csv_lines 
                     | beam.ParDo(CollectClose())
                     | "Grouping Keys Close" >> beam.GroupByKey()
                    )
        '''output    = ( 
                    ({'Open'  : open_col, 
                      'Close' : close_col} 
                     | beam.CoGroupByKey())
                     | beam.io.WriteToText(known_args.output)
                    )'''
        '''output = ( (close_col, open_col)
                     | beam.Flatten()
                     | beam.io.WriteToText(known_args.output)
                    )'''
        '''output = (open_col 
                      | 'Sum' >> beam.CombineValues(sum) 
                      | beam.io.WriteToText(known_args.output)
                     )'''
        mean_open = ( open_col 
                     | "Calculating mean for open" >> beam.CombineValues(beam.combiners.MeanCombineFn())
                     | beam.io.WriteToText(known_args.output)
                    )
        
if __name__ == '__main__':
    run()