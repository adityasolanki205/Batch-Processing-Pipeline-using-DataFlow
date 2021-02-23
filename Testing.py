#!/usr/bin/env python
# coding: utf-8

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import argparse

SCHEMA =  'Existing_account:STRING,Duration_month:INTEGER,Credit_history:STRING,Purpose:STRING,Credit_amount:FLOAT,Saving:STRING,Employment_duration:STRING,Installment_rate:INTEGER,Personal_status:STRING,Debtors:STRING,Residential_Duration:INTEGER,Property:STRING,Age:INTEGER,Installment_plans:STRING,Housing:STRING,Number_of_credits:INTERGER,Job:STRING,Liable_People:INTEGER,Telephone:STRING,Foreign_worker:STRING,Classification:INTEGER'

class Split(beam.DoFn):
    def process(self, element):
        Existing_account,Duration_month,Credit_history,Purpose,Credit_amount,Saving,Employment_duration,Installment_rate,Personal_status,Debtors,Residential_Duration,Property,Age,Installment_plans,Housing,Number_of_credits,Job,Liable_People,Telephone,Foreign_worker,Classification= element.split(' ')
        return [{
            'Date': Date,
            'Open': float(Open),
            'Close': float(Close)
        }]

    
def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
      '--input',
      dest='input',
      default='../data/german.data',
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
                     | beam.Pardo(Split())
                     | beam.io.WriteToText(known_args.output)
                    )
        
if __name__ == '__main__':
    run()