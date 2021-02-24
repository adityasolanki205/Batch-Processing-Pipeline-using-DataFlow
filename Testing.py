#!/usr/bin/env python
# coding: utf-8

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import argparse

SCHEMA =  'Existing_account:STRING,Duration_month:INTEGER,Credit_history:STRING,Purpose:STRING,Credit_amount:FLOAT,Saving:STRING,Employment_duration:STRING,Installment_rate:INTEGER,Personal_status:STRING,Debtors:STRING,Residential_Duration:INTEGER,Property:STRING,Age:INTEGER,Installment_plans:STRING,Housing:STRING,Number_of_credits:INTERGER,Job:STRING,Liable_People:INTEGER,Telephone:STRING,Foreign_worker:STRING,Classification:INTEGER'

class Split(beam.DoFn):
    def process(self, element):
        Existing_account,  Duration_month,Credit_history,Purpose,Credit_amount,Saving,Employment_duration,Installment_rate,Personal_status,Debtors,Residential_Duration,Property,Age,Installment_plans,Housing,Number_of_credits,Job,Liable_People,Telephone,Foreign_worker,Classification = element.split(' ')
        return [{
            'Existing_account': str(Existing_account),
            'Duration_month': str(Duration_month),
            'Credit_history': str(Credit_history),
            'Purpose': str(Purpose),
            'Credit_amount': str(Credit_amount),
            'Saving': str(Saving),
            'Employment_duration':str(Employment_duration),
            'Installment_rate': str(Installment_rate),
            'Personal_status': str(Personal_status),
            'Debtors': str(Debtors),
            'Residential_Duration': str(Residential_Duration),
            'Property': str(Property),
            'Age': str(Age),
            'Installment_plans':str(Installment_plans),
            'Housing': str(Housing),
            'Number_of_credits': str(Number_of_credits),
            'Job': str(Job),
            'Liable_People': str(Liable_People),
            'Telephone': str(Telephone),
            'Foreign_worker': str(Foreign_worker),
            'Classification': str(Classification)
        }]

def filter_data(data):
    return data['Purpose'] !=  'NULL' and data['Classification'] !=  'NULL' and data['Property'] !=  'NULL' and data['Personal_status'] != 'NULL' and data['Existing_account'] != 'NULL' and data['Credit_amount'] != 'NULL' 
    
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
                     | beam.ParDo(Split())
                     | beam.Filter(filter_data)
                     | beam.io.WriteToText(known_args.output)
                    )
        
if __name__ == '__main__':
    run()