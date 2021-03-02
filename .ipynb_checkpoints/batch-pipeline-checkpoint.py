#!/usr/bin/env python
# coding: utf-8


import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import argparse

SCHEMA='Duration_month:INTEGER,Credit_history:STRING,Credit_amount:FLOAT,Saving:STRING,Employment_duration:STRING,Installment_rate:INTEGER,Personal_status:STRING,Debtors:STRING,Residential_Duration:INTEGER,Property:STRING,Age:INTEGER,Installment_plans:STRING,Housing:STRING,Number_of_credits:INTEGER,Job:STRING,Liable_People:INTEGER,Telephone:STRING,Foreign_worker:STRING,Classification:INTEGER,Month:STRING,days:INTEGER,File_Month:STRING,Version:INTEGER'


class Split(beam.DoFn):
    #This Function Splits the Dataset into a dictionary
    def process(self, element):
        Existing_account,Duration_month,Credit_history,Purpose,Credit_amount,Saving,Employment_duration,Installment_rate,Personal_status,Debtors,Residential_Duration,Property,Age,Installment_plans,Housing,Number_of_credits,Job,Liable_People,Telephone,Foreign_worker,Classification = element.split(' ')
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

def Filter_Data(data):
    #This will remove rows the with Null values in any one of the columns
    return data['Purpose'] !=  'NULL' and len(data['Purpose']) <= 3  and  data['Classification'] !=  'NULL' and data['Property'] !=  'NULL' and data['Personal_status'] != 'NULL' and data['Existing_account'] != 'NULL' and data['Credit_amount'] != 'NULL' and data['Installment_plans'] != 'NULL'

def Convert_Datatype(data):
    #This will convert the datatype of columns from String to integers or Float values
    data['Duration_month'] = int(data['Duration_month']) if 'Duration_month' in data else None
    data['Credit_amount'] = float(data['Credit_amount']) if 'Credit_amount' in data else None
    data['Installment_rate'] = int(data['Installment_rate']) if 'Installment_rate' in data else None
    data['Residential_Duration'] = int(data['Residential_Duration']) if 'Residential_Duration' in data else None
    data['Age'] = int(data['Age']) if 'Age' in data else None
    data['Number_of_credits'] = int(data['Number_of_credits']) if 'Number_of_credits' in data else None
    data['Liable_People'] = int(data['Liable_People']) if 'Liable_People' in data else None
    data['Classification'] =  int(data['Classification']) if 'Classification' in data else None
   
    return data

def Data_Wrangle(data):
    #Here we perform data wrangling where Values in columns are converted to make more sense
    Month_Dict = {
    'A':'January',
    'B':'February',
    'C':'March',
    'D':'April',
    'E':'May',
    'F':'June',
    'G':'July',
    'H':'August',
    'I':'September',
    'J':'October',
    'K':'November',
    'L':'December'
    }
    existing_account = list(data['Existing_account'])
    for i in range(len(existing_account)):
        month = Month_Dict[existing_account[0]]
        days = int(''.join(existing_account[1:]))
        data['Month'] = month
        data['days'] = days
    purpose = list(data['Purpose'])
    for i in range(len(purpose)):
        file_month = Month_Dict[purpose[0]]
        version = int(''.join(purpose[1:]))
        data['File_Month'] = file_month
        data['Version'] = version
    return data

def Del_Unwanted(data):
    #Here we delete redundant columns
    del data['Purpose']
    del data['Existing_account']
    return data
    
def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
      '--input',
      dest='input',
      help='Input file to process')
    parser.add_argument(
      '--project',
      dest='project',
      help='Project used for this Pipeline')
    known_args, pipeline_args = parser.parse_known_args(argv)
    options = PipelineOptions(pipeline_args)
    PROJECT_ID = known_args.project
    with beam.Pipeline(options=PipelineOptions()) as p:
        data = (p 
                     | beam.io.ReadFromText(known_args.input) )
        parsed_data = (data 
                     | 'Parsing Data' >> beam.ParDo(Split()))
        filtered_data = (parsed_data
                     | 'Filtering Data' >> beam.Filter(Filter_Data))
        Converted_data = (filtered_data
                     | 'Convert Datatypes' >> beam.Map(Convert_Datatype))
        Wrangled_data = (Converted_data
                     | 'Wrangling Data' >> beam.Map(Data_Wrangle))
        Cleaned_data = (Wrangled_data
                     | 'Delete Unwanted Columns' >> beam.Map(Del_Unwanted))
        output =( Cleaned_data      
                     | 'Writing to bigquery' >> beam.io.WriteToBigQuery(
                       '{0}:GermanCredit.GermanCreditTable'.format(PROJECT_ID),
                       schema=SCHEMA,
                       write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
                )
        
if __name__ == '__main__':
    run()