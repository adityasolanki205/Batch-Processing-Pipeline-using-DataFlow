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

def normalize_for_bigquery(data):

    return {
        "Duration_month": int(data["Duration_month"]) if data.get("Duration_month") is not None else None,
        "Credit_history": str(data["Credit_history"]) if data.get("Credit_history") is not None else None,
        "Credit_amount": float(data["Credit_amount"]) if data.get("Credit_amount") is not None else None,
        "Saving": str(data["Saving"]) if data.get("Saving") is not None else None,
        "Employment_duration": str(data["Employment_duration"]) if data.get("Employment_duration") is not None else None,
        "Installment_rate": int(data["Installment_rate"]) if data.get("Installment_rate") is not None else None,
        "Personal_status": str(data["Personal_status"]) if data.get("Personal_status") is not None else None,
        "Debtors": str(data["Debtors"]) if data.get("Debtors") is not None else None,
        "Residential_Duration": int(data["Residential_Duration"]) if data.get("Residential_Duration") is not None else None,
        "Property": str(data["Property"]) if data.get("Property") is not None else None,
        "Age": int(data["Age"]) if data.get("Age") is not None else None,
        "Installment_plans": str(data["Installment_plans"]) if data.get("Installment_plans") is not None else None,
        "Housing": str(data["Housing"]) if data.get("Housing") is not None else None,
        "Number_of_credits": int(data["Number_of_credits"]) if data.get("Number_of_credits") is not None else None,
        "Job": str(data["Job"]) if data.get("Job") is not None else None,
        "Liable_People": int(data["Liable_People"]) if data.get("Liable_People") is not None else None,
        "Telephone": str(data["Telephone"]) if data.get("Telephone") is not None else None,
        "Foreign_worker": str(data["Foreign_worker"]) if data.get("Foreign_worker") is not None else None,
        "Classification": int(data["Classification"]) if data.get("Classification") is not None else None,
        "Month": str(data["Month"]) if data.get("Month") is not None else None,
        "days": int(data["days"]) if data.get("days") is not None else None,
        "File_Month": str(data["File_Month"]) if data.get("File_Month") is not None else None,
        "Version": int(data["Version"]) if data.get("Version") is not None else None,
    }


def Filter_Data(data):
    #This will remove rows the with Null values in any one of the columns
    return data['Purpose'] !=  'NULL' and len(data['Purpose']) <= 3  and  data['Classification'] !=  'NULL' and data['Property'] !=  'NULL' and data['Personal_status'] != 'NULL' and data['Existing_account'] != 'NULL' and data['Credit_amount'] != 'NULL' and data['Installment_plans'] != 'NULL'


def Convert_Datatype(data):

    return {
        'Existing_account': data.get('Existing_account'),
        'Duration_month': int(data['Duration_month'])
            if data.get('Duration_month') not in (None, '', 'NULL') else None,
        'Credit_history': data.get('Credit_history'),
        'Purpose': data.get('Purpose'),
        'Credit_amount': float(data['Credit_amount'])
            if data.get('Credit_amount') not in (None, '', 'NULL') else None,
        'Saving': data.get('Saving'),
        'Employment_duration': data.get('Employment_duration'),
        'Installment_rate': int(data['Installment_rate'])
            if data.get('Installment_rate') not in (None, '', 'NULL') else None,
        'Personal_status': data.get('Personal_status'),
        'Debtors': data.get('Debtors'),
        'Residential_Duration': int(data['Residential_Duration'])
            if data.get('Residential_Duration') not in (None, '', 'NULL') else None,
        'Property': data.get('Property'),
        'Age': int(data['Age'])
            if data.get('Age') not in (None, '', 'NULL') else None,
        'Installment_plans': data.get('Installment_plans'),
        'Housing': data.get('Housing'),
        'Number_of_credits': int(data['Number_of_credits'])
            if data.get('Number_of_credits') not in (None, '', 'NULL') else None,
        'Job': data.get('Job'),
        'Liable_People': int(data['Liable_People'])
            if data.get('Liable_People') not in (None, '', 'NULL') else None,
        'Telephone': data.get('Telephone'),
        'Foreign_worker': data.get('Foreign_worker'),
        'Classification': int(data['Classification'])
            if data.get('Classification') not in (None, '', 'NULL') else None
    }
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
    option = PipelineOptions(pipeline_args)
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
        Normalized_data = (Cleaned_data
                    | "Normalize BigQuery Types" >> beam.Map(normalize_for_bigquery))
        output =( Normalized_data      
                     | 'Writing to bigquery' >> beam.io.WriteToBigQuery(
                       '{0}:GermanCredit.GermanCreditTable'.format(PROJECT_ID),
                       schema=SCHEMA,
                       write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
                )
        
if __name__ == '__main__':
    run()
