# Batch Processing Pipeline using DataFlow (Under Construction)
This is one of the part of **Introduction to Apache Beam using Python** Repository. Here we will try to learn basics of Apache Beam to create **Batch** pipelines. We will learn step by step how to create a batch pipeline using [German Credit Risk](https://www.kaggle.com/uciml/german-credit). The complete process is divided into 5 parts:

1. **Reading the data**
2. **Parsing the data**
3. **Filtering the data**
4. **Performing Type Convertion**
5. **Data wrangling**
6. **Inserting Data in Bigquery**


## Motivation
For the last two years, I have been part of a great learning curve wherein I have upskilled myself to move into a Machine Learning and Cloud Computing. This project was practice project for all the learnings I have had. This is first of the many more to come. 
 

## Libraries/frameworks used

<b>Built with</b>
- [Apache Beam](https://beam.apache.org/documentation/programming-guide/)
- [Anaconda](https://www.anaconda.com/)
- [Python](https://www.python.org/)
- [Google DataFlow](https://cloud.google.com/dataflow)
- [Google Cloud Storage](https://cloud.google.com/storage)

## Code Example

```bash
    # clone this repo:
    git clone https://github.com/adityasolanki205/Batch-Processing-Pipeline-using-DataFlow.git
    
    # Installing Apache Beam on the SDK
    sudo pip3 install apache_beam[gcp]
    
    # Copy the Data from SDK to Cloud Storage
    cd Batch-Processing-Pipeline-using-DataFlow/data
    gsutil cp german.data gs://batch-pipeline-testing/batch/
```
    # Run the Pipeline
    python3 Testing.py --runner DataFlowRunner --project <Your Project Name> 
    --temp_location gs://batch-pipeline-testing/Batch/Temp 
    --staging_location gs://batch-pipeline-testing/Batch/Stage 
    --input gs://batch-pipeline-testing/Batch/german.data 
    --region asia-east1 
    --job_name germannnalysis
```

## Pipeline Construction

Below are the steps to setup the enviroment and run the codes:

1. **Setup**: First we will have to setup free google cloud account which can be done [here](https://cloud.google.com/free).Then we need to Download the data from [German Credit Risk](https://www.kaggle.com/uciml/german-credit).

2. **Cloning the Repository to Cloud SDK**: We will have to copy the repository on Cloud SDK using below command:

```bash
    # clone this repo:
    git clone https://github.com/adityasolanki205/Batch-Processing-Pipeline-using-DataFlow.git
```

3. **Reading the Data**: Now we will go step by step to create a pipeline starting with reading the data. The data is read using **beam.io.ReadFromText**. Here we will just read the input values and save it in a file. The output is stored in text file named simpleoutput.

```python
    def run(argv=None, save_main_session=True):
        parser = argparse.ArgumentParser()
        parser.add_argument(
          '--input',
          dest='input',
          help='Input file to process')
        parser.add_argument(
          '--output',
          dest='output',
          default='../output/result.txt',
          help='Output file to write results to.')
        known_args, pipeline_args = parser.parse_known_args(argv)
        options = PipelineOptions(pipeline_args)
        with beam.Pipeline(options=PipelineOptions()) as p:
            data = (p 
                         | beam.io.ReadFromText(known_args.input)
                         | 'Writing output' >> beam.io.WriteToText(known_args.output)
                   ) 

    if __name__ == '__main__':
        run()
``` 


## Credits
1. Akash Nimare's [README.md](https://gist.github.com/akashnimare/7b065c12d9750578de8e705fb4771d2f#file-readme-md)
2. [Apache Beam](https://beam.apache.org/documentation/programming-guide/#triggers)
