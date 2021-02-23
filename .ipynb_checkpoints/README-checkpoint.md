# Intro to Apache Beam (Under Construction)
This is a **Introduction to Apache Beam using Python** Repository. Here we will try to learn basics of Apache Beam to create **Batch** and **Streaming** pipelines. We will follow the learn step by step how to create a pipeline and what are the outputs after each phase. To establish that we will try to create simple pipeline to calculate the mean of two columns in a CSV file.

1. **Introduction to Apache Beam Model**
2. **Basic Codes**
3. **Batch Pipelines**
4. **Streaming Pipeplines**
5. **Conclusion**


## Motivation
For the last two years, I have been part of a great learning curve wherein I have upskilled myself to move into a Machine Learning and Cloud Computing. This project was practice project for all the learnings I have had. This is first of the many more to come. 
 

## Libraries/frameworks used

<b>Built with</b>
- [Apache Beam](https://beam.apache.org/documentation/programming-guide/)
- [Anaconda](https://www.anaconda.com/)
- [Python](https://www.python.org/)

## Code Example

```bash
    # clone this repo, removing the '-' to allow python imports:
    git clone https://github.com/adityasolanki205/Intro-to-Apache-Beam.git
    
    # Installing Virtual Environment
    pip install --upgrade virtualenv
    
    # Create virtual environment 
    virtualenv /path/to/directory
    
    # Activate a virtual environment
    . /path/to/directory/bin/activate
    
    # Install Apache Beam
    pip install apache-beam

    # Execute a Pipeline
    python -m Testing --input ./data/sp500.csv --output ./output/result.txt
    
```

## Apache Beam

Below are the steps to setup the enviroment and run the codes:

- **Introduction to Apache Beam Model**: Apache Beam is an open source model for creating both batch and streaming data-parallel processing pipelines. we will use python to build a program that defines the pipeline. The pipeline is then executed by one of Beam’s supported distributed processing back-ends like Google Cloud Dataflow.

    Beam is particularly useful for Embarrassingly Parallel data processing tasks, for Extract, Transform, and Load (ETL) tasks and pure data integration. These tasks are useful for moving data between different storage media and data sources, transforming data into a more desirable format, or loading data onto a new system.

    Everything in Apache beam are done in form of abstractions like pipelines, Pcollections and Ptransforms. Ptransforms are performed on Pcollections and this process is called pipeline.


- **Basic Codes**: Now we go step by step to learn Apache beam coding:
    
    i. ***Pipeline*** : The Pipeline abstraction encapsulates all the data and steps in your data processing task. Your Beam driver program typically starts by constructing a Pipeline object, and then using that object as the basis for creating the pipeline’s data sets as PCollections and its operations as Transforms.
      
    - ***Creating Pipeline*** :
      
      ```python
        import apache_beam as beam
        import apache_beam.options.pipeline_options as PipelineOptions

        with beam.Pipeline(options=PipelineOptions()) as p:
             pass
      ```
                 
    - ***Setting Pipeline options from command-line*** :
          
       ```python
         import apache_beam as beam
         from apache_beam.options.pipeline_options import PipelineOptions
         import argparse
            
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
                  pass
                
          if __name__ == '__main__':
             run()
       ```

     ii. ***Pcollection*** : The PCollection abstraction represents a potentially distributed, multi-element data set. You can think of a PCollection as “pipeline” data; Beam transforms use PCollection objects as inputs and outputs. As such, if you want to work with data in your pipeline, it must be in the form of a PCollection. 
    
    iii. ***Transform*** : Transforms are the operations in your pipeline, and provide a generic processing framework. You provide processing logic in the form of a function object (colloquially referred to as “user code”), and your user code is applied to each element of an input PCollection (or more than one PCollection). Types of transform functions are as follows:
    
    - ***ParDo*** : ParDo is a Beam transform for generic parallel processing. A ParDo transform considers each element in the input PCollection, performs some processing function (your user code) on that element, and emits zero, one, or multiple elements to an output PCollection. We will try to use this to create a SPLIT() function that will segregate the input CSV elements. Output saved from this is present with the name ****PARDO.txt****
         
       ```python
         class Split(beam.DoFn):
        
             def process(self, element):
              
                 Date,Open,High,Low,Close,Volume, AdjClose = element.split(',')
                 return [{
                         'Date': Date,
                         'Open': float(Open),
                         'Close': float(Close)
                         }]
            ...
            
         with beam.Pipeline(options=PipelineOptions()) as p:
            
             csv_lines = (p 
                          | beam.io.ReadFromText(known_args.input,  skip_header_lines = 1) 
                          | beam.ParDo(Split())
                          | beam.io.WriteToText(known_args.output))
       ```

    - ***GroupByKey*** : GroupByKey is a Beam transform for processing collections of key/value pairs. It’s a parallel reduction operation. The input to GroupByKey is a collection of key/value pairs that represents a multimap, where the collection contains multiple pairs that have the same key, but different values. Given such a collection, you use GroupByKey to collect all of the values associated with each unique key. We will try to use this to create a Singular output file containing all OPEN or CLOSE column values. Output saved from this is present with the name ****GroupByKey.txt****

       ```python
         class CollectOpen(beam.DoFn):
        
             def process(self, element):
                 result = [(1,element['Open'])]
                 return result
            ...
            
         with beam.Pipeline(options=PipelineOptions()) as p:
            
             csv_lines = (p 
                          | beam.io.ReadFromText(known_args.input,  skip_header_lines = 1) 
                          | beam.ParDo(Split())
             open_col  = (csv_lines 
                          | beam.ParDo(CollectOpen()) 
                          | "Grouping Keys Open" >> beam.GroupByKey()
                          | beam.io.WriteToText(known_args.output))          
       ```
    - ***CoGroupByKey*** : CoGroupByKey performs a relational join of two or more key/value PCollections that have the same key type. Consider using CoGroupByKey if you have multiple data sets that provide information about related things. For Example we will combine the output of GroupByKey output from above into one key with the name ****CoGroupByKey.txt****

       ```python
         class CollectOpen(beam.DoFn):
        
             def process(self, element):
                 result = [(1,element['Open'])]
                 return result
         class CollectClose(beam.DoFn):
        
             def process(self, element):
                 result = [(1,element['Close'])]
                 return result
            ...
            
         with beam.Pipeline(options=PipelineOptions()) as p:
            
             csv_lines = (p 
                          | beam.io.ReadFromText(known_args.input, skip_header_lines = 1) 
                          | beam.ParDo(Split())
             open_col  = (csv_lines 
                          | beam.ParDo(CollectOpen()) 
                          | "Grouping Keys Open" >> beam.GroupByKey()
                          )
             close_col =  (csv_lines 
                          | beam.ParDo(CollectClose())
                          | "Grouping Keys Close" >> beam.GroupByKey()
                          )
             output    = ( 
                         ({'Open'  : open_col, 
                          'Close'  : close_col} 
                          | beam.CoGroupByKey())
                          | beam.io.WriteToText(known_args.output)
                         )
       ```

    - ***Flatten*** : Flatten is a Beam transform for PCollection objects that store the same data type. Flatten merges multiple PCollection objects into a single logical PCollection. It returns a single PCollection that contains all of the elements in the PCollection objects in that tuple. Output saved from this is present with the name ****Flatten.txt****

       ```python
         class CollectOpen(beam.DoFn):
        
             def process(self, element):
                 result = [(1,element['Open'])]
                 return result
         class CollectClose(beam.DoFn):
        
             def process(self, element):
                 result = [(1,element['Close'])]
                 return result
            ...
            
         with beam.Pipeline(options=PipelineOptions()) as p:
            
             csv_lines =  (p 
                          | beam.io.ReadFromText(known_args.input, skip_header_lines = 1) 
                          | beam.ParDo(Split())
             open_col  =  (csv_lines 
                          | beam.ParDo(CollectOpen()) 
                          | "Grouping Keys Open" >> beam.GroupByKey()
                          )
             close_col =  (csv_lines 
                          | beam.ParDo(CollectClose())
                          | "Grouping Keys Close" >> beam.GroupByKey()
                          )
             output =     ( (close_col, open_col)
                          | beam.Flatten()
                          | beam.io.WriteToText(known_args.output)
                          )
       ```
    - ***CombineValues*** : CombineValues accepts a function that takes an iterable of elements as an input, and combines them to return a single element. CombineValues expects a keyed PCollection of elements, where the value is an iterable of elements to be combined. Output saved from this is present with the name ****CombineValues.txt****

       ```python
         class CollectOpen(beam.DoFn):
        
             def process(self, element):
                 result = [(1,element['Open'])]
                 return result
         class CollectClose(beam.DoFn):
        
             def process(self, element):
                 result = [(1,element['Close'])]
                 return result
            ...
            
         with beam.Pipeline(options=PipelineOptions()) as p:
            
             csv_lines =  (p 
                          | beam.io.ReadFromText(known_args.input,  skip_header_lines = 1) 
                          | beam.ParDo(Split())
             open_col  =  (csv_lines 
                          | beam.ParDo(CollectOpen()) 
                          | "Grouping Keys Open" >> beam.GroupByKey()
                          )
             close_col =  (csv_lines 
                          | beam.ParDo(CollectClose())
                          | "Grouping Keys Close" >> beam.GroupByKey()
                          )
             output =     ( open_col
                          |'Sum' >> beam.CombineValues(sum) 
                          | beam.io.WriteToText(known_args.output)
                          )
       ```
    - ***MeanCombineFn*** : MeanCombineFn accepts a function that takes an iterable of elements as an input, and combines them to return a mean of the input. CombineValues expects a keyed PCollection of elements, where the value is an iterable of elements to be combined. Output saved from this is present with the name ****MeanCombineFn.txt****

       ```python
         class CollectOpen(beam.DoFn):
        
             def process(self, element):
                 result = [(1,element['Open'])]
                 return result
         class CollectClose(beam.DoFn):
        
             def process(self, element):
                 result = [(1,element['Close'])]
                 return result
            ...
            
         with beam.Pipeline(options=PipelineOptions()) as p:
            
             csv_lines =  (p 
                          | beam.io.ReadFromText(known_args.input,  skip_header_lines = 1) 
                          | beam.ParDo(Split())
             open_col  =  (csv_lines 
                          | beam.ParDo(CollectOpen()) 
                          | "Grouping Keys Open" >> beam.GroupByKey()
                          )
             close_col =  (csv_lines 
                          | beam.ParDo(CollectClose())
                          | "Grouping Keys Close" >> beam.GroupByKey()
                          )
             mean_open =  ( open_col 
                          | "Calculating mean" >> beam.CombineValues(beam.combiners.MeanCombineFn())
                          | beam.io.WriteToText(known_args.output)
                          )
       ``` 
    iv. ***Pipeline I/O*** : When you create a pipeline, you often need to read data from some external source, such as a file or a database. Likewise, you may want your pipeline to output its result data to an external storage system. Beam provides read and write transforms for a number of common data storage types. Most commonly used transforms are stated below:
    
    - ***ReadFromText*** : Read transforms read data from an external source and return a PCollection representation of the data for use by your pipeline. 
    
       ```python
            with beam.Pipeline(options=PipelineOptions()) as p:
                csv_lines =  (p | beam.io.ReadFromText(known_args.input,  skip_header_lines = 1) 
       ``` 
       
    - ***WriteToText*** : Write transforms write the data in a PCollection to an external data source. You will most often use write transforms at the end of your pipeline to output your pipeline’s final results. 
    
       ```python
            with beam.Pipeline(options=PipelineOptions()) as p:
                output =  (csv_lines | beam.io.WriteToText(known_args.output) 
       ```
       
    v. ***Schemas*** : Often records have a nested structure. A nested structure occurs when a field itself has subfields so the type of the field itself has a schema. Fields that are array or map types is also a common feature of these structured records. For example Transaction table can be defined like given below and Code for this schema is also provided:
    
    | Field Name     | Field Name    |
    | -------------- | --------------|
    | Bank           | String        |
    | purchaseAmount | Double        |
    
    
   - ***Typing*** : Beam will automatically infer the correct schema based on the members of the class. 

       ```python
            import typing
            class Transaction(typing.NamedTuple):
                  bank: str
                  purchase_amount: float
       ```       

    vi. ***Windowing Basics*** : Some Beam transforms, such as GroupByKey and Combine, group multiple elements by a common key. Ordinarily, that grouping operation groups all of the elements that have the same key within the entire data set. With an unbounded data set, it is impossible to collect all of the elements, since new elements are constantly being added and may be infinitely many (e.g. streaming data). If you are working with unbounded PCollections, windowing is especially useful. You can set the windowing function for a PCollection by applying the Window transform. When you apply the Window transform, you must provide a WindowFn. The various types of windowing function are explained below:

    - ***Fixed Time Window*** : The simplest form of windowing is using fixed time windows: given a timestamped PCollection which might be continuously updating, each window might capture (for example) all elements with timestamps that fall into a 60 second interval.

       ```python
            from apache_beam import window 
            fixed_windowed_item = ( 
                                     items | 
                                    'Fixed Window' >> beam.WindowInto(window.FixedWindows(60)) 
                                  )
       ```    
    
    - ***Sliding Time Window*** : A sliding time window also represents time intervals in the data stream; however, sliding time windows can overlap. For example, each window might capture 60 seconds worth of data, but a new window starts every 30 seconds. The frequency with which sliding windows begin is called the period. Therefore, our example would have a window duration of 60 seconds and a period of 30 seconds.Because multiple windows overlap, most elements in a data set will belong to more than one window. This kind of windowing is useful for taking running averages of data; using sliding time windows, you can compute a running average of the past 60 seconds’ worth of data, updated every 30 seconds. 

       ```python
            from apache_beam import window 
            sliding_windowed_item = ( 
                                     items | 
                                    'sliding Window' >> beam.WindowInto(window.SlidingWindows(60, 30)) 
                                  )
       ```  

    - ***Per Session Window*** : A session window function defines windows that contain elements that are within a certain gap duration of another element. Session windowing applies on a per-key basis and is useful for data that is irregularly distributed with respect to time. For example, a data stream representing user mouse activity may have long periods of idle time interspersed with high concentrations of clicks. If data arrives after the minimum specified gap duration time, this initiates the start of a new window. The following example code shows how to apply Window to divide a PCollection into session windows, where each session must be separated by a time gap of at least 10 minutes (600 seconds). Note that the sessions are per-key — each key in the collection will have its own session groupings depending on the data distribution

       ```python
            from apache_beam import window 
            session_windowed_item = ( 
                                     items | 
                                    'Session Window' >> beam.WindowInto(window.Sessions(10 * 60)) 
                                  )
       ```  

    - ***Single Global Window*** : By default, all data in a PCollection is assigned to the single global window, and late data is discarded. If your data set is of a fixed size, you can use the global window default for your PCollection. You can use the single global window if you are working with an unbounded data set (e.g. from a streaming data source) but use caution when applying aggregating transforms such as GroupByKey and Combine. The single global window with a default trigger generally requires the entire data set to be available before processing, which is not possible with continuously updating data.
    
       ```python
            from apache_beam import window 
            Global_windowed_item = ( 
                                     items | 
                                    'GLobal Window' >> beam.WindowInto(window.GlobalWindows()) 
                                  )
       ``` 

    vi. ***Watermarks and late data*** : In any data processing system, there is a certain amount of lag between the time a data event occurs (the “event time”, determined by the timestamp on the data element itself) and the time the actual data element gets processed at any stage in your pipeline (the “processing time”, determined by the clock on the system processing the element).For example, let’s say we have a PCollection that’s using fixed-time windowing, with windows that are five minutes long. For each window, Beam must collect all the data with an event time timestamp in the given window range (between 0:00 and 4:59 in the first window, for instance). Data with timestamps outside that range (data from 5:00 or later) belong to a different window. However, data isn’t always guaranteed to arrive in a pipeline in time order, or to always arrive at predictable intervals. Beam tracks a ***watermark***, which is the system’s notion of when all data in a certain window can be expected to have arrived in the pipeline. Once the watermark progresses past the end of a window, any further element that arrives with a timestamp in that window is considered ***late data***.You can allow late data by invoking the .withAllowedLateness operation when you set your PCollection's windowing strategy. The following code example demonstrates a windowing strategy that will allow late data up to two days after the end of a window.
    
    ```python
            pc = [Initial PCollection]
            pc | beam.WindowInto(
                                 FixedWindows(60),
                                 trigger=trigger_fn
                                 accumulation_mode=accumulation_mode,
                                 timestamp_combiner=timestamp_combiner,
                                 allowed_lateness=Duration(seconds=2*24*60*60)
                                )
    ``` 

    vi. ***Triggers*** : When collecting and grouping data into windows, Beam uses triggers to determine when to emit the aggregated results of each window. If you use Beam’s default windowing configuration and default trigger, Beam outputs the aggregated result when it estimates all data has arrived, and discards all subsequent data for that window.Beam provides a number of pre-built triggers that you can set:

    - ***Event time triggers*** : These triggers operate on the event time, as indicated by the timestamp on each data element. Beam’s default trigger is event time-based. The AfterWatermark trigger operates on event time. The AfterWatermark trigger emits the contents of a window after the watermark passes the end of the window, based on the timestamps attached to the data elements. The watermark is a global progress metric, and is Beam’s notion of input completeness within your pipeline at any given point. AfterWatermark only fires when the watermark passes the end of the window.
    
       ```python
            AfterWatermark(
                 early=AfterProcessingTime(delay=1 * 60), late=AfterCount(1))

       ```   
       
    - ***Processing time triggers*** : These triggers operate on the processing time – the time when the data element is processed at any given stage in the pipeline.The AfterProcessingTime trigger operates on processing time. For example, the AfterProcessingTime trigger emits a window after a certain amount of processing time has passed since data was received. The processing time is determined by the system clock, rather than the data element’s timestamp. The AfterProcessingTime trigger is useful for triggering early results from a window, particularly a window with a large time frame such as a single global window.
    
    
    - **Data Driven triggers*** : These triggers operate by examining the data as it arrives in each window, and firing when that data meets a certain property. Currently, data-driven triggers only support firing after a certain number of data elements.Beam provides one data-driven trigger, AfterCount. This trigger works on an element count; it fires after the current pane has collected at least N elements. This allows a window to emit early results (before all the data has accumulated), which can be particularly useful if you are using a single global window.
    

    - **Composite triggers*** : These triggers combine multiple triggers in various ways.
    
        - You can add additional early firings or late firings to **AfterWatermark.pastEndOfWindow** via **.withEarlyFirings** and **.withLateFirings**.
        
        - **Repeatedly.forever** specifies a trigger that executes forever. Any time the trigger’s conditions are met, it causes a window to emit results and then resets and starts over. It can be useful to combine **Repeatedly.forever** with **.orFinally** to specify a condition that causes the repeating trigger to stop.
        
        - **AfterEach.inOrder** combines multiple triggers to fire in a specific sequence. Each time a trigger in the sequence emits a window, the sequence advances to the next trigger.
        
        - **AfterFirst** takes multiple triggers and emits the first time any of its argument triggers is satisfied. This is equivalent to a logical OR operation for multiple triggers.
        
        - **AfterAll** takes multiple triggers and emits when all of its argument triggers are satisfied. This is equivalent to a logical AND operation for multiple triggers.
        
        - **orFinally** can serve as a final condition to cause any trigger to fire one final time and never fire again.
   
    - **Setting a Trigger*** : When you set a windowing function for a PCollection by using the WindowInto transform, you can also specify a trigger.You set the trigger(s) for a PCollection by setting the trigger parameter when you use the WindowInto transform. This code sample sets a time-based trigger for a PCollection, which emits results one minute after the first element in that window has been processed. The accumulation_mode parameter sets the window’s accumulation mode.

       ```python
           pcollection | WindowInto(
                         FixedWindows(1 * 60),
                         trigger=AfterProcessingTime(1 * 60),
                         accumulation_mode=AccumulationMode.DISCARDING)
       ``` 
    vi. ***Metrics*** : Metrics provide some insight into the current state of a user pipeline, potentially while the pipeline is running.There are various types of matrics currently supported.

    - **Counter*** : A metric that reports a single long value and can be incremented or decremented.
    
    - **Distribution*** : A metric that reports information about the distribution of reported values.

    - **Gauge*** : A metric that reports the latest value out of reported values. Since metrics are collected from many workers the value may not be the absolute last, but one of the latest values.
    
- **Batch Pipelines**: After learning basics of Apache Beam, let us try to create a Batch Pipeline to run it on Google DataFlow. You can find the repository [Here](https://github.com/adityasolanki205/Batch-Pipeline-using-Apache-Beam.git)

## Credits
1. Akash Nimare's [README.md](https://gist.github.com/akashnimare/7b065c12d9750578de8e705fb4771d2f#file-readme-md)
2. [Apache Beam](https://beam.apache.org/documentation/programming-guide/#triggers)
