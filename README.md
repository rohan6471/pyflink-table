# pyflink-table

## Team 
![group_pic](https://github.com/rohan6471/pyflink-table/blob/main/group.PNG)

## Team Members and Subtopics

- Scan, Select and Filter - Kamal Reddy
- Aggregation operations - Mohan Pratapa
- Column Operations - Bhaskar Reddy
- Set Operations - Chandrakanth Polisetty
- Join operations on pyflink tables - Rohan Bhandari
- Convert pandas dataframes to pyflink table - suma soma


### Aggregation operations - Mohan Pratapa

- Data set containing the names of different countires was taken from www.kaggle.com. 
- Aggregation operations like Group by has been performed on the data set.
- Python script has been developed which has the source and sink tables setup. 
- Environment path set up needs to be done in the python file.
- To execute the python script, type the command python aggregations.py
- The result has been displayed in the output/aggregation folder in different partition files.

# Flink
Apache Flink is an open-source, unified stream-processing and batch-processing framework. It provides a high-throughput, low-latency streaming engine as well as support for 
event-time processing and state management. Its applications are fault-tolerant in the event of machine failure and support exactly-once semantics.It does not provide its own data-storage system, but provides data-source and sink connectors to systems such as Amazon Kinesis, Apache Kafka, HDFS, Apache Cassandra, and ElasticSearch 

## SUMA SOMA 

## SUBTOPIC- Convert pandas dataframes to pyflink table
To convert pandas Dataframe to pyflink table i am using Colaboratory a product from Google Research.

Pandas DataFrames can be converted into a PyFlink Table. Internally, PyFlink will serialize the Pandas DataFrame using Arrow columnar format. The serialized data will be processed and deserialized in Arrow source during execution. The Arrow source can also be used in streaming jobs.

## prerequisites

- Apache Flink

- Python

- A Dataset

    I took Netflix dataset from kaggle.

- Web browser

- Colaboratory

    Colaboratory, or "Colab" for short, allows you to write and execute Python in your browser, with Zero configuration required, Free access to GPUs and Easy sharing.

## Process and Commands

## What is Colaboratory and how to use it?

Google provides Jupyter Notebook like interface to run Python code on online Virtual Machines.

With Colab you can harness the full power of popular Python libraries to analyze and visualize data.

After opening [colab](https://colab.research.google.com/), select New Python3 Notebook

start importing all necessary libraries.

Add python script to the cell and run the code pressing Ctlr + ENTER keys.

## steps :

Upload your dataset to colab.

![image](https://github.com/rohan6471/pyflink-table/blob/main/sumas_pandas_flinktable/images/uploadingdataset.png)


Install Apache Flink using command.

-  !pip install apache-flink

Add the following statements to import the libraries.

- from pyflink.table import StreamTableEnvironment, DataTypes, table_config
- from pyflink.datastream import StreamExecutionEnvironment

- import pandas as pd

Table API applications begin by declaring a table environment; either a BatchTableEvironment for batch applications or StreamTableEnvironment for streaming applications. This serves as the main entry point for interacting with the Flink runtime. It can be used for setting execution parameters such as restart strategy, default parallelism, etc. The table config allows setting Table API specific configurations


```env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
t_config = table_config.TableConfig()
t_env = StreamTableEnvironment.create(env,t_config)
t_env.get_config().get_configuration().set_boolean("python.fn-execution.memory.managed", True)
```
Create a Pandas DataFrame

- df = pd.read_csv("/content/netflix_titles.csv")

Reading the columns from csv file.

- col = ['show_id', 'type', 'title']

Create a flink table using dataframe.

- table = t_env.from_pandas(df,col)

Command to show the values 

- table.select("show_id, type").to_pandas()

Script: 

![image](https://github.com/rohan6471/pyflink-table/blob/main/sumas_pandas_flinktable/images/script.png)


Output: 


![image](https://github.com/rohan6471/pyflink-table/blob/main/sumas_pandas_flinktable/images/output.png)


References:

[About colaboratory](https://www.geeksforgeeks.org/how-to-run-python-code-on-google-colaboratory/)

[apache flink example](https://ci.apache.org/projects/flink/flink-docs-stable/dev/python/table-api-users-guide/conversion_of_pandas.html#convert-pandas-dataframe-to-pyflink-table)

## Chandrakanth Polisetty - Set operations
## Prerequisites:
* Two datasets to perform set operations
* Apache Flink

## Process

* Two datasets containing information about the cricket players have been taken form www.kaggle.com
* Create the table enivornment using the following commands
```
t_env = BatchTableEnvironment.create(
    environment_settings=EnvironmentSettings.new_instance().in_batch_mode().use_blink_planner().build())
```
* Developed the python script with source and sink tables in it.
* These were the commands used to perform operations on datasets:
```
left = t_env.from_path("IPLDC")
right = t_env.from_path("IPLMI")
result = left.union(right)
result1 =left.union_all(right)
result2 = left.intersect(right)
result3 = left.intersect_all(right)
```
* Run the script using "python filename.py"
* result will be displayed in output/sets in your project folder.     




