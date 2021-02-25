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

# Flink
Apache Flink is an open-source, unified stream-processing and batch-processing framework. It provides a high-throughput, low-latency streaming engine as well as support for 
event-time processing and state management. Its applications are fault-tolerant in the event of machine failure and support exactly-once semantics.It does not provide its own data-storage system, but provides data-source and sink connectors to systems such as Amazon Kinesis, Apache Kafka, HDFS, Apache Cassandra, and ElasticSearch 

## Scan, Select and Filter - Kamal Reddy

In this project, I have taken data set which contains the data on countries and its respective count and performed operations like scan, select and filter.

## Aggregation operations - Mohan Pratapa

- Data set containing the names of different countires was taken from www.kaggle.com. 
- Aggregation operations like Group by has been performed on the data set.
- Python script has been developed which has the source and sink tables setup. 
- Environment path set up needs to be done in the python file.
- To execute the python script, type the command python aggregations.py
- The result has been displayed in the output/aggregation folder in different partition files.

# Rohan Bhandari
## Join Operations on PyFlink Table 
### Prerequisites
- python 3.5+ versions
- Install apache-flink using pip command  
``` python -m pip install apache-flink ```
- Two datasets to perform join operations

## Process
- #### Creating Table Environment
We need to configure the flink table environment using the BatchTableEnvironment as we are currently working on the batch data and create an instance for the enivornment.    
```
t_env = BatchTableEnvironment.create(environment_settings=EnvironmentSettings.new_instance().in_batch_mode().use_blink_planner().build())

```         

- #### Creating Source and Sink Tables
After creating the table enivornment, we need to configure the source and sink table and need to define the schema for each table. We need to configure two sources as we are performing joins operations on two different datasets and also we need to configure the sink table with the schema that matches the expected results.

* Creating sourcing table  
```
source_ddl1 = f"""
        create table `Order`(
            CustomerID BIGINT,
            OrderID BIGINT
            ) with (
            'connector' = 'filesystem',
            'format' = 'csv',
            'path' = '{source_data_path1}'
        )
        """ 
   ```
   * Creating sink Table  
   ```
   sink_ddl = f"""
    create table `Result`(
        OrderID BIGINT,
        CustomerName VARCHAR,
        Country VARCHAR        
    ) with (
        'connector' = 'filesystem',
        'format' = 'csv',
        'path' = '{result_data_path}'
    )
    """
   ```
  - #### Join Operations
 After creating the source and sink tables we need to perform the join operations as below:   
 ```
left.join(right).where(left.CustomerID == right.CustID).select(left.OrderID , right.CustomerName, right.Country).execute_insert("Result").wait()
left_outer_result = left.left_outer_join(right, left.CustomerID == right.CustID).select(left.OrderID, right.CustomerName, right.Country)
right_outer_result = left.right_outer_join(right, left.CustomerID == right.CustID).select(left.OrderID, right.CustomerName, right.Country)
left_outer_result.execute_insert("Result").wait()
right_outer_result.execute_insert("Result").wait()
 ```
 ## Execution
 #### After completing the sript, we need to save the script to the root folder of our project.
 
To execute, open the powershell as administrator from root folder of the project.    
Then run the following command: 

```python joins.py ```  
 
We get the output to the redirected output folder.

 ## References
 - [https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/python/table-api-users-guide/operations.html](https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/python/table-api-users-guide/operations.html)
 
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

## Column Operations- Bhaskar Reddy
- I have taken the data set from an external source https://www.kaggle.com/
- Performed operation like adding column to the table, droping specified column from the table, replacing the columns.
- Python script has been developed which has the source and sink tables setup. 
- Used the powershell terminal to excute the python script "python column.py

```
# environment configuration
t_env = BatchTableEnvironment.create(
    environment_settings=EnvironmentSettings.new_instance().in_batch_mode().use_blink_planner().build())
```
* Developed the python script with source and sink tables in it.
* These were the commands used to perform operations on datasets:
source_ddl = f"""
        create table Orders(
            DEST_COUNTRY VARCHAR,
            ORIGIN_COUNTRY VARCHAR,
            Cnt BIGINT
            ) with (
            'connector' = 'filesystem',
            'format' = 'csv',
            'path' = '{source_data_path}'
        )
        """
t_env.execute_sql(source_ddl)

sink_ddl = f"""
    create table `ResultC`(
        ORIGIN_COUNTRY VARCHAR,
        Cnt BIGINT        
        ) with (
        'connector' = 'filesystem',
        'format' = 'csv',
        'path' = '{result_data_path}'
    )
    """
t_env.execute_sql(sink_ddl)

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
