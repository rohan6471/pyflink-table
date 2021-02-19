from pyflink.table import *
from pyflink.table.expressions import col

# environment configuration
t_env = BatchTableEnvironment.create(
    environment_settings=EnvironmentSettings.new_instance().in_batch_mode().use_blink_planner().build())

# register Orders table and Result table sink in table environment
source_data_path1 = "data/dc.csv"
source_data_path2 = "data/mi.csv"
result_data_path = "output/sets"
source_ddl1 = f"""
        create table `IPLDC`(
            ID BIGINT,
            BATSMAN VARCHAR,
            TEAM VARCHAR,
            BOWLER VARCHAR
            
            ) with (
            'connector' = 'filesystem',
            'format' = 'csv',
            'path' = '{source_data_path1}'
        )
        """
source_ddl2 = f"""
        create table `IPLMI`(
            ID BIGINT,
            BATSMAN VARCHAR,
            TEAM VARCHAR,
            BOWLER VARCHAR
            ) with (
            'connector' = 'filesystem',
            'format' = 'csv',
            'path' = '{source_data_path2}'
        )
        """
t_env.execute_sql(source_ddl1)
t_env.execute_sql(source_ddl2)

sink_ddl = f"""
    create table `Result`(
        ID BIGINT,
            BATSMAN VARCHAR,
            TEAM VARCHAR,
            BOWLER VARCHAR        
    ) with (
        'connector' = 'filesystem',
        'format' = 'csv',
        'path' = '{result_data_path}'
    )
    """
t_env.execute_sql(sink_ddl)

left = t_env.from_path("IPLDC")
right = t_env.from_path("IPLMI")
result = left.union(right)
result1 =left.union_all(right)
result2 = left.intersect(right)
result3 = left.intersect_all(right)
result.execute_insert("Result").wait()
result1.execute_insert("Result").wait()
result2.execute_insert("Result").wait()
result3.execute_insert("Result").wait()

