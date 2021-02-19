from pyflink.table import *

# environment configuration
t_env = BatchTableEnvironment.create(
    environment_settings=EnvironmentSettings.new_instance().in_batch_mode().use_blink_planner().build())

# register Orders table and Result table sink in table environment
source_data_path = "data/data.csv"
result_data_path = "output/aggregation"
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
    create table `Result`(
        COUNTRY VARCHAR,
        cnt BIGINT
    ) with (
        'connector' = 'filesystem',
        'format' = 'csv',
        'path' = '{result_data_path}'
    )
    """
t_env.execute_sql(sink_ddl)

# specify table program
orders = t_env.from_path("Orders")  

orders.group_by("DEST_COUNTRY").select(orders.DEST_COUNTRY, orders.DEST_COUNTRY.count.alias('cnt')).execute_insert("Result").wait()