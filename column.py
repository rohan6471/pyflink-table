from pyflink.table import *
from pyflink.table.expressions import concat

# environment configuration
t_env = BatchTableEnvironment.create(
    environment_settings=EnvironmentSettings.new_instance().in_batch_mode().use_blink_planner().build())

# register Orders table and Result table sink in table environment
source_data_path = "data/data.csv"
result_data_path = "output/column"
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

# specify table program
orders = t_env.from_path("Orders")  # schema (a, b, c, rowtime)
# result = orders.add_columns(concat(orders.DEST_COUNTRY, ' code 1'))
# result.execute_insert("ResultC").wait()

# result2 = orders.add_or_replace_columns(concat(orders.DEST_COUNTRY, ' code 1').alias('desc'))
# result2.execute_insert("ResultC").wait()

result3 = orders.drop_columns(orders.DEST_COUNTRY)
result3.execute_insert("ResultC").wait()

# orders.group_by("DEST_COUNTRY").select(orders.DEST_COUNTRY, orders.DEST_COUNTRY.count.alias('cnt')).execute_insert("Result").wait()