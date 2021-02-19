from pyflink.table import *
from pyflink.table.expressions import col

# environment configuration
t_env = BatchTableEnvironment.create(
        environment_settings=EnvironmentSettings.new_instance().in_batch_mode().use_blink_planner().build())
 
# register Orders table and Result table sink in table environment
source_data_path1 = "data/orders.csv"
source_data_path2 = "data/customer.csv"
result_data_path = "output_rohan/joins"
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
source_ddl2 = f"""
        create table `Customer`(
            CustID BIGINT,
            CustomerName VARCHAR,
            Country VARCHAR
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
        OrderID BIGINT,
        CustomerName VARCHAR,
        Country VARCHAR        
    ) with (
        'connector' = 'filesystem',
        'format' = 'csv',
        'path' = '{result_data_path}'
    )
    """
t_env.execute_sql(sink_ddl)

left = t_env.from_path("Order")
right = t_env.from_path("Customer")
left.join(right).where(left.CustomerID == right.CustID).select(left.OrderID , right.CustomerName, right.Country).execute_insert("Result").wait()
left_outer_result = left.left_outer_join(right, left.CustomerID == right.CustID).select(left.OrderID, right.CustomerName, right.Country)
right_outer_result = left.right_outer_join(right, left.CustomerID == right.CustID).select(left.OrderID, right.CustomerName, right.Country)
left_outer_result.execute_insert("Result").wait()
right_outer_result.execute_insert("Result").wait()
