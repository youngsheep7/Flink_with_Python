from pyflink.datastream import StreamExecutionEnvironment
from pyflink.dataset import ExecutionEnvironment
from pyflink.table import TableConfig, DataTypes, BatchTableEnvironment, StreamTableEnvironment
from pyflink.table.descriptors import Schema, OldCsv, FileSystem, Kafka, Json, Csv

exec_env = StreamExecutionEnvironment.get_execution_environment()
exec_env.set_parallelism(1)
t_config = TableConfig()
t_env = StreamTableEnvironment.create(exec_env, t_config)

t_env.connect(Kafka()
              .version("0.11")
              .topic("test")
              .property("zookeeper.connect", "localhost:2181")
              .property("bootstrap.servers", "localhost:9092")
              ) \
    .in_append_mode() \
    .with_format(Csv()
                 .line_delimiter("\r\n")      \
                 .derive_schema()) \
    .with_schema(Schema()
                 .field("tbd", DataTypes.STRING())) \
    .register_table_source('mySource')

t_env.connect(FileSystem().path('../production_data/kafkaoutput')) \
    .with_format(OldCsv()
                 .field('tbd', DataTypes.STRING())) \
    .with_schema(Schema()
                 .field("tbd", DataTypes.STRING())) \
    .register_table_sink('mySink')

t_env.scan('mySource') \
    .insert_into('mySink')

t_env.execute("tutorial_job")