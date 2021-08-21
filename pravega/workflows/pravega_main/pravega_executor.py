import time
from typing import List

import ai_flow as af
from ai_flow_plugins.job_plugins import python, flink
from pyflink.table import Table

from tensorflow_main import train


def get_model_path():
    return '/Users/nicholas/Downloads/pravega_hackathon/pravega'


def get_data_path():
    return '/Users/nicholas/Downloads/pravega_hackathon/pravega/data'


def get_dependencies_path():
    return '/Users/nicholas/Downloads/pravega_hackathon'


class TrainModel(python.PythonProcessor):
    def process(self, execution_context: python.python_processor.ExecutionContext, input_list: List) -> List:
        train_path = get_data_path() + '/train.csv'
        model_dir = get_model_path() + '/model/base_model'
        save_name = 'base_model'
        train(train_path, model_dir, save_name)
        af.register_model_version(model=execution_context.config['model_info'], model_path=model_dir)
        return []


class Source(flink.FlinkPythonProcessor):
    def __init__(self) -> None:
        super().__init__()

    def process(self, execution_context: flink.ExecutionContext, input_list: List[Table] = None) -> List[Table]:
        print("### {} setup done2 for {}".format(self.__class__.__name__, "sads"))
        t_env = execution_context.table_env

        t_env.get_config().set_python_executable('/usr/local/bin/python')
        print('Source(flink.FlinkPythonProcessor)')
        print(t_env.get_config().get_configuration().to_dict())

        t_env.get_config().get_configuration().set_boolean('python.fn-execution.memory.managed', True)
        t_env.get_config().get_configuration().set_string('pipeline.global-job-parameters',
                                                          '"modelPath:""{}/model/base_model/frozen_model"""'
                                                          .format(get_model_path()))
        t_env.get_config().get_configuration().set_string('pipeline.classpaths',
                                                          'file://{}/analytics-zoo-bigdl_0.12.2-spark_2.4.3-0.10.0-serving.jar;'
                                                          'file://{}/pravega-connectors-flink-1.11_2.12-0.10.0-259.612304b-20210818.090839-1.jar'
                                                          .format(get_dependencies_path(), get_dependencies_path()))
        t_env.get_config().get_configuration().set_string('classloader.resolve-order', 'parent-first')
        t_env.get_config().get_configuration().set_integer('python.fn-execution.bundle.size', 1)
        t_env.register_java_function('cluster_serving',
                                     'com.intel.analytics.zoo.serving.operator.ClusterServingFunction')

        t_env.execute_sql('''
                    CREATE TABLE input_table (
                        uuid STRING,
                        visit_time STRING,
                        user_id STRING,
                        item_id STRING,
                        features STRING
                    ) WITH (
                        'connector' = 'pravega',
                        'controller-uri' = 'tcp://localhost:9090',
                        'scope' = 'pravega-scope',
                        'scan.streams' = 'predict-input-stream',
                        'format' = 'csv'
                    )
                ''')

        t_env.execute_sql('''
                    CREATE TABLE write_example (
                        uuid STRING,
                        data STRING
                    ) WITH (
                        'connector' = 'pravega',
                        'controller-uri' = 'tcp://localhost:9090',
                        'scope' = 'pravega-scope',
                        'sink.stream' = 'predict-output-stream',
                        'format' = 'csv'
                    )
                ''')

        t_env.from_path('input_table').print_schema()
        return [t_env.from_path('input_table')]


class Transformer(flink.FlinkPythonProcessor):
    def __init__(self):
        super().__init__()
        self.model_name = None

    def setup(self, execution_context: flink.ExecutionContext):
        self.model_name = execution_context.config['model_info']

    def process(self, execution_context: flink.ExecutionContext, input_list: List[Table] = None) -> List[Table]:
        result_table = input_list[0].select('uuid, cluster_serving(uuid, features)')
        return [result_table]


class Sink(flink.FlinkPythonProcessor):
    def process(self, execution_context: flink.ExecutionContext, input_list: List[Table] = None) -> List[Table]:
        print("### {} setup done".format(self.__class__.__name__))
        execution_context.statement_set.add_insert('write_example', input_list[0])
        return []
